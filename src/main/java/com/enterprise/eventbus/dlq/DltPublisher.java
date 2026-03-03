package com.enterprise.eventbus.dlq;

import com.enterprise.eventbus.config.TopicBindingProperties;
import com.enterprise.eventbus.exception.DltPublishException;
import com.enterprise.eventbus.handler.DltAlertHandler;
import com.enterprise.eventbus.model.DltRecord;
import com.enterprise.eventbus.model.DltStrategy;
import com.enterprise.eventbus.model.TierFailureRecord;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

public class DltPublisher {

    private static final Logger log = LoggerFactory.getLogger(DltPublisher.class);
    private static final long DLT_SEND_TIMEOUT_SECONDS = 30;

    private final String bindingName;
    private final String dltTopic;
    private final KafkaTemplate<byte[], byte[]> kafkaTemplate;
    private final DltDocumentRepository repository;
    private final TopicBindingProperties bindingConfig;
    private final DltAlertHandler alertHandler;
    private final Counter dltCounter;

    public DltPublisher(String bindingName,
                        TopicBindingProperties bindingConfig,
                        KafkaTemplate<byte[], byte[]> kafkaTemplate,
                        DltDocumentRepository repository,
                        DltAlertHandler alertHandler,
                        MeterRegistry meterRegistry) {
        this.bindingName = bindingName;
        this.bindingConfig = bindingConfig;
        this.dltTopic = bindingConfig.getDlt().resolveTopicName(bindingConfig.getTopic());
        this.kafkaTemplate = kafkaTemplate;
        this.repository = repository;
        this.alertHandler = alertHandler;
        this.dltCounter = Counter.builder("eventbus.records.dlt")
                .tag("binding", bindingName)
                .tag("topic", dltTopic)
                .register(meterRegistry);
    }

    public void publish(ConsumerRecord<?, ?> originalRecord,
                        String dltReason,
                        int totalAttempts,
                        Instant firstFailureTimestamp,
                        Throwable lastException,
                        List<TierFailureRecord> tierHistory) {

        log.warn("Publishing to DLT [{}]: topic={}, partition={}, offset={}, reason={}, attempts={}",
                dltTopic, originalRecord.topic(), originalRecord.partition(),
                originalRecord.offset(), dltReason, totalAttempts);

        var headers = DltHeaderEnricher.createDltHeaders(
                originalRecord, bindingName, dltReason, totalAttempts,
                firstFailureTimestamp, lastException);

        byte[] keyBytes = extractBytes(originalRecord.key());
        byte[] valueBytes = extractBytes(originalRecord.value());

        var producerRecord = new ProducerRecord<>(dltTopic, null,
                originalRecord.timestamp(), keyBytes, valueBytes, headers);

        // Synchronous send — if this fails, the original record must NOT be acknowledged
        try {
            kafkaTemplate.send(producerRecord).get(DLT_SEND_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            dltCounter.increment();
            log.info("Successfully published to DLT [{}]: offset={}", dltTopic, originalRecord.offset());
        } catch (Exception ex) {
            log.error("CRITICAL: Failed to publish to DLT [{}]: offset={}. "
                    + "Original record will NOT be acknowledged to prevent message loss.",
                    dltTopic, originalRecord.offset(), ex);
            throw new DltPublishException("Failed to publish to DLT topic " + dltTopic, ex);
        }

        // Persist to MongoDB for audit and replay
        persistToMongo(originalRecord, dltReason, totalAttempts, firstFailureTimestamp,
                lastException, tierHistory, keyBytes, valueBytes);

        // Alert if strategy requires it
        if (bindingConfig.getDlt().getStrategy() == DltStrategy.ALERT_AND_STORE && alertHandler != null) {
            invokeAlertHandler(originalRecord, dltReason, totalAttempts,
                    firstFailureTimestamp, lastException, tierHistory, keyBytes, valueBytes);
        }
    }

    private void persistToMongo(ConsumerRecord<?, ?> record, String dltReason,
                                 int totalAttempts, Instant firstFailureTimestamp,
                                 Throwable lastException, List<TierFailureRecord> tierHistory,
                                 byte[] keyBytes, byte[] valueBytes) {
        try {
            var dltRecord = new DltRecord(
                    UUID.randomUUID().toString(),
                    bindingName,
                    record.topic(),
                    record.partition(),
                    record.offset(),
                    Instant.ofEpochMilli(record.timestamp()),
                    keyBytes,
                    valueBytes,
                    extractHeaders(record),
                    dltReason,
                    totalAttempts,
                    firstFailureTimestamp,
                    lastException != null ? lastException.getClass().getName() : "Unknown",
                    lastException != null ? truncate(lastException.getMessage(), 500) : "",
                    lastException != null ? truncateStackTrace(lastException) : "",
                    tierHistory != null ? tierHistory : List.of(),
                    0,
                    Instant.now()
            );
            repository.save(DltDocumentEntity.from(dltRecord));
            log.debug("Persisted DLT record to MongoDB: id={}", dltRecord.id());
        } catch (Exception ex) {
            // MongoDB persistence failure should not block DLT publishing
            log.error("Failed to persist DLT record to MongoDB for offset={}: {}",
                    record.offset(), ex.getMessage(), ex);
        }
    }

    private void invokeAlertHandler(ConsumerRecord<?, ?> record, String dltReason,
                                     int totalAttempts, Instant firstFailureTimestamp,
                                     Throwable lastException, List<TierFailureRecord> tierHistory,
                                     byte[] keyBytes, byte[] valueBytes) {
        try {
            var dltRecord = new DltRecord(
                    UUID.randomUUID().toString(), bindingName,
                    record.topic(), record.partition(), record.offset(),
                    Instant.ofEpochMilli(record.timestamp()), keyBytes, valueBytes,
                    extractHeaders(record), dltReason, totalAttempts,
                    firstFailureTimestamp,
                    lastException != null ? lastException.getClass().getName() : "Unknown",
                    lastException != null ? lastException.getMessage() : "",
                    "", tierHistory != null ? tierHistory : List.of(), 0, Instant.now()
            );
            alertHandler.onDeadLetter(dltRecord);
        } catch (Exception ex) {
            log.error("DLT alert handler failed for binding '{}': {}", bindingName, ex.getMessage(), ex);
        }
    }

    private Map<String, String> extractHeaders(ConsumerRecord<?, ?> record) {
        var map = new HashMap<String, String>();
        for (Header header : record.headers()) {
            map.put(header.key(), header.value() != null
                    ? new String(header.value(), StandardCharsets.UTF_8) : "");
        }
        return map;
    }

    private byte[] extractBytes(Object value) {
        if (value == null) {
            return new byte[0];
        }
        if (value instanceof byte[] bytes) {
            return bytes;
        }
        return value.toString().getBytes(StandardCharsets.UTF_8);
    }

    private String truncate(String value, int max) {
        if (value == null) {
            return "";
        }
        return value.length() <= max ? value : value.substring(0, max) + "...";
    }

    private String truncateStackTrace(Throwable t) {
        var sb = new StringBuilder();
        sb.append(t.toString()).append("\n");
        for (var element : t.getStackTrace()) {
            if (sb.length() > 2000) {
                sb.append("... [truncated]");
                break;
            }
            sb.append("\tat ").append(element).append("\n");
        }
        return sb.toString();
    }

    public String getDltTopic() { return dltTopic; }
    public String getBindingName() { return bindingName; }
}
