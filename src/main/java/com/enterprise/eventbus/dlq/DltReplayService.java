package com.enterprise.eventbus.dlq;

import com.enterprise.eventbus.config.DltProperties;
import com.enterprise.eventbus.model.EventBusHeaders;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.domain.PageRequest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Replays dead-letter records from MongoDB back to Kafka topics.
 * Supports replay to original topic or to a specific retry tier.
 */
@Service
public class DltReplayService {

    private static final Logger log = LoggerFactory.getLogger(DltReplayService.class);
    private static final long SEND_TIMEOUT_SECONDS = 30;

    private final DltDocumentRepository repository;
    private final KafkaTemplate<byte[], byte[]> kafkaTemplate;
    private final MeterRegistry meterRegistry;

    public DltReplayService(DltDocumentRepository repository,
                            KafkaTemplate<byte[], byte[]> kafkaTemplate,
                            MeterRegistry meterRegistry) {
        this.repository = repository;
        this.kafkaTemplate = kafkaTemplate;
        this.meterRegistry = meterRegistry;
    }

    /**
     * Replay records from DLT back to the original topic.
     *
     * @param bindingName  binding to replay
     * @param originalTopic target topic
     * @param maxReplays   maximum replay count before records are skipped
     * @param count        number of records to replay
     * @return number of records actually replayed
     */
    public int replayToOriginalTopic(String bindingName, String originalTopic,
                                      int maxReplays, int count) {
        return replayToTopic(bindingName, originalTopic, maxReplays, count);
    }

    /**
     * Replay records to a specific retry tier topic.
     */
    public int replayToRetryTier(String bindingName, String retryTopic,
                                  int maxReplays, int count) {
        return replayToTopic(bindingName, retryTopic, maxReplays, count);
    }

    /**
     * Replay records within a time window.
     */
    public int replayByTimeWindow(String bindingName, String targetTopic,
                                   Instant from, Instant to) {
        var records = repository.findByBindingNameAndCreatedAtBetweenOrderByCreatedAtAsc(
                bindingName, from, to);

        log.info("Found {} DLT records for binding '{}' between {} and {}",
                records.size(), bindingName, from, to);

        int replayed = 0;
        for (var entity : records) {
            if (sendReplayRecord(entity, targetTopic)) {
                replayed++;
            }
        }

        log.info("Replayed {}/{} records for binding '{}' to topic '{}'",
                replayed, records.size(), bindingName, targetTopic);
        return replayed;
    }

    /**
     * Purge DLT records older than the threshold.
     */
    public long purge(String bindingName, Instant olderThan) {
        long deleted = repository.deleteByBindingNameAndCreatedAtBefore(bindingName, olderThan);
        log.info("Purged {} DLT records for binding '{}' older than {}",
                deleted, bindingName, olderThan);
        return deleted;
    }

    private int replayToTopic(String bindingName, String targetTopic,
                               int maxReplays, int count) {
        var pageable = PageRequest.of(0, count);
        var records = repository.findByBindingNameAndReplayCountLessThanOrderByCreatedAtAsc(
                bindingName, maxReplays, pageable);

        log.info("Found {} eligible DLT records for replay (binding='{}', maxReplays={}, requested={})",
                records.size(), bindingName, maxReplays, count);

        int replayed = 0;
        for (var entity : records) {
            if (sendReplayRecord(entity, targetTopic)) {
                replayed++;
            }
        }

        Counter.builder("eventbus.dlt.replayed")
                .tag("binding", bindingName)
                .register(meterRegistry)
                .increment(replayed);

        log.info("Replayed {}/{} records for binding '{}' to topic '{}'",
                replayed, records.size(), bindingName, targetTopic);
        return replayed;
    }

    private boolean sendReplayRecord(DltDocumentEntity entity, String targetTopic) {
        try {
            var headers = new RecordHeaders();
            if (entity.getOriginalHeaders() != null) {
                entity.getOriginalHeaders().forEach((k, v) ->
                        headers.add(k, v.getBytes(StandardCharsets.UTF_8)));
            }
            headers.add(EventBusHeaders.REPLAY_COUNT,
                    String.valueOf(entity.getReplayCount() + 1).getBytes(StandardCharsets.UTF_8));

            var producerRecord = new ProducerRecord<>(
                    targetTopic, null, null, entity.getKey(), entity.getValue(), headers);

            kafkaTemplate.send(producerRecord).get(SEND_TIMEOUT_SECONDS, TimeUnit.SECONDS);

            // Update replay count in MongoDB
            entity.setReplayCount(entity.getReplayCount() + 1);
            entity.setLastReplayedAt(Instant.now());
            repository.save(entity);

            return true;
        } catch (Exception ex) {
            log.error("Failed to replay DLT record id={} to topic '{}': {}",
                    entity.getId(), targetTopic, ex.getMessage(), ex);
            return false;
        }
    }
}
