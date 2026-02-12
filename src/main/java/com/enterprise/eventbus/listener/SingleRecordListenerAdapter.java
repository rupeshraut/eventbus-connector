package com.enterprise.eventbus.listener;

import com.enterprise.eventbus.dlq.DltPublisher;
import com.enterprise.eventbus.handler.ExceptionClassifier;
import com.enterprise.eventbus.handler.RecordHandler;
import com.enterprise.eventbus.model.EventBusHeaders;
import com.enterprise.eventbus.model.ExceptionRouting;
import com.enterprise.eventbus.model.TierFailureRecord;
import com.enterprise.eventbus.observability.MdcContextPropagator;
import com.enterprise.eventbus.resilience.PollIntervalSafetyValve;
import com.enterprise.eventbus.resilience.ResilienceDecorator;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.listener.AcknowledgingMessageListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * Single-record acknowledging listener adapter.
 * Wraps the user's {@link RecordHandler} with Resilience4j retry + circuit breaker,
 * poll-interval safety valve, deserialization failure detection, and DLT publishing.
 */
public class SingleRecordListenerAdapter<K, V> implements AcknowledgingMessageListener<K, V> {

    private static final Logger log = LoggerFactory.getLogger(SingleRecordListenerAdapter.class);

    private final String bindingName;
    private final RecordHandler<K, V> handler;
    private final ResilienceDecorator resilienceDecorator;
    private final ExceptionClassifier exceptionClassifier;
    private final DltPublisher dltPublisher;
    private final RetryTierPublisher retryTierPublisher;
    private final PollIntervalSafetyValve safetyValve;
    private final Counter processedCounter;
    private final Counter failedCounter;
    private final Timer processingTimer;

    public SingleRecordListenerAdapter(String bindingName,
                                        RecordHandler<K, V> handler,
                                        ResilienceDecorator resilienceDecorator,
                                        ExceptionClassifier exceptionClassifier,
                                        DltPublisher dltPublisher,
                                        RetryTierPublisher retryTierPublisher,
                                        PollIntervalSafetyValve safetyValve,
                                        MeterRegistry meterRegistry) {
        this.bindingName = bindingName;
        this.handler = handler;
        this.resilienceDecorator = resilienceDecorator;
        this.exceptionClassifier = exceptionClassifier;
        this.dltPublisher = dltPublisher;
        this.retryTierPublisher = retryTierPublisher;
        this.safetyValve = safetyValve;

        this.processedCounter = Counter.builder("eventbus.records.processed")
                .tag("binding", bindingName).tag("type", "single").register(meterRegistry);
        this.failedCounter = Counter.builder("eventbus.records.failed")
                .tag("binding", bindingName).tag("type", "single").register(meterRegistry);
        this.processingTimer = Timer.builder("eventbus.processing.duration")
                .tag("binding", bindingName).tag("type", "single").register(meterRegistry);
    }

    @Override
    public void onMessage(ConsumerRecord<K, V> record, Acknowledgment acknowledgment) {
        MdcContextPropagator.setContext(bindingName, record);
        safetyValve.markPollStart();

        try {
            // Step 1: Deserialization failure check
            if (isDeserializationFailure(record)) {
                handleDeserializationFailure(record, acknowledgment);
                return;
            }

            // Step 2: Process with Resilience4j decoration
            processingTimer.record(() -> {
                try {
                    resilienceDecorator.execute(r -> handler.handle(r), record);
                    processedCounter.increment();
                } catch (Exception ex) {
                    throw new RuntimeException(ex);
                }
            });

            // Step 3: Acknowledge on success
            acknowledgment.acknowledge();

        } catch (Exception ex) {
            Throwable cause = (ex.getCause() != null) ? ex.getCause() : ex;
            failedCounter.increment();
            handleFailure(record, acknowledgment, cause);
        } finally {
            MdcContextPropagator.clear();
        }
    }

    private void handleFailure(ConsumerRecord<K, V> record, Acknowledgment acknowledgment,
                                Throwable exception) {
        var routing = exceptionClassifier.classify(exception, 0);

        switch (routing.routing()) {
            case DEAD_LETTER -> {
                log.warn("Non-retryable exception for binding '{}', sending to DLT: {}",
                        bindingName, exception.getMessage());
                dltPublisher.publish(record, "NON_RETRYABLE",
                        1, Instant.now(), exception, List.of());
                acknowledgment.acknowledge();
            }
            case NEXT_TIER -> {
                log.info("Tier-0 retries exhausted for binding '{}', publishing to retry tier 1",
                        bindingName);
                retryTierPublisher.publishToTier(record, 1, 1, exception);
                acknowledgment.acknowledge();
            }
            case SKIP_TO_TIER -> {
                int targetTier = routing.targetTier();
                log.info("Skipping to retry tier {} for binding '{}'", targetTier, bindingName);
                retryTierPublisher.publishToTier(record, targetTier, 1, exception);
                acknowledgment.acknowledge();
            }
            case PAUSE_CONTAINER -> {
                log.warn("Exception classified as PAUSE_CONTAINER for binding '{}'. "
                        + "Not acknowledging record — container will be paused by circuit breaker.",
                        bindingName);
                // Do NOT acknowledge — record will be re-delivered after resume
            }
        }
    }

    private boolean isDeserializationFailure(ConsumerRecord<K, V> record) {
        return record.headers().lastHeader(
                ErrorHandlingDeserializer.VALUE_DESERIALIZER_EXCEPTION_HEADER) != null;
    }

    private void handleDeserializationFailure(ConsumerRecord<K, V> record,
                                               Acknowledgment acknowledgment) {
        log.error("Deserialization failure detected for binding '{}': topic={}, partition={}, offset={}. "
                + "Sending directly to DLT.", bindingName, record.topic(),
                record.partition(), record.offset());
        dltPublisher.publish(record, "DESERIALIZATION_FAILURE",
                0, Instant.now(), new RuntimeException("Deserialization failed"), List.of());
        acknowledgment.acknowledge();
    }
}
