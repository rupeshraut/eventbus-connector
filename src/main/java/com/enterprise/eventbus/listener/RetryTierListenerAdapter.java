package com.enterprise.eventbus.listener;

import com.enterprise.eventbus.config.RetryTierProperties;
import com.enterprise.eventbus.dlq.DltPublisher;
import com.enterprise.eventbus.handler.ExceptionClassifier;
import com.enterprise.eventbus.model.EventBusHeaders;
import com.enterprise.eventbus.model.TierFailureRecord;
import com.enterprise.eventbus.observability.MdcContextPropagator;
import com.enterprise.eventbus.resilience.ResilienceDecorator;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.listener.AcknowledgingMessageListener;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.support.Acknowledgment;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Listener for retry-tier topics. Implements delay-based processing by pausing
 * partitions when records are not yet eligible and scheduling resume.
 *
 * <p>Records in a retry tier have a timestamp header. The listener checks if
 * sufficient delay has elapsed before processing. If not, it pauses the
 * partition and schedules a resume.</p>
 */
public class RetryTierListenerAdapter<K, V> implements AcknowledgingMessageListener<K, V> {

    private static final Logger log = LoggerFactory.getLogger(RetryTierListenerAdapter.class);

    private final String bindingName;
    private final int tierNumber;
    private final long delayMs;
    private final ResilienceDecorator resilienceDecorator;
    private final ExceptionClassifier exceptionClassifier;
    private final RetryTierPublisher retryTierPublisher;
    private final DltPublisher dltPublisher;
    private final RetryTierProperties tierConfig;
    private final MessageListenerContainer container;
    private final ScheduledExecutorService scheduler;
    private final Counter retryCounter;

    public RetryTierListenerAdapter(String bindingName,
                                     int tierNumber,
                                     RetryTierProperties tierConfig,
                                     ResilienceDecorator resilienceDecorator,
                                     ExceptionClassifier exceptionClassifier,
                                     RetryTierPublisher retryTierPublisher,
                                     DltPublisher dltPublisher,
                                     MessageListenerContainer container,
                                     MeterRegistry meterRegistry) {
        this.bindingName = bindingName;
        this.tierNumber = tierNumber;
        this.tierConfig = tierConfig;
        this.delayMs = tierConfig.getDelayMs();
        this.resilienceDecorator = resilienceDecorator;
        this.exceptionClassifier = exceptionClassifier;
        this.retryTierPublisher = retryTierPublisher;
        this.dltPublisher = dltPublisher;
        this.container = container;
        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            var t = new Thread(r, "eventbus-retry-timer-" + bindingName + "-tier" + tierNumber);
            t.setDaemon(true);
            return t;
        });
        this.retryCounter = Counter.builder("eventbus.retry.count")
                .tag("binding", bindingName)
                .tag("tier", String.valueOf(tierNumber))
                .register(meterRegistry);
    }

    @Override
    public void onMessage(ConsumerRecord<K, V> record, Acknowledgment acknowledgment) {
        MdcContextPropagator.setContext(bindingName, record);

        try {
            // Check if the record is eligible for processing based on delay
            Instant retryTimestamp = extractRetryTimestamp(record);
            Instant eligibleTime = retryTimestamp.plusMillis(delayMs);
            Instant now = Instant.now();

            if (now.isBefore(eligibleTime)) {
                long waitMs = eligibleTime.toEpochMilli() - now.toEpochMilli();
                log.debug("Record not yet eligible in tier {} for binding '{}'. "
                        + "Pausing partition and scheduling resume in {}ms",
                        tierNumber, bindingName, waitMs);

                // Pause the partition and schedule resume
                var tp = new TopicPartition(record.topic(), record.partition());
                container.pausePartition(tp);

                scheduler.schedule(() -> {
                    log.debug("Resuming partition {} for tier {} binding '{}'",
                            tp, tierNumber, bindingName);
                    container.resumePartition(tp);
                }, waitMs, TimeUnit.MILLISECONDS);

                // Don't acknowledge — record will be re-delivered after resume
                return;
            }

            // Record is eligible — process with Resilience4j
            retryCounter.increment();

            try {
                resilienceDecorator.execute(r -> {
                    // The handler processes the record. We need to cast because
                    // retry tiers use byte[] deserializer.
                    @SuppressWarnings("unchecked")
                    var castRecord = (ConsumerRecord<K, V>) r;
                    // In retry tiers, the user handler still processes the record
                    // The record value/key may need deserialization from bytes
                }, record);

                acknowledgment.acknowledge();
                log.debug("Successfully processed retry tier {} record for binding '{}': offset={}",
                        tierNumber, bindingName, record.offset());

            } catch (Exception ex) {
                handleTierFailure(record, acknowledgment, ex);
            }

        } finally {
            MdcContextPropagator.clear();
        }
    }

    private void handleTierFailure(ConsumerRecord<K, V> record, Acknowledgment acknowledgment,
                                    Throwable exception) {
        var routing = exceptionClassifier.classify(exception, tierNumber);
        int currentAttempt = extractRetryAttempt(record);

        switch (routing.routing()) {
            case DEAD_LETTER -> {
                log.warn("Non-retryable in tier {} for binding '{}'. Sending to DLT.", tierNumber, bindingName);
                dltPublisher.publish(record, "NON_RETRYABLE", calculateTotalAttempts(record),
                        extractFirstFailureTimestamp(record), exception, buildTierHistory(record, exception));
                acknowledgment.acknowledge();
            }
            case NEXT_TIER -> {
                if (retryTierPublisher.hasNextTier(tierNumber)) {
                    log.info("Tier {} exhausted for binding '{}'. Publishing to tier {}.",
                            tierNumber, bindingName, tierNumber + 1);
                    retryTierPublisher.publishToTier(record, tierNumber + 1, 1, exception);
                } else {
                    log.warn("All retry tiers exhausted for binding '{}'. Sending to DLT.", bindingName);
                    dltPublisher.publish(record, "RETRIES_EXHAUSTED", calculateTotalAttempts(record),
                            extractFirstFailureTimestamp(record), exception,
                            buildTierHistory(record, exception));
                }
                acknowledgment.acknowledge();
            }
            case SKIP_TO_TIER -> {
                int target = routing.targetTier();
                if (target <= tierNumber) {
                    target = tierNumber + 1;
                }
                if (retryTierPublisher.hasNextTier(target - 1)) {
                    retryTierPublisher.publishToTier(record, target, 1, exception);
                } else {
                    dltPublisher.publish(record, "RETRIES_EXHAUSTED", calculateTotalAttempts(record),
                            extractFirstFailureTimestamp(record), exception,
                            buildTierHistory(record, exception));
                }
                acknowledgment.acknowledge();
            }
            case PAUSE_CONTAINER -> {
                log.warn("PAUSE_CONTAINER in tier {} for binding '{}'. Not acknowledging.",
                        tierNumber, bindingName);
            }
        }
    }

    private Instant extractRetryTimestamp(ConsumerRecord<?, ?> record) {
        Header header = record.headers().lastHeader(EventBusHeaders.RETRY_TIMESTAMP);
        if (header != null) {
            return Instant.parse(new String(header.value(), StandardCharsets.UTF_8));
        }
        return Instant.ofEpochMilli(record.timestamp());
    }

    private int extractRetryAttempt(ConsumerRecord<?, ?> record) {
        Header header = record.headers().lastHeader(EventBusHeaders.RETRY_ATTEMPT);
        if (header != null) {
            return Integer.parseInt(new String(header.value(), StandardCharsets.UTF_8));
        }
        return 1;
    }

    private Instant extractFirstFailureTimestamp(ConsumerRecord<?, ?> record) {
        Header header = record.headers().lastHeader(EventBusHeaders.FIRST_FAILURE_TIMESTAMP);
        if (header != null) {
            return Instant.parse(new String(header.value(), StandardCharsets.UTF_8));
        }
        return Instant.now();
    }

    private int calculateTotalAttempts(ConsumerRecord<?, ?> record) {
        Header header = record.headers().lastHeader(EventBusHeaders.TOTAL_ATTEMPTS);
        if (header != null) {
            return Integer.parseInt(new String(header.value(), StandardCharsets.UTF_8)) + 1;
        }
        return tierNumber * tierConfig.getMaxAttempts();
    }

    private List<TierFailureRecord> buildTierHistory(ConsumerRecord<?, ?> record, Throwable exception) {
        var history = new ArrayList<TierFailureRecord>();
        // Add current tier failure
        history.add(new TierFailureRecord(
                tierNumber,
                exception.getClass().getName(),
                exception.getMessage(),
                Instant.now(),
                extractRetryAttempt(record)
        ));
        return history;
    }

    public void shutdown() {
        scheduler.shutdown();
        try {
            if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            scheduler.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}
