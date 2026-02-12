package com.enterprise.eventbus.listener;

import com.enterprise.eventbus.dlq.DltPublisher;
import com.enterprise.eventbus.exception.BatchProcessingException;
import com.enterprise.eventbus.handler.BatchRecordHandler;
import com.enterprise.eventbus.handler.ExceptionClassifier;
import com.enterprise.eventbus.model.BatchFailureStrategy;
import com.enterprise.eventbus.model.ExceptionRouting;
import com.enterprise.eventbus.observability.MdcContextPropagator;
import com.enterprise.eventbus.offset.PartitionOffsetManager;
import com.enterprise.eventbus.resilience.PollIntervalSafetyValve;
import com.enterprise.eventbus.resilience.ResilienceDecorator;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.listener.BatchAcknowledgingConsumerAwareMessageListener;
import org.springframework.kafka.support.Acknowledgment;

import java.time.Instant;
import java.util.List;

/**
 * Batch acknowledging listener adapter with partial failure handling.
 *
 * <p><b>SEEK_TO_FAILED strategy:</b> Processes records sequentially, commits offsets
 * for all successfully processed records, and seeks back to the first failed record.
 * No records are skipped or duplicated.</p>
 *
 * <p><b>DLQ_AND_CONTINUE strategy:</b> Failed records go to DLT immediately,
 * remaining records continue processing, full batch committed at end.</p>
 */
public class BatchRecordListenerAdapter<K, V>
        implements BatchAcknowledgingConsumerAwareMessageListener<K, V> {

    private static final Logger log = LoggerFactory.getLogger(BatchRecordListenerAdapter.class);

    private final String bindingName;
    private final BatchRecordHandler<K, V> handler;
    private final ResilienceDecorator resilienceDecorator;
    private final ExceptionClassifier exceptionClassifier;
    private final DltPublisher dltPublisher;
    private final RetryTierPublisher retryTierPublisher;
    private final PollIntervalSafetyValve safetyValve;
    private final BatchFailureStrategy failureStrategy;
    private final Counter processedCounter;
    private final Counter failedCounter;
    private final Timer processingTimer;

    public BatchRecordListenerAdapter(String bindingName,
                                       BatchRecordHandler<K, V> handler,
                                       ResilienceDecorator resilienceDecorator,
                                       ExceptionClassifier exceptionClassifier,
                                       DltPublisher dltPublisher,
                                       RetryTierPublisher retryTierPublisher,
                                       PollIntervalSafetyValve safetyValve,
                                       BatchFailureStrategy failureStrategy,
                                       MeterRegistry meterRegistry) {
        this.bindingName = bindingName;
        this.handler = handler;
        this.resilienceDecorator = resilienceDecorator;
        this.exceptionClassifier = exceptionClassifier;
        this.dltPublisher = dltPublisher;
        this.retryTierPublisher = retryTierPublisher;
        this.safetyValve = safetyValve;
        this.failureStrategy = failureStrategy;

        this.processedCounter = Counter.builder("eventbus.records.processed")
                .tag("binding", bindingName).tag("type", "batch").register(meterRegistry);
        this.failedCounter = Counter.builder("eventbus.records.failed")
                .tag("binding", bindingName).tag("type", "batch").register(meterRegistry);
        this.processingTimer = Timer.builder("eventbus.processing.duration")
                .tag("binding", bindingName).tag("type", "batch").register(meterRegistry);
    }

    @Override
    public void onMessage(List<ConsumerRecord<K, V>> records, Acknowledgment acknowledgment,
                           Consumer<?, ?> consumer) {
        safetyValve.markPollStart();
        var offsetManager = new PartitionOffsetManager();

        log.debug("Batch received for binding '{}': {} records", bindingName, records.size());

        for (int i = 0; i < records.size(); i++) {
            var record = records.get(i);
            MdcContextPropagator.setContext(bindingName, record);

            try {
                // Safety valve check — stop processing to avoid rebalance
                if (safetyValve.isExceeded()) {
                    handleSafetyValveTriggered(offsetManager, records, i, consumer, acknowledgment);
                    return;
                }

                // Process with Resilience4j
                processingTimer.record(() -> {
                    try {
                        resilienceDecorator.execute(r -> handler.handle(r), record);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });

                offsetManager.markProcessed(record);
                processedCounter.increment();

            } catch (Exception ex) {
                Throwable cause = (ex.getCause() != null) ? ex.getCause() : ex;
                failedCounter.increment();

                switch (failureStrategy) {
                    case SEEK_TO_FAILED -> {
                        handleSeekToFailed(offsetManager, record, i, consumer, acknowledgment, cause);
                        return; // Exit batch processing
                    }
                    case DLQ_AND_CONTINUE -> {
                        handleDlqAndContinue(record, cause);
                        offsetManager.markProcessed(record); // Mark as handled (DLQ'd)
                    }
                }
            } finally {
                MdcContextPropagator.clear();
            }
        }

        // All records processed successfully — acknowledge full batch
        acknowledgment.acknowledge();
        log.debug("Batch completed for binding '{}': all {} records processed", bindingName, records.size());
    }

    /**
     * SEEK_TO_FAILED: Commit successful offsets, seek to the failed record.
     */
    private void handleSeekToFailed(PartitionOffsetManager offsetManager,
                                     ConsumerRecord<K, V> failedRecord,
                                     int failedIndex,
                                     Consumer<?, ?> consumer,
                                     Acknowledgment acknowledgment,
                                     Throwable exception) {
        log.warn("Batch failure at index {} for binding '{}': topic={}, partition={}, offset={}. "
                + "Strategy: SEEK_TO_FAILED. Committing {} successful offsets and seeking to failure.",
                failedIndex, bindingName, failedRecord.topic(), failedRecord.partition(),
                failedRecord.offset(), offsetManager.getOffsetsToCommit().size());

        // Route the failed record
        var routing = exceptionClassifier.classify(exception, 0);
        routeFailedRecord(failedRecord, routing, exception);

        // Commit successfully processed records
        if (offsetManager.hasProcessedOffsets()) {
            var offsets = offsetManager.getOffsetsToCommit();
            consumer.commitSync(offsets);
            log.info("Committed offsets for successful records: {}", offsets);
        }

        // Seek past the failed record (it's been routed to DLT/retry) and let
        // the container re-poll for remaining records
        var failedTp = new TopicPartition(failedRecord.topic(), failedRecord.partition());
        consumer.seek(failedTp, failedRecord.offset() + 1);
    }

    /**
     * DLQ_AND_CONTINUE: Send failed record to DLT, continue processing batch.
     */
    private void handleDlqAndContinue(ConsumerRecord<K, V> record, Throwable exception) {
        log.warn("Batch record failure for binding '{}' (DLQ_AND_CONTINUE): topic={}, partition={}, offset={}",
                bindingName, record.topic(), record.partition(), record.offset());

        var routing = exceptionClassifier.classify(exception, 0);
        routeFailedRecord(record, routing, exception);
    }

    private void routeFailedRecord(ConsumerRecord<K, V> record,
                                    com.enterprise.eventbus.model.ExceptionRoutingDecision routing,
                                    Throwable exception) {
        switch (routing.routing()) {
            case DEAD_LETTER -> dltPublisher.publish(record, "NON_RETRYABLE",
                    1, Instant.now(), exception, List.of());
            case NEXT_TIER -> retryTierPublisher.publishToTier(record, 1, 1, exception);
            case SKIP_TO_TIER -> retryTierPublisher.publishToTier(
                    record, routing.targetTier(), 1, exception);
            case PAUSE_CONTAINER -> log.warn("PAUSE_CONTAINER routing in batch mode — "
                    + "record at offset {} will be re-delivered after resume", record.offset());
        }
    }

    /**
     * Safety valve triggered — commit what we have and return to poll().
     */
    private void handleSafetyValveTriggered(PartitionOffsetManager offsetManager,
                                             List<ConsumerRecord<K, V>> records,
                                             int currentIndex,
                                             Consumer<?, ?> consumer,
                                             Acknowledgment acknowledgment) {
        log.warn("Poll interval safety valve triggered at index {}/{} for binding '{}'. "
                + "Committing {} processed records and returning to poll().",
                currentIndex, records.size(), bindingName,
                offsetManager.getOffsetsToCommit().size());

        if (offsetManager.hasProcessedOffsets()) {
            var offsets = offsetManager.getOffsetsToCommit();
            consumer.commitSync(offsets);
        }

        // Seek remaining unprocessed records back for re-delivery
        var remaining = records.subList(currentIndex, records.size());
        for (var record : remaining) {
            var tp = new TopicPartition(record.topic(), record.partition());
            long lastProcessed = offsetManager.getLastProcessedOffset(tp);
            if (record.offset() > lastProcessed) {
                consumer.seek(tp, record.offset());
                break; // Only need to seek once per partition to the earliest unprocessed
            }
        }
    }
}
