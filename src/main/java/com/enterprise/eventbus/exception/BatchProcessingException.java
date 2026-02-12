package com.enterprise.eventbus.exception;

import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * Wraps a batch processing failure with the index and record that failed.
 * Used by the batch error handler to commit successful offsets and seek to the failure point.
 */
public final class BatchProcessingException extends EventBusException {

    private final int failedIndex;
    private final transient ConsumerRecord<?, ?> failedRecord;

    public BatchProcessingException(String message, Throwable cause, int failedIndex,
                                    ConsumerRecord<?, ?> failedRecord) {
        super(message, cause);
        this.failedIndex = failedIndex;
        this.failedRecord = failedRecord;
    }

    public int getFailedIndex() { return failedIndex; }
    public ConsumerRecord<?, ?> getFailedRecord() { return failedRecord; }
}
