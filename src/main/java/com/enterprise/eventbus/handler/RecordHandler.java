package com.enterprise.eventbus.handler;

import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * User-facing handler for single-record processing.
 * The connector handles retries, DLQ, offset management, and lifecycle.
 *
 * @param <K> record key type
 * @param <V> record value type
 */
@FunctionalInterface
public interface RecordHandler<K, V> {

    /**
     * Process a single record. Throw any exception to trigger retry/DLQ behavior.
     */
    void handle(ConsumerRecord<K, V> record) throws Exception;
}
