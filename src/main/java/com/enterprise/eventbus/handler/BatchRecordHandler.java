package com.enterprise.eventbus.handler;

import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * User-facing handler for batch processing. The connector iterates the batch
 * and invokes this handler per record, managing partial failure and offset commits.
 *
 * @param <K> record key type
 * @param <V> record value type
 */
@FunctionalInterface
public interface BatchRecordHandler<K, V> {

    /**
     * Process a single record from a batch. Throw any exception to trigger
     * partial-batch failure handling (commit successes, seek to failure).
     */
    void handle(ConsumerRecord<K, V> record) throws Exception;
}
