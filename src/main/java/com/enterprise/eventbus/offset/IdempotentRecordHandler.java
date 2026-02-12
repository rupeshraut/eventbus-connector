package com.enterprise.eventbus.offset;

import com.enterprise.eventbus.handler.RecordHandler;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DuplicateKeyException;

import java.time.Instant;

/**
 * Decorator that wraps a {@link RecordHandler} with MongoDB-backed idempotency checking.
 * Before invoking the delegate, checks if the topic-partition-offset triple has already
 * been processed. After successful processing, records the offset.
 *
 * <p>Uses MongoDB's unique compound index for atomic check-and-insert.
 * TTL index auto-expires old records after 7 days.</p>
 *
 * @param <K> record key type
 * @param <V> record value type
 */
public class IdempotentRecordHandler<K, V> implements RecordHandler<K, V> {

    private static final Logger log = LoggerFactory.getLogger(IdempotentRecordHandler.class);

    private final RecordHandler<K, V> delegate;
    private final IdempotencyRepository repository;
    private final String bindingName;

    public IdempotentRecordHandler(RecordHandler<K, V> delegate,
                                    IdempotencyRepository repository,
                                    String bindingName) {
        this.delegate = delegate;
        this.repository = repository;
        this.bindingName = bindingName;
    }

    @Override
    public void handle(ConsumerRecord<K, V> record) throws Exception {
        String deduplicationKey = record.topic() + "-" + record.partition() + "-" + record.offset();

        // Attempt to insert the idempotency record first (atomic check-and-insert via unique index)
        try {
            repository.save(new IdempotencyRecord(
                    record.topic(), record.partition(), record.offset(),
                    bindingName, Instant.now()));
        } catch (DuplicateKeyException e) {
            // Already processed — skip
            log.debug("Duplicate record detected for binding '{}': {}", bindingName, deduplicationKey);
            return;
        }

        try {
            delegate.handle(record);
        } catch (Exception ex) {
            // Processing failed — remove the idempotency record so it can be retried
            try {
                repository.deleteById(deduplicationKey);
            } catch (Exception deleteEx) {
                log.warn("Failed to remove idempotency record for {}: {}",
                        deduplicationKey, deleteEx.getMessage());
            }
            throw ex;
        }
    }
}
