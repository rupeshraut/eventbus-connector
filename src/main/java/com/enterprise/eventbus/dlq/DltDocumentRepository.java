package com.enterprise.eventbus.dlq;

import org.springframework.data.domain.Pageable;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.stereotype.Repository;

import java.time.Instant;
import java.util.List;

/**
 * MongoDB repository for dead-letter records. Supports replay, purge, and statistics queries.
 */
@Repository
public interface DltDocumentRepository extends MongoRepository<DltDocumentEntity, String> {

    /**
     * Find records for replay: by binding, ordered oldest-first, limited by page size.
     */
    List<DltDocumentEntity> findByBindingNameOrderByCreatedAtAsc(String bindingName, Pageable pageable);

    /**
     * Find replay-eligible records (replay count below max).
     */
    List<DltDocumentEntity> findByBindingNameAndReplayCountLessThanOrderByCreatedAtAsc(
            String bindingName, int maxReplayCount, Pageable pageable);

    /**
     * Find records within a time window for targeted replay.
     */
    List<DltDocumentEntity> findByBindingNameAndCreatedAtBetweenOrderByCreatedAtAsc(
            String bindingName, Instant from, Instant to);

    /**
     * Count records by binding name.
     */
    long countByBindingName(String bindingName);

    /**
     * Count records by binding name and exception class.
     */
    long countByBindingNameAndLastExceptionClass(String bindingName, String exceptionClass);

    /**
     * Delete records older than a threshold.
     */
    long deleteByBindingNameAndCreatedAtBefore(String bindingName, Instant before);

    /**
     * Find oldest record for statistics.
     */
    DltDocumentEntity findFirstByBindingNameOrderByCreatedAtAsc(String bindingName);

    /**
     * Find newest record for statistics.
     */
    DltDocumentEntity findFirstByBindingNameOrderByCreatedAtDesc(String bindingName);
}
