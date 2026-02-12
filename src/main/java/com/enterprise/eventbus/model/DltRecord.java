package com.enterprise.eventbus.model;

import java.time.Instant;
import java.util.List;
import java.util.Map;

/**
 * Immutable representation of a dead-letter record with full forensic context.
 * Persisted to MongoDB for audit and replay.
 */
public record DltRecord(
        String id,
        String bindingName,
        String originalTopic,
        int originalPartition,
        long originalOffset,
        Instant originalTimestamp,
        byte[] key,
        byte[] value,
        Map<String, String> originalHeaders,
        String dltReason,
        int totalAttempts,
        Instant firstFailureTimestamp,
        String lastExceptionClass,
        String lastExceptionMessage,
        String lastExceptionStackTrace,
        List<TierFailureRecord> tierHistory,
        int replayCount,
        Instant createdAt
) {

    public DltRecord withIncrementedReplayCount() {
        return new DltRecord(
                id, bindingName, originalTopic, originalPartition, originalOffset,
                originalTimestamp, key, value, originalHeaders, dltReason,
                totalAttempts, firstFailureTimestamp, lastExceptionClass,
                lastExceptionMessage, lastExceptionStackTrace, tierHistory,
                replayCount + 1, createdAt
        );
    }
}
