package com.enterprise.eventbus.offset;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Tracks successfully processed offsets per partition during batch processing.
 * Thread-confined — one instance per consumer thread per poll cycle.
 *
 * <p>On partial batch failure, this provides the offset map for selective commit
 * (commit-the-good, seek-to-the-bad).</p>
 */
public class PartitionOffsetManager {

    private static final Logger log = LoggerFactory.getLogger(PartitionOffsetManager.class);

    private final Map<TopicPartition, Long> processedOffsets = new HashMap<>();

    /**
     * Mark a record as successfully processed.
     */
    public void markProcessed(ConsumerRecord<?, ?> record) {
        var tp = new TopicPartition(record.topic(), record.partition());
        // Store offset + 1 because commitSync expects the NEXT offset to fetch
        processedOffsets.merge(tp, record.offset() + 1, Math::max);
    }

    /**
     * Get the offset map for selective commit. Each entry is the next offset to fetch
     * for the partition (i.e., last processed offset + 1).
     */
    public Map<TopicPartition, OffsetAndMetadata> getOffsetsToCommit() {
        var result = new HashMap<TopicPartition, OffsetAndMetadata>();
        processedOffsets.forEach((tp, nextOffset) ->
                result.put(tp, new OffsetAndMetadata(nextOffset)));
        return Map.copyOf(result);
    }

    /**
     * Check if any offsets have been tracked (i.e., at least one record was processed).
     */
    public boolean hasProcessedOffsets() {
        return !processedOffsets.isEmpty();
    }

    /**
     * Get the highest processed offset for a specific partition.
     */
    public long getLastProcessedOffset(TopicPartition tp) {
        Long nextOffset = processedOffsets.get(tp);
        return nextOffset != null ? nextOffset - 1 : -1;
    }

    /**
     * Reset for a new poll cycle.
     */
    public void reset() {
        processedOffsets.clear();
    }

    @Override
    public String toString() {
        return "PartitionOffsetManager{offsets=" + processedOffsets + "}";
    }
}
