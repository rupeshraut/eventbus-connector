package com.enterprise.eventbus.dlq;

import com.enterprise.eventbus.model.TierFailureRecord;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.index.CompoundIndex;
import org.springframework.data.mongodb.core.index.Indexed;
import org.springframework.data.mongodb.core.mapping.Document;

import java.time.Instant;
import java.util.List;
import java.util.Map;

/**
 * MongoDB document for persisting dead-letter records.
 * Provides full forensic context for debugging and replay.
 *
 * <p>Indexed for efficient queries by binding name, creation time, and replay eligibility.</p>
 */
@Document(collection = "eventbus_dlt_records")
@CompoundIndex(name = "idx_binding_created", def = "{'bindingName': 1, 'createdAt': 1}")
@CompoundIndex(name = "idx_binding_replayed", def = "{'bindingName': 1, 'replayCount': 1, 'createdAt': 1}")
public class DltDocumentEntity {

    @Id
    private String id;

    @Indexed
    private String bindingName;

    private String originalTopic;
    private int originalPartition;
    private long originalOffset;
    private Instant originalTimestamp;

    private byte[] key;
    private byte[] value;
    private Map<String, String> originalHeaders;

    private String dltReason;
    private int totalAttempts;
    private Instant firstFailureTimestamp;

    private String lastExceptionClass;
    private String lastExceptionMessage;
    private String lastExceptionStackTrace;

    private List<TierFailureRecord> tierHistory;

    private int replayCount;

    @Indexed
    private Instant createdAt;

    private Instant lastReplayedAt;

    // -- Static factory --

    public static DltDocumentEntity from(com.enterprise.eventbus.model.DltRecord record) {
        var entity = new DltDocumentEntity();
        entity.setId(record.id());
        entity.setBindingName(record.bindingName());
        entity.setOriginalTopic(record.originalTopic());
        entity.setOriginalPartition(record.originalPartition());
        entity.setOriginalOffset(record.originalOffset());
        entity.setOriginalTimestamp(record.originalTimestamp());
        entity.setKey(record.key());
        entity.setValue(record.value());
        entity.setOriginalHeaders(record.originalHeaders());
        entity.setDltReason(record.dltReason());
        entity.setTotalAttempts(record.totalAttempts());
        entity.setFirstFailureTimestamp(record.firstFailureTimestamp());
        entity.setLastExceptionClass(record.lastExceptionClass());
        entity.setLastExceptionMessage(record.lastExceptionMessage());
        entity.setLastExceptionStackTrace(record.lastExceptionStackTrace());
        entity.setTierHistory(record.tierHistory());
        entity.setReplayCount(record.replayCount());
        entity.setCreatedAt(record.createdAt());
        return entity;
    }

    public com.enterprise.eventbus.model.DltRecord toModel() {
        return new com.enterprise.eventbus.model.DltRecord(
                id, bindingName, originalTopic, originalPartition, originalOffset,
                originalTimestamp, key, value, originalHeaders, dltReason,
                totalAttempts, firstFailureTimestamp, lastExceptionClass,
                lastExceptionMessage, lastExceptionStackTrace, tierHistory,
                replayCount, createdAt
        );
    }

    // -- Getters / Setters --

    public String getId() { return id; }
    public void setId(String id) { this.id = id; }

    public String getBindingName() { return bindingName; }
    public void setBindingName(String bindingName) { this.bindingName = bindingName; }

    public String getOriginalTopic() { return originalTopic; }
    public void setOriginalTopic(String originalTopic) { this.originalTopic = originalTopic; }

    public int getOriginalPartition() { return originalPartition; }
    public void setOriginalPartition(int originalPartition) { this.originalPartition = originalPartition; }

    public long getOriginalOffset() { return originalOffset; }
    public void setOriginalOffset(long originalOffset) { this.originalOffset = originalOffset; }

    public Instant getOriginalTimestamp() { return originalTimestamp; }
    public void setOriginalTimestamp(Instant originalTimestamp) { this.originalTimestamp = originalTimestamp; }

    public byte[] getKey() { return key; }
    public void setKey(byte[] key) { this.key = key; }

    public byte[] getValue() { return value; }
    public void setValue(byte[] value) { this.value = value; }

    public Map<String, String> getOriginalHeaders() { return originalHeaders; }
    public void setOriginalHeaders(Map<String, String> originalHeaders) { this.originalHeaders = originalHeaders; }

    public String getDltReason() { return dltReason; }
    public void setDltReason(String dltReason) { this.dltReason = dltReason; }

    public int getTotalAttempts() { return totalAttempts; }
    public void setTotalAttempts(int totalAttempts) { this.totalAttempts = totalAttempts; }

    public Instant getFirstFailureTimestamp() { return firstFailureTimestamp; }
    public void setFirstFailureTimestamp(Instant firstFailureTimestamp) { this.firstFailureTimestamp = firstFailureTimestamp; }

    public String getLastExceptionClass() { return lastExceptionClass; }
    public void setLastExceptionClass(String lastExceptionClass) { this.lastExceptionClass = lastExceptionClass; }

    public String getLastExceptionMessage() { return lastExceptionMessage; }
    public void setLastExceptionMessage(String lastExceptionMessage) { this.lastExceptionMessage = lastExceptionMessage; }

    public String getLastExceptionStackTrace() { return lastExceptionStackTrace; }
    public void setLastExceptionStackTrace(String lastExceptionStackTrace) { this.lastExceptionStackTrace = lastExceptionStackTrace; }

    public List<TierFailureRecord> getTierHistory() { return tierHistory; }
    public void setTierHistory(List<TierFailureRecord> tierHistory) { this.tierHistory = tierHistory; }

    public int getReplayCount() { return replayCount; }
    public void setReplayCount(int replayCount) { this.replayCount = replayCount; }

    public Instant getCreatedAt() { return createdAt; }
    public void setCreatedAt(Instant createdAt) { this.createdAt = createdAt; }

    public Instant getLastReplayedAt() { return lastReplayedAt; }
    public void setLastReplayedAt(Instant lastReplayedAt) { this.lastReplayedAt = lastReplayedAt; }
}
