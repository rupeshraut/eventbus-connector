package com.enterprise.eventbus.offset;

import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.index.CompoundIndex;
import org.springframework.data.mongodb.core.index.Indexed;
import org.springframework.data.mongodb.core.mapping.Document;

import java.time.Instant;

/**
 * MongoDB document for tracking processed record offsets.
 * Used by the optional idempotency interceptor to prevent duplicate processing.
 *
 * <p>TTL index auto-expires old records after 7 days to prevent unbounded growth.</p>
 */
@Document(collection = "eventbus_idempotency")
@CompoundIndex(name = "idx_dedup_key", def = "{'topic': 1, 'partition': 1, 'offset': 1}", unique = true)
public class IdempotencyRecord {

    @Id
    private String id;

    private String topic;
    private int partition;
    private long offset;
    private String bindingName;

    @Indexed(expireAfter = "7d")
    private Instant processedAt;

    public IdempotencyRecord() {}

    public IdempotencyRecord(String topic, int partition, long offset,
                              String bindingName, Instant processedAt) {
        this.id = topic + "-" + partition + "-" + offset;
        this.topic = topic;
        this.partition = partition;
        this.offset = offset;
        this.bindingName = bindingName;
        this.processedAt = processedAt;
    }

    public String getId() { return id; }
    public void setId(String id) { this.id = id; }

    public String getTopic() { return topic; }
    public void setTopic(String topic) { this.topic = topic; }

    public int getPartition() { return partition; }
    public void setPartition(int partition) { this.partition = partition; }

    public long getOffset() { return offset; }
    public void setOffset(long offset) { this.offset = offset; }

    public String getBindingName() { return bindingName; }
    public void setBindingName(String bindingName) { this.bindingName = bindingName; }

    public Instant getProcessedAt() { return processedAt; }
    public void setProcessedAt(Instant processedAt) { this.processedAt = processedAt; }
}
