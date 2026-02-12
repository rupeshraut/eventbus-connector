package com.enterprise.eventbus.config;

import com.enterprise.eventbus.model.BatchFailureStrategy;
import com.enterprise.eventbus.model.ListenerType;
import com.enterprise.eventbus.model.OrderingMode;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import org.springframework.kafka.listener.ContainerProperties;

/**
 * Configuration for a single topic binding. Each binding produces a main container
 * plus optional retry-tier containers and a DLT topic.
 */
public class TopicBindingProperties {

    @NotBlank(message = "Topic name is required")
    private String topic;

    @NotBlank(message = "Consumer group ID is required")
    private String groupId;

    private ListenerType listenerType = ListenerType.SINGLE;

    private ContainerProperties.AckMode ackMode = ContainerProperties.AckMode.MANUAL_IMMEDIATE;

    @Min(1)
    private int concurrency = 1;

    @Min(1)
    private int maxPollRecords = 50;

    @Min(5000)
    private long maxPollIntervalMs = 600_000L;

    @Min(10000)
    private long sessionTimeoutMs = 45_000L;

    @Min(1000)
    private long heartbeatIntervalMs = 10_000L;

    private boolean autoStartup = true;

    private boolean staticMembership = false;

    private String cluster = "default";

    private BatchFailureStrategy batchFailureStrategy = BatchFailureStrategy.SEEK_TO_FAILED;

    private OrderingMode orderingMode = OrderingMode.RELAXED_IN_RETRY;

    @Valid
    private RetryProperties retry = new RetryProperties();

    @Valid
    private CircuitBreakerProperties circuitBreaker = new CircuitBreakerProperties();

    @Valid
    private DltProperties dlt = new DltProperties();

    /** Safety valve threshold as percentage of max.poll.interval.ms (0.0 - 1.0). */
    private double pollIntervalSafetyThreshold = 0.7;

    // -- Getters / Setters --

    public String getTopic() { return topic; }
    public void setTopic(String topic) { this.topic = topic; }

    public String getGroupId() { return groupId; }
    public void setGroupId(String groupId) { this.groupId = groupId; }

    public ListenerType getListenerType() { return listenerType; }
    public void setListenerType(ListenerType listenerType) { this.listenerType = listenerType; }

    public ContainerProperties.AckMode getAckMode() { return ackMode; }
    public void setAckMode(ContainerProperties.AckMode ackMode) { this.ackMode = ackMode; }

    public int getConcurrency() { return concurrency; }
    public void setConcurrency(int concurrency) { this.concurrency = concurrency; }

    public int getMaxPollRecords() { return maxPollRecords; }
    public void setMaxPollRecords(int maxPollRecords) { this.maxPollRecords = maxPollRecords; }

    public long getMaxPollIntervalMs() { return maxPollIntervalMs; }
    public void setMaxPollIntervalMs(long maxPollIntervalMs) { this.maxPollIntervalMs = maxPollIntervalMs; }

    public long getSessionTimeoutMs() { return sessionTimeoutMs; }
    public void setSessionTimeoutMs(long sessionTimeoutMs) { this.sessionTimeoutMs = sessionTimeoutMs; }

    public long getHeartbeatIntervalMs() { return heartbeatIntervalMs; }
    public void setHeartbeatIntervalMs(long heartbeatIntervalMs) { this.heartbeatIntervalMs = heartbeatIntervalMs; }

    public boolean isAutoStartup() { return autoStartup; }
    public void setAutoStartup(boolean autoStartup) { this.autoStartup = autoStartup; }

    public boolean isStaticMembership() { return staticMembership; }
    public void setStaticMembership(boolean staticMembership) { this.staticMembership = staticMembership; }

    public String getCluster() { return cluster; }
    public void setCluster(String cluster) { this.cluster = cluster; }

    public BatchFailureStrategy getBatchFailureStrategy() { return batchFailureStrategy; }
    public void setBatchFailureStrategy(BatchFailureStrategy batchFailureStrategy) { this.batchFailureStrategy = batchFailureStrategy; }

    public OrderingMode getOrderingMode() { return orderingMode; }
    public void setOrderingMode(OrderingMode orderingMode) { this.orderingMode = orderingMode; }

    public RetryProperties getRetry() { return retry; }
    public void setRetry(RetryProperties retry) { this.retry = retry; }

    public CircuitBreakerProperties getCircuitBreaker() { return circuitBreaker; }
    public void setCircuitBreaker(CircuitBreakerProperties circuitBreaker) { this.circuitBreaker = circuitBreaker; }

    public DltProperties getDlt() { return dlt; }
    public void setDlt(DltProperties dlt) { this.dlt = dlt; }

    public double getPollIntervalSafetyThreshold() { return pollIntervalSafetyThreshold; }
    public void setPollIntervalSafetyThreshold(double pollIntervalSafetyThreshold) { this.pollIntervalSafetyThreshold = pollIntervalSafetyThreshold; }

    /**
     * Effective max.poll.records based on listener type if not explicitly set.
     */
    public int getEffectiveMaxPollRecords() {
        return maxPollRecords > 0 ? maxPollRecords
                : (listenerType == ListenerType.BATCH ? 200 : 50);
    }

    /**
     * Calculate the poll interval safety budget in milliseconds.
     */
    public long getPollIntervalSafetyBudgetMs() {
        return (long) (maxPollIntervalMs * pollIntervalSafetyThreshold);
    }
}
