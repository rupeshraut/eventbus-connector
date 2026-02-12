package com.enterprise.eventbus.config;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;

public class RetryTierProperties {

    @Min(value = 1000, message = "Tier delay must be at least 1000ms")
    private long delayMs = 10_000L;

    @Min(value = 1, message = "Max attempts must be at least 1")
    private int maxAttempts = 3;

    @NotBlank
    private String topicSuffix;

    @Min(1)
    private int concurrency = 1;

    // -- Getters / Setters --

    public long getDelayMs() { return delayMs; }
    public void setDelayMs(long delayMs) { this.delayMs = delayMs; }

    public int getMaxAttempts() { return maxAttempts; }
    public void setMaxAttempts(int maxAttempts) { this.maxAttempts = maxAttempts; }

    public String getTopicSuffix() { return topicSuffix; }
    public void setTopicSuffix(String topicSuffix) { this.topicSuffix = topicSuffix; }

    public int getConcurrency() { return concurrency; }
    public void setConcurrency(int concurrency) { this.concurrency = concurrency; }

    /**
     * Resolves the full retry topic name from the base topic.
     */
    public String resolveTopicName(String baseTopic) {
        return baseTopic + "." + topicSuffix;
    }
}
