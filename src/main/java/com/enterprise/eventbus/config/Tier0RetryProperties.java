package com.enterprise.eventbus.config;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.Positive;

/**
 * In-memory (Resilience4j) Tier-0 retry configuration.
 */
public class Tier0RetryProperties {

    @Min(1)
    private int maxAttempts = 3;

    @Positive
    private long initialBackoffMs = 100L;

    @Positive
    private double multiplier = 2.0;

    @Positive
    private long maxBackoffMs = 2000L;

    // -- Getters / Setters --

    public int getMaxAttempts() { return maxAttempts; }
    public void setMaxAttempts(int maxAttempts) { this.maxAttempts = maxAttempts; }

    public long getInitialBackoffMs() { return initialBackoffMs; }
    public void setInitialBackoffMs(long initialBackoffMs) { this.initialBackoffMs = initialBackoffMs; }

    public double getMultiplier() { return multiplier; }
    public void setMultiplier(double multiplier) { this.multiplier = multiplier; }

    public long getMaxBackoffMs() { return maxBackoffMs; }
    public void setMaxBackoffMs(long maxBackoffMs) { this.maxBackoffMs = maxBackoffMs; }

    /**
     * Calculates worst-case total retry duration in milliseconds.
     */
    public long calculateWorstCaseDurationMs() {
        long total = 0;
        long currentBackoff = initialBackoffMs;
        for (int i = 1; i < maxAttempts; i++) {
            total += Math.min(currentBackoff, maxBackoffMs);
            currentBackoff = (long) (currentBackoff * multiplier);
        }
        return total;
    }
}
