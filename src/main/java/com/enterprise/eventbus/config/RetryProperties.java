package com.enterprise.eventbus.config;

import jakarta.validation.Valid;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Complete retry configuration: in-memory tier-0 + topic-based tiers + exception classification.
 */
public class RetryProperties {

    @Valid
    private Tier0RetryProperties tier0 = new Tier0RetryProperties();

    @Valid
    private List<RetryTierProperties> tiers = new ArrayList<>();

    private List<String> retryableExceptions = new ArrayList<>();

    private List<String> nonRetryableExceptions = new ArrayList<>();

    /** Maps exception class name → tier number to skip to. */
    private Map<String, Integer> skipToTierMapping = new HashMap<>();

    // -- Getters / Setters --

    public Tier0RetryProperties getTier0() { return tier0; }
    public void setTier0(Tier0RetryProperties tier0) { this.tier0 = tier0; }

    public List<RetryTierProperties> getTiers() { return tiers; }
    public void setTiers(List<RetryTierProperties> tiers) { this.tiers = tiers; }

    public List<String> getRetryableExceptions() { return retryableExceptions; }
    public void setRetryableExceptions(List<String> retryableExceptions) { this.retryableExceptions = retryableExceptions; }

    public List<String> getNonRetryableExceptions() { return nonRetryableExceptions; }
    public void setNonRetryableExceptions(List<String> nonRetryableExceptions) { this.nonRetryableExceptions = nonRetryableExceptions; }

    public Map<String, Integer> getSkipToTierMapping() { return skipToTierMapping; }
    public void setSkipToTierMapping(Map<String, Integer> skipToTierMapping) { this.skipToTierMapping = skipToTierMapping; }

    /**
     * Provides default tiers if none are configured.
     */
    public List<RetryTierProperties> getEffectiveTiers() {
        if (!tiers.isEmpty()) {
            return tiers;
        }
        return createDefaultTiers();
    }

    private List<RetryTierProperties> createDefaultTiers() {
        var tier1 = new RetryTierProperties();
        tier1.setDelayMs(10_000L);
        tier1.setMaxAttempts(3);
        tier1.setTopicSuffix("retry-1");

        var tier2 = new RetryTierProperties();
        tier2.setDelayMs(60_000L);
        tier2.setMaxAttempts(3);
        tier2.setTopicSuffix("retry-2");

        var tier3 = new RetryTierProperties();
        tier3.setDelayMs(300_000L);
        tier3.setMaxAttempts(3);
        tier3.setTopicSuffix("retry-3");

        return List.of(tier1, tier2, tier3);
    }
}
