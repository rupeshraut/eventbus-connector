package com.enterprise.eventbus.model;

/**
 * Immutable routing decision produced by the {@link com.enterprise.eventbus.handler.ExceptionClassifier}.
 * Uses factory methods to enforce valid construction.
 */
public record ExceptionRoutingDecision(
        ExceptionRouting routing,
        int targetTier
) {

    public static ExceptionRoutingDecision nextTier() {
        return new ExceptionRoutingDecision(ExceptionRouting.NEXT_TIER, -1);
    }

    public static ExceptionRoutingDecision deadLetter() {
        return new ExceptionRoutingDecision(ExceptionRouting.DEAD_LETTER, -1);
    }

    public static ExceptionRoutingDecision skipToTier(int tier) {
        if (tier < 1) {
            throw new IllegalArgumentException("Skip-to-tier target must be >= 1, got: " + tier);
        }
        return new ExceptionRoutingDecision(ExceptionRouting.SKIP_TO_TIER, tier);
    }

    public static ExceptionRoutingDecision pauseContainer() {
        return new ExceptionRoutingDecision(ExceptionRouting.PAUSE_CONTAINER, -1);
    }
}
