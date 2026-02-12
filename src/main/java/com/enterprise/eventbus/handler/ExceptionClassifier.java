package com.enterprise.eventbus.handler;

import com.enterprise.eventbus.model.ExceptionRoutingDecision;

/**
 * Classifies exceptions to determine routing: next tier, skip to tier, or dead letter.
 * Implementations should be stateless and thread-safe.
 */
@FunctionalInterface
public interface ExceptionClassifier {

    /**
     * Classify an exception and decide where the failed record should be routed.
     *
     * @param exception   the exception thrown by the handler
     * @param currentTier the current retry tier (0 = main consumer)
     * @return routing decision
     */
    ExceptionRoutingDecision classify(Throwable exception, int currentTier);
}
