package com.enterprise.eventbus.resilience;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.time.Instant;

/**
 * Time-budget guard that prevents processing from exceeding the poll interval.
 * Used by both single and batch listeners to avoid rebalance-triggering timeouts.
 *
 * <p>Thread-confined — one instance per consumer thread per poll cycle.</p>
 */
public class PollIntervalSafetyValve {

    private static final Logger log = LoggerFactory.getLogger(PollIntervalSafetyValve.class);

    private final long safetyBudgetMs;
    private final String bindingName;
    private final Clock clock;
    private long pollStartTimeMs;

    public PollIntervalSafetyValve(String bindingName, long safetyBudgetMs) {
        this(bindingName, safetyBudgetMs, Clock.systemUTC());
    }

    // Visible for testing
    PollIntervalSafetyValve(String bindingName, long safetyBudgetMs, Clock clock) {
        this.bindingName = bindingName;
        this.safetyBudgetMs = safetyBudgetMs;
        this.clock = clock;
    }

    /**
     * Mark the start of a new poll cycle. Must be called before processing begins.
     */
    public void markPollStart() {
        this.pollStartTimeMs = clock.millis();
    }

    /**
     * Check whether the safety budget has been exceeded.
     *
     * @return true if elapsed time exceeds the safety threshold, meaning
     *         processing should stop and return to poll()
     */
    public boolean isExceeded() {
        long elapsed = clock.millis() - pollStartTimeMs;
        if (elapsed >= safetyBudgetMs) {
            log.warn("Poll interval safety valve triggered for binding '{}': "
                    + "elapsed={}ms >= budget={}ms. Stopping batch processing to avoid rebalance.",
                    bindingName, elapsed, safetyBudgetMs);
            return true;
        }
        return false;
    }

    /**
     * Returns remaining budget in milliseconds. Used to cap retry durations.
     */
    public long remainingBudgetMs() {
        long elapsed = clock.millis() - pollStartTimeMs;
        return Math.max(0, safetyBudgetMs - elapsed);
    }

    /**
     * Returns elapsed time since poll start.
     */
    public long elapsedMs() {
        return clock.millis() - pollStartTimeMs;
    }
}
