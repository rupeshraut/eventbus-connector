package com.enterprise.eventbus.model;

/**
 * Centralized header key constants used across the connector.
 * Prevents magic-string duplication.
 */
public final class EventBusHeaders {

    private EventBusHeaders() {
        // constants only
    }

    // -- Inbound enrichment --
    public static final String BINDING_NAME = "x-eventbus-binding-name";
    public static final String RECEIVED_TIMESTAMP = "x-eventbus-received-timestamp";
    public static final String CONSUMER_INSTANCE = "x-eventbus-consumer-instance";

    // -- Original record provenance --
    public static final String ORIGINAL_TOPIC = "x-eventbus-original-topic";
    public static final String ORIGINAL_PARTITION = "x-eventbus-original-partition";
    public static final String ORIGINAL_OFFSET = "x-eventbus-original-offset";
    public static final String ORIGINAL_TIMESTAMP = "x-eventbus-original-timestamp";

    // -- Retry tier --
    public static final String RETRY_TIER = "x-eventbus-retry-tier";
    public static final String RETRY_ATTEMPT = "x-eventbus-retry-attempt";
    public static final String RETRY_TIMESTAMP = "x-eventbus-retry-timestamp";

    // -- DLT --
    public static final String DLT_REASON = "x-eventbus-dlt-reason";
    public static final String DLT_TIMESTAMP = "x-eventbus-dlt-timestamp";
    public static final String TOTAL_ATTEMPTS = "x-eventbus-total-attempts";
    public static final String FIRST_FAILURE_TIMESTAMP = "x-eventbus-first-failure-timestamp";
    public static final String LAST_EXCEPTION_CLASS = "x-eventbus-last-exception-class";
    public static final String LAST_EXCEPTION_MESSAGE = "x-eventbus-last-exception-message";
    public static final String LAST_EXCEPTION_STACKTRACE = "x-eventbus-last-exception-stacktrace";

    // -- Replay --
    public static final String REPLAY_COUNT = "x-eventbus-replay-count";

    // -- Tier-specific forensic (pattern: x-eventbus-tier{N}-exception) --
    public static String tierException(int tier) {
        return "x-eventbus-tier" + tier + "-exception";
    }

    public static String tierExhaustedAt(int tier) {
        return "x-eventbus-tier" + tier + "-exhausted-at";
    }
}
