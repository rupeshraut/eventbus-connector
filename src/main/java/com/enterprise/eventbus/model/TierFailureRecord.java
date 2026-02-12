package com.enterprise.eventbus.model;

import java.time.Instant;

/**
 * Per-tier failure information — part of the forensic trail on a DLT record.
 */
public record TierFailureRecord(
        int tier,
        String exceptionClass,
        String exceptionMessage,
        Instant exhaustedAt,
        int attempts
) {}
