package com.enterprise.eventbus.dlq;

import com.enterprise.eventbus.model.EventBusHeaders;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.time.Instant;

/**
 * Enriches Kafka record headers with forensic information for DLT and retry-tier publishing.
 */
public final class DltHeaderEnricher {

    private static final int MAX_STACKTRACE_LENGTH = 2048;

    private DltHeaderEnricher() {}

    /**
     * Creates DLT headers from a failed consumer record.
     */
    public static Headers createDltHeaders(ConsumerRecord<?, ?> original,
                                            String bindingName,
                                            String dltReason,
                                            int totalAttempts,
                                            Instant firstFailureTimestamp,
                                            Throwable lastException) {
        var headers = new RecordHeaders();

        // Copy original headers
        original.headers().forEach(h ->
                headers.add(h.key(), h.value()));

        // Provenance
        addHeader(headers, EventBusHeaders.ORIGINAL_TOPIC, original.topic());
        addHeader(headers, EventBusHeaders.ORIGINAL_PARTITION, String.valueOf(original.partition()));
        addHeader(headers, EventBusHeaders.ORIGINAL_OFFSET, String.valueOf(original.offset()));
        addHeader(headers, EventBusHeaders.ORIGINAL_TIMESTAMP, String.valueOf(original.timestamp()));

        // DLT metadata
        addHeader(headers, EventBusHeaders.BINDING_NAME, bindingName);
        addHeader(headers, EventBusHeaders.DLT_REASON, dltReason);
        addHeader(headers, EventBusHeaders.DLT_TIMESTAMP, Instant.now().toString());
        addHeader(headers, EventBusHeaders.TOTAL_ATTEMPTS, String.valueOf(totalAttempts));
        addHeader(headers, EventBusHeaders.FIRST_FAILURE_TIMESTAMP, firstFailureTimestamp.toString());

        // Exception details
        if (lastException != null) {
            addHeader(headers, EventBusHeaders.LAST_EXCEPTION_CLASS, lastException.getClass().getName());
            addHeader(headers, EventBusHeaders.LAST_EXCEPTION_MESSAGE,
                    truncate(lastException.getMessage(), 500));
            addHeader(headers, EventBusHeaders.LAST_EXCEPTION_STACKTRACE,
                    truncateStackTrace(lastException));
        }

        return headers;
    }

    /**
     * Creates retry-tier headers when publishing to a retry topic.
     */
    public static Headers createRetryHeaders(ConsumerRecord<?, ?> original,
                                              String bindingName,
                                              int retryTier,
                                              int retryAttempt,
                                              Throwable exception) {
        var headers = new RecordHeaders();

        // Copy original headers
        original.headers().forEach(h ->
                headers.add(h.key(), h.value()));

        // Provenance (only set if not already present — first tier sets these)
        if (!hasHeader(original.headers(), EventBusHeaders.ORIGINAL_TOPIC)) {
            addHeader(headers, EventBusHeaders.ORIGINAL_TOPIC, original.topic());
            addHeader(headers, EventBusHeaders.ORIGINAL_PARTITION, String.valueOf(original.partition()));
            addHeader(headers, EventBusHeaders.ORIGINAL_OFFSET, String.valueOf(original.offset()));
            addHeader(headers, EventBusHeaders.ORIGINAL_TIMESTAMP, String.valueOf(original.timestamp()));
        }

        // Retry metadata
        addHeader(headers, EventBusHeaders.BINDING_NAME, bindingName);
        addHeader(headers, EventBusHeaders.RETRY_TIER, String.valueOf(retryTier));
        addHeader(headers, EventBusHeaders.RETRY_ATTEMPT, String.valueOf(retryAttempt));
        addHeader(headers, EventBusHeaders.RETRY_TIMESTAMP, Instant.now().toString());

        // Tier-specific exception
        if (exception != null) {
            addHeader(headers, EventBusHeaders.tierException(retryTier), exception.getClass().getName());
            addHeader(headers, EventBusHeaders.tierExhaustedAt(retryTier), Instant.now().toString());
        }

        return headers;
    }

    private static void addHeader(RecordHeaders headers, String key, String value) {
        if (value != null) {
            headers.add(key, value.getBytes(StandardCharsets.UTF_8));
        }
    }

    private static boolean hasHeader(Headers headers, String key) {
        return headers.lastHeader(key) != null;
    }

    private static String truncate(String value, int maxLen) {
        if (value == null) {
            return "";
        }
        return value.length() <= maxLen ? value : value.substring(0, maxLen) + "...";
    }

    private static String truncateStackTrace(Throwable t) {
        var sw = new StringWriter();
        t.printStackTrace(new PrintWriter(sw));
        String full = sw.toString();
        return full.length() <= MAX_STACKTRACE_LENGTH
                ? full
                : full.substring(0, MAX_STACKTRACE_LENGTH) + "\n... [truncated]";
    }
}
