package com.enterprise.eventbus.handler;

import com.enterprise.eventbus.config.RetryProperties;
import com.enterprise.eventbus.model.ExceptionRoutingDecision;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Configuration-driven exception classifier. Resolves exception class names
 * from the binding config into routing decisions.
 */
public class DefaultExceptionClassifier implements ExceptionClassifier {

    private static final Logger log = LoggerFactory.getLogger(DefaultExceptionClassifier.class);

    private final Set<String> nonRetryableExceptions;
    private final Map<String, Integer> skipToTierMapping;

    public DefaultExceptionClassifier(RetryProperties retryProperties) {
        this.nonRetryableExceptions = Set.copyOf(retryProperties.getNonRetryableExceptions());
        this.skipToTierMapping = Map.copyOf(retryProperties.getSkipToTierMapping());
    }

    @Override
    public ExceptionRoutingDecision classify(Throwable exception, int currentTier) {
        String exceptionName = exception.getClass().getName();

        // Check non-retryable first — these go straight to DLT
        if (isNonRetryable(exception)) {
            log.debug("Exception classified as non-retryable: {}", exceptionName);
            return ExceptionRoutingDecision.deadLetter();
        }

        // Check skip-to-tier mapping
        Integer targetTier = findSkipToTier(exception);
        if (targetTier != null && targetTier > currentTier) {
            log.debug("Exception classified for skip-to-tier {}: {}", targetTier, exceptionName);
            return ExceptionRoutingDecision.skipToTier(targetTier);
        }

        // Default: next tier
        return ExceptionRoutingDecision.nextTier();
    }

    private boolean isNonRetryable(Throwable exception) {
        return matchesAny(exception, nonRetryableExceptions);
    }

    private Integer findSkipToTier(Throwable exception) {
        for (var entry : skipToTierMapping.entrySet()) {
            if (matchesException(exception, entry.getKey())) {
                return entry.getValue();
            }
        }
        return null;
    }

    private boolean matchesAny(Throwable exception, Set<String> classNames) {
        return classNames.stream().anyMatch(name -> matchesException(exception, name));
    }

    /**
     * Matches exception by class name or simple name, walking the cause chain.
     */
    private boolean matchesException(Throwable exception, String className) {
        Throwable current = exception;
        while (current != null) {
            if (current.getClass().getName().equals(className)
                    || current.getClass().getSimpleName().equals(className)) {
                return true;
            }
            current = current.getCause();
        }
        return false;
    }
}
