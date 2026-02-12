package com.enterprise.eventbus.resilience;

import com.enterprise.eventbus.config.Tier0RetryProperties;
import io.github.resilience4j.retry.RetryConfig;
import io.github.resilience4j.retry.RetryRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.time.Duration;

/**
 * Factory for creating Resilience4j Retry configurations from binding properties.
 */
@Component
public class RetryConfigFactory {

    private static final Logger log = LoggerFactory.getLogger(RetryConfigFactory.class);

    /**
     * Creates a Resilience4j RetryConfig for tier-0 in-memory retries.
     */
    public RetryConfig createTier0Config(String bindingName, Tier0RetryProperties props) {
        var config = RetryConfig.custom()
                .maxAttempts(props.getMaxAttempts())
                .waitDuration(Duration.ofMillis(props.getInitialBackoffMs()))
                .intervalFunction(attempt -> calculateBackoff(attempt, props))
                .retryOnException(ex -> true)  // all exceptions retryable at tier-0; classifier runs after
                .failAfterMaxAttempts(true)
                .build();

        log.info("Created tier-0 retry config for '{}': maxAttempts={}, initialBackoff={}ms, multiplier={}",
                bindingName, props.getMaxAttempts(), props.getInitialBackoffMs(), props.getMultiplier());

        return config;
    }

    /**
     * Creates a named Retry instance registered in the global registry.
     */
    public io.github.resilience4j.retry.Retry createRetry(String bindingName,
                                                            Tier0RetryProperties props,
                                                            RetryRegistry registry) {
        var config = createTier0Config(bindingName, props);
        return registry.retry(bindingName + "-tier0", config);
    }

    private long calculateBackoff(int attempt, Tier0RetryProperties props) {
        long backoff = (long) (props.getInitialBackoffMs() * Math.pow(props.getMultiplier(), attempt - 1));
        return Math.min(backoff, props.getMaxBackoffMs());
    }
}
