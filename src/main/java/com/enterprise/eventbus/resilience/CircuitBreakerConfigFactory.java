package com.enterprise.eventbus.resilience;

import com.enterprise.eventbus.config.CircuitBreakerProperties;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.time.Duration;

/**
 * Factory for creating Resilience4j CircuitBreaker instances from binding properties.
 */
@Component
public class CircuitBreakerConfigFactory {

    private static final Logger log = LoggerFactory.getLogger(CircuitBreakerConfigFactory.class);

    /**
     * Creates a CircuitBreaker instance for the binding. Returns null if CB is disabled.
     */
    public CircuitBreaker create(String bindingName, CircuitBreakerProperties props,
                                  CircuitBreakerRegistry registry) {
        if (!props.isEnabled()) {
            log.info("CircuitBreaker disabled for binding '{}'", bindingName);
            return null;
        }

        var config = CircuitBreakerConfig.custom()
                .failureRateThreshold(props.getFailureRateThreshold())
                .slowCallRateThreshold(props.getSlowCallRateThreshold())
                .slowCallDurationThreshold(Duration.ofMillis(props.getSlowCallDurationMs()))
                .waitDurationInOpenState(Duration.ofMillis(props.getWaitDurationInOpenStateMs()))
                .slidingWindowSize(props.getSlidingWindowSize())
                .minimumNumberOfCalls(props.getMinimumNumberOfCalls())
                .permittedNumberOfCallsInHalfOpenState(props.getPermittedNumberOfCallsInHalfOpenState())
                .automaticTransitionFromOpenToHalfOpenEnabled(true)
                .build();

        var circuitBreaker = registry.circuitBreaker(bindingName, config);

        log.info("Created CircuitBreaker for '{}': failureRate={}%, slowCallRate={}%, "
                + "slidingWindow={}, waitInOpen={}ms",
                bindingName, props.getFailureRateThreshold(), props.getSlowCallRateThreshold(),
                props.getSlidingWindowSize(), props.getWaitDurationInOpenStateMs());

        return circuitBreaker;
    }
}
