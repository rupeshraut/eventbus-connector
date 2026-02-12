package com.enterprise.eventbus.resilience;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.event.CircuitBreakerOnStateTransitionEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Consumer;

/**
 * Bridges Resilience4j CircuitBreaker state transitions to container lifecycle operations.
 * When the circuit opens, the container is paused. When it transitions to half-open, the container resumes.
 */
public class CircuitBreakerLifecycleBridge {

    private static final Logger log = LoggerFactory.getLogger(CircuitBreakerLifecycleBridge.class);

    private final String bindingName;
    private final CircuitBreaker circuitBreaker;
    private final Runnable pauseAction;
    private final Runnable resumeAction;

    public CircuitBreakerLifecycleBridge(String bindingName,
                                         CircuitBreaker circuitBreaker,
                                         Runnable pauseAction,
                                         Runnable resumeAction) {
        this.bindingName = bindingName;
        this.circuitBreaker = circuitBreaker;
        this.pauseAction = pauseAction;
        this.resumeAction = resumeAction;
    }

    /**
     * Registers state transition listeners on the circuit breaker.
     */
    public void activate() {
        if (circuitBreaker == null) {
            return;
        }

        circuitBreaker.getEventPublisher()
                .onStateTransition(this::onStateTransition);

        log.info("CircuitBreaker lifecycle bridge activated for binding '{}'", bindingName);
    }

    private void onStateTransition(CircuitBreakerOnStateTransitionEvent event) {
        var transition = event.getStateTransition();

        switch (transition) {
            case CLOSED_TO_OPEN, CLOSED_TO_FORCED_OPEN, HALF_OPEN_TO_OPEN -> {
                log.warn("CircuitBreaker OPENED for binding '{}' — pausing container. Transition: {}",
                        bindingName, transition);
                try {
                    pauseAction.run();
                } catch (Exception e) {
                    log.error("Failed to pause container for binding '{}' on circuit open", bindingName, e);
                }
            }
            case OPEN_TO_HALF_OPEN -> {
                log.info("CircuitBreaker HALF_OPEN for binding '{}' — resuming container for probe",
                        bindingName);
                try {
                    resumeAction.run();
                } catch (Exception e) {
                    log.error("Failed to resume container for binding '{}' on half-open", bindingName, e);
                }
            }
            case HALF_OPEN_TO_CLOSED, FORCED_OPEN_TO_CLOSED, FORCED_OPEN_TO_HALF_OPEN -> {
                log.info("CircuitBreaker CLOSED for binding '{}' — normal processing resumed. Transition: {}",
                        bindingName, transition);
                try {
                    resumeAction.run();
                } catch (Exception e) {
                    log.error("Failed to resume container for binding '{}' on circuit close", bindingName, e);
                }
            }
        }
    }

    public void deactivate() {
        // CircuitBreaker doesn't support removing listeners, but the bridge
        // becomes a no-op once the container is destroyed
        log.info("CircuitBreaker lifecycle bridge deactivated for binding '{}'", bindingName);
    }
}
