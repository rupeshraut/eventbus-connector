package com.enterprise.eventbus.observability;

import com.enterprise.eventbus.container.ContainerRegistry;
import com.enterprise.eventbus.model.ContainerState;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.stereotype.Component;

import java.util.HashMap;

/**
 * Spring Boot health indicator that reports the aggregate health of all
 * registered event bus containers and circuit breakers.
 */
@Component
public class EventBusHealthIndicator implements HealthIndicator {

    private final ContainerRegistry registry;

    public EventBusHealthIndicator(ContainerRegistry registry) {
        this.registry = registry;
    }

    @Override
    public Health health() {
        var registrations = registry.listAll();

        if (registrations.isEmpty()) {
            return Health.up()
                    .withDetail("status", "No bindings registered")
                    .build();
        }

        var details = new HashMap<String, Object>();
        boolean anyDown = false;
        boolean anyDegraded = false;

        for (var registration : registrations) {
            String name = registration.bindingName();
            ContainerState state = registration.state();
            details.put(name + ".state", state.name());

            // Check circuit breaker
            if (registration.circuitBreaker() != null) {
                var cbState = registration.circuitBreaker().getState().name();
                details.put(name + ".circuitBreaker", cbState);
                if ("OPEN".equals(cbState) || "FORCED_OPEN".equals(cbState)) {
                    anyDegraded = true;
                }
            }

            // Check container state
            if (state == ContainerState.DESTROYED) {
                anyDown = true;
            } else if (state == ContainerState.PAUSED || state == ContainerState.STOPPED) {
                anyDegraded = true;
            }
        }

        details.put("totalBindings", registrations.size());

        if (anyDown) {
            return Health.down().withDetails(details).build();
        } else if (anyDegraded) {
            return Health.status("DEGRADED").withDetails(details).build();
        }
        return Health.up().withDetails(details).build();
    }
}
