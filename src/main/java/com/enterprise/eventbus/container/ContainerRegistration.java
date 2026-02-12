package com.enterprise.eventbus.container;

import com.enterprise.eventbus.config.TopicBindingProperties;
import com.enterprise.eventbus.model.ContainerState;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;

import java.util.List;

/**
 * Immutable registration record for a binding's containers and associated components.
 */
public record ContainerRegistration(
        String bindingName,
        TopicBindingProperties config,
        ConcurrentMessageListenerContainer<?, ?> mainContainer,
        List<ConcurrentMessageListenerContainer<?, ?>> retryTierContainers,
        CircuitBreaker circuitBreaker,
        ContainerState state
) {

    public ContainerRegistration withState(ContainerState newState) {
        return new ContainerRegistration(
                bindingName, config, mainContainer, retryTierContainers, circuitBreaker, newState
        );
    }
}
