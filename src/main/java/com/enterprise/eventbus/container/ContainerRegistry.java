package com.enterprise.eventbus.container;

import com.enterprise.eventbus.exception.BindingAlreadyExistsException;
import com.enterprise.eventbus.exception.BindingNotFoundException;
import com.enterprise.eventbus.model.ContainerState;
import org.springframework.stereotype.Component;

import java.util.Collection;
import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Thread-safe registry of all container registrations, keyed by binding name.
 * Single source of truth for all live containers.
 */
@Component
public class ContainerRegistry {

    private final ConcurrentHashMap<String, ContainerRegistration> registrations = new ConcurrentHashMap<>();

    public void register(ContainerRegistration registration) {
        var existing = registrations.putIfAbsent(registration.bindingName(), registration);
        if (existing != null) {
            throw new BindingAlreadyExistsException(registration.bindingName());
        }
    }

    public ContainerRegistration unregister(String bindingName) {
        var removed = registrations.remove(bindingName);
        if (removed == null) {
            throw new BindingNotFoundException(bindingName);
        }
        return removed;
    }

    public Optional<ContainerRegistration> get(String bindingName) {
        return Optional.ofNullable(registrations.get(bindingName));
    }

    public ContainerRegistration getRequired(String bindingName) {
        return get(bindingName).orElseThrow(() -> new BindingNotFoundException(bindingName));
    }

    public Optional<ContainerRegistration> getByTopic(String topic) {
        return registrations.values().stream()
                .filter(r -> r.config().getTopic().equals(topic))
                .findFirst();
    }

    public Collection<ContainerRegistration> listAll() {
        return Collections.unmodifiableCollection(registrations.values());
    }

    public void updateState(String bindingName, ContainerState newState) {
        registrations.compute(bindingName, (key, existing) -> {
            if (existing == null) {
                throw new BindingNotFoundException(bindingName);
            }
            return existing.withState(newState);
        });
    }

    public int size() {
        return registrations.size();
    }

    public boolean contains(String bindingName) {
        return registrations.containsKey(bindingName);
    }
}
