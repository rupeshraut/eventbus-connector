package com.enterprise.eventbus.container;

import com.enterprise.eventbus.exception.InvalidContainerStateException;
import com.enterprise.eventbus.model.ContainerState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.stereotype.Component;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Manages lifecycle operations (start, stop, pause, resume, destroy) for
 * registered containers. Uses striped locking per binding name for concurrency safety.
 */
@Component
public class LifecycleManager {

    private static final Logger log = LoggerFactory.getLogger(LifecycleManager.class);

    private static final Set<ContainerState> STARTABLE = Set.of(ContainerState.CREATED, ContainerState.STOPPED);
    private static final Set<ContainerState> STOPPABLE = Set.of(ContainerState.RUNNING, ContainerState.PAUSED);
    private static final Set<ContainerState> PAUSABLE = Set.of(ContainerState.RUNNING);
    private static final Set<ContainerState> RESUMABLE = Set.of(ContainerState.PAUSED);

    private final ContainerRegistry registry;
    private final ConcurrentHashMap<String, ReentrantLock> locks = new ConcurrentHashMap<>();

    public LifecycleManager(ContainerRegistry registry) {
        this.registry = registry;
    }

    public void start(String bindingName) {
        withLock(bindingName, () -> {
            var reg = registry.getRequired(bindingName);
            validateTransition(bindingName, reg.state(), "start", STARTABLE);

            log.info("Starting containers for binding '{}'", bindingName);
            reg.mainContainer().start();
            reg.retryTierContainers().forEach(ConcurrentMessageListenerContainer::start);

            registry.updateState(bindingName, ContainerState.RUNNING);
            log.info("Binding '{}' is now RUNNING", bindingName);
        });
    }

    public void stop(String bindingName) {
        withLock(bindingName, () -> {
            var reg = registry.getRequired(bindingName);
            validateTransition(bindingName, reg.state(), "stop", STOPPABLE);

            log.info("Stopping containers for binding '{}'", bindingName);
            reg.retryTierContainers().forEach(ConcurrentMessageListenerContainer::stop);
            reg.mainContainer().stop();

            registry.updateState(bindingName, ContainerState.STOPPED);
            log.info("Binding '{}' is now STOPPED", bindingName);
        });
    }

    public void pause(String bindingName) {
        withLock(bindingName, () -> {
            var reg = registry.getRequired(bindingName);
            validateTransition(bindingName, reg.state(), "pause", PAUSABLE);

            log.info("Pausing containers for binding '{}'", bindingName);
            reg.mainContainer().pause();
            reg.retryTierContainers().forEach(ConcurrentMessageListenerContainer::pause);

            registry.updateState(bindingName, ContainerState.PAUSED);
            log.info("Binding '{}' is now PAUSED", bindingName);
        });
    }

    public void resume(String bindingName) {
        withLock(bindingName, () -> {
            var reg = registry.getRequired(bindingName);
            validateTransition(bindingName, reg.state(), "resume", RESUMABLE);

            log.info("Resuming containers for binding '{}'", bindingName);
            reg.mainContainer().resume();
            reg.retryTierContainers().forEach(ConcurrentMessageListenerContainer::resume);

            registry.updateState(bindingName, ContainerState.RUNNING);
            log.info("Binding '{}' is now RUNNING (resumed)", bindingName);
        });
    }

    public void destroy(String bindingName) {
        withLock(bindingName, () -> {
            var reg = registry.getRequired(bindingName);

            log.info("Destroying containers for binding '{}'", bindingName);

            // Stop first if running or paused
            if (reg.state() == ContainerState.RUNNING || reg.state() == ContainerState.PAUSED) {
                reg.retryTierContainers().forEach(c -> {
                    try { c.stop(); } catch (Exception e) { log.warn("Error stopping retry container", e); }
                });
                try { reg.mainContainer().stop(); } catch (Exception e) { log.warn("Error stopping main container", e); }
            }

            // Destroy all containers
            reg.retryTierContainers().forEach(c -> {
                try { c.destroy(); } catch (Exception e) { log.warn("Error destroying retry container", e); }
            });
            try { reg.mainContainer().destroy(); } catch (Exception e) { log.warn("Error destroying main container", e); }

            registry.unregister(bindingName);
            locks.remove(bindingName);
            log.info("Binding '{}' DESTROYED and unregistered", bindingName);
        });
    }

    /**
     * Pause triggered by circuit breaker — skips state validation for safety.
     */
    public void pauseForCircuitBreaker(String bindingName) {
        withLock(bindingName, () -> {
            var reg = registry.get(bindingName).orElse(null);
            if (reg == null || reg.state() != ContainerState.RUNNING) return;

            log.warn("Circuit breaker triggered PAUSE for binding '{}'", bindingName);
            reg.mainContainer().pause();
            reg.retryTierContainers().forEach(ConcurrentMessageListenerContainer::pause);
            registry.updateState(bindingName, ContainerState.PAUSED);
        });
    }

    /**
     * Resume triggered by circuit breaker — skips state validation for safety.
     */
    public void resumeForCircuitBreaker(String bindingName) {
        withLock(bindingName, () -> {
            var reg = registry.get(bindingName).orElse(null);
            if (reg == null || reg.state() != ContainerState.PAUSED) return;

            log.info("Circuit breaker triggered RESUME for binding '{}'", bindingName);
            reg.mainContainer().resume();
            reg.retryTierContainers().forEach(ConcurrentMessageListenerContainer::resume);
            registry.updateState(bindingName, ContainerState.RUNNING);
        });
    }

    private void validateTransition(String bindingName, ContainerState current,
                                     String operation, Set<ContainerState> validStates) {
        if (!validStates.contains(current)) {
            throw new InvalidContainerStateException(bindingName, current, operation);
        }
    }

    private void withLock(String bindingName, Runnable action) {
        var lock = locks.computeIfAbsent(bindingName, k -> new ReentrantLock());
        lock.lock();
        try {
            action.run();
        } finally {
            lock.unlock();
        }
    }
}
