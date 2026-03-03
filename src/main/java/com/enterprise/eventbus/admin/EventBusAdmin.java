package com.enterprise.eventbus.admin;

import com.enterprise.eventbus.config.TopicBindingProperties;
import com.enterprise.eventbus.container.*;
import com.enterprise.eventbus.dlq.DltReplayService;
import com.enterprise.eventbus.dlq.DltStatsService;
import com.enterprise.eventbus.model.ContainerState;
import com.enterprise.eventbus.resilience.CircuitBreakerLifecycleBridge;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;

@Component
public class EventBusAdmin {

    private static final Logger log = LoggerFactory.getLogger(EventBusAdmin.class);

    private final ContainerFactory factory;
    private final ContainerRegistry registry;
    private final LifecycleManager lifecycleManager;
    private final DltReplayService replayService;
    private final DltStatsService statsService;
    private final ConcurrentHashMap<String, CircuitBreakerLifecycleBridge> cbBridges
            = new ConcurrentHashMap<>();

    public EventBusAdmin(ContainerFactory factory, ContainerRegistry registry,
                         LifecycleManager lifecycleManager, DltReplayService replayService,
                         DltStatsService statsService) {
        this.factory = factory;
        this.registry = registry;
        this.lifecycleManager = lifecycleManager;
        this.replayService = replayService;
        this.statsService = statsService;
    }

    public void register(String bindingName, TopicBindingProperties config) {
        log.info("Registering binding '{}'", bindingName);
        var registration = factory.create(bindingName, config);
        registry.register(registration);

        if (registration.circuitBreaker() != null
                && config.getCircuitBreaker().isAutomaticPauseOnOpen()) {
            var bridge = new CircuitBreakerLifecycleBridge(bindingName,
                    registration.circuitBreaker(),
                    () -> lifecycleManager.pauseForCircuitBreaker(bindingName),
                    () -> lifecycleManager.resumeForCircuitBreaker(bindingName));
            bridge.activate();
            cbBridges.put(bindingName, bridge);
        }

        if (config.isAutoStartup()) {
            lifecycleManager.start(bindingName);
        }
    }

    public void start(String name) { lifecycleManager.start(name); }
    public void stop(String name) { lifecycleManager.stop(name); }
    public void pause(String name) { lifecycleManager.pause(name); }
    public void resume(String name) { lifecycleManager.resume(name); }

    public void destroy(String name) {
        var bridge = cbBridges.remove(name);
        if (bridge != null) {
            bridge.deactivate();
        }
        lifecycleManager.destroy(name);
    }

    public ContainerState getState(String name) {
        return registry.getRequired(name).state();
    }

    public Collection<ContainerRegistration> listBindings() {
        return registry.listAll();
    }

    // -- DLT Operations --

    public int replayDlt(String bindingName, int count) {
        var config = registry.getRequired(bindingName).config();
        return replayService.replayToOriginalTopic(bindingName, config.getTopic(),
                config.getDlt().getReplay().getMaxReplays(), count);
    }

    public int replayDltByTimeWindow(String bindingName, Instant from, Instant to) {
        var config = registry.getRequired(bindingName).config();
        return replayService.replayByTimeWindow(bindingName, config.getTopic(), from, to);
    }

    public int replayDltToTier(String bindingName, int tierNumber, int count) {
        var config = registry.getRequired(bindingName).config();
        var tiers = config.getRetry().getEffectiveTiers();
        if (tierNumber < 1 || tierNumber > tiers.size()) {
            throw new IllegalArgumentException("Tier %d out of range [1..%d]"
                    .formatted(tierNumber, tiers.size()));
        }
        String retryTopic = tiers.get(tierNumber - 1).resolveTopicName(config.getTopic());
        return replayService.replayToRetryTier(bindingName, retryTopic,
                config.getDlt().getReplay().getMaxReplays(), count);
    }

    public long purgeDlt(String bindingName, Instant olderThan) {
        return replayService.purge(bindingName, olderThan);
    }

    public DltStatsService.DltStats dltStats(String bindingName) {
        return statsService.getStats(bindingName);
    }
}
