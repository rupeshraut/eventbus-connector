package com.enterprise.eventbus.config;

import com.enterprise.eventbus.exception.ConfigurationValidationException;
import com.enterprise.eventbus.model.OrderingMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

/**
 * Validates the mathematical coherence and logical consistency of binding configurations.
 * Runs at startup and when dynamic bindings are registered.
 */
@Component
public class ConfigurationValidator {

    private static final Logger log = LoggerFactory.getLogger(ConfigurationValidator.class);

    /**
     * Validates a single binding. Throws on failure.
     */
    public void validate(String bindingName, TopicBindingProperties binding) {
        var violations = new ArrayList<String>();

        validateSessionHeartbeat(binding, violations);
        validateRetryBudget(binding, violations);
        validateTierDelayOrdering(binding, violations);
        validateExceptionClassification(binding, violations);
        validateStrictOrderingCompatibility(binding, violations);
        validateCircuitBreakerRequirements(binding, violations);

        if (!violations.isEmpty()) {
            throw new ConfigurationValidationException(bindingName, violations);
        }

        log.info("Configuration validated successfully for binding '{}'", bindingName);
    }

    /**
     * Validates all bindings in the properties.
     */
    public void validateAll(EventBusProperties properties) {
        properties.getBindings().forEach(this::validate);
    }

    private void validateSessionHeartbeat(TopicBindingProperties binding, List<String> violations) {
        if (binding.getSessionTimeoutMs() < 3 * binding.getHeartbeatIntervalMs()) {
            violations.add(
                    "session.timeout.ms (%d) must be >= 3 × heartbeat.interval.ms (%d)"
                            .formatted(binding.getSessionTimeoutMs(), binding.getHeartbeatIntervalMs())
            );
        }
    }

    private void validateRetryBudget(TopicBindingProperties binding, List<String> violations) {
        long tier0WorstCase = binding.getRetry().getTier0().calculateWorstCaseDurationMs();
        long maxBudget = binding.getPollIntervalSafetyBudgetMs();
        long worstCasePerPoll = tier0WorstCase * binding.getEffectiveMaxPollRecords();

        if (worstCasePerPoll > maxBudget) {
            violations.add(
                    "Retry budget exceeded: tier0 worst-case (%dms) × max.poll.records (%d) = %dms, "
                            .formatted(tier0WorstCase, binding.getEffectiveMaxPollRecords(), worstCasePerPoll)
                    + "which exceeds safety budget of %dms (%.0f%% of max.poll.interval.ms=%dms). "
                            .formatted(maxBudget, binding.getPollIntervalSafetyThreshold() * 100,
                                    binding.getMaxPollIntervalMs())
                    + "Reduce max.poll.records, retry attempts, or increase max.poll.interval.ms"
            );
        }
    }

    private void validateTierDelayOrdering(TopicBindingProperties binding, List<String> violations) {
        var tiers = binding.getRetry().getEffectiveTiers();
        for (int i = 1; i < tiers.size(); i++) {
            if (tiers.get(i).getDelayMs() <= tiers.get(i - 1).getDelayMs()) {
                violations.add(
                        "Retry tier delays must be strictly increasing: tier-%d delay (%dms) <= tier-%d delay (%dms)"
                                .formatted(i + 1, tiers.get(i).getDelayMs(), i, tiers.get(i - 1).getDelayMs())
                );
            }
        }
    }

    private void validateExceptionClassification(TopicBindingProperties binding, List<String> violations) {
        var retry = binding.getRetry();
        var overlap = retry.getRetryableExceptions().stream()
                .filter(retry.getNonRetryableExceptions()::contains)
                .toList();
        if (!overlap.isEmpty()) {
            violations.add("Exceptions cannot be both retryable and non-retryable: " + overlap);
        }

        var tiers = retry.getEffectiveTiers();
        retry.getSkipToTierMapping().forEach((exClass, tierNum) -> {
            if (tierNum < 1 || tierNum > tiers.size()) {
                violations.add("Skip-to-tier target %d for %s is out of range [1..%d]"
                        .formatted(tierNum, exClass, tiers.size()));
            }
        });
    }

    private void validateStrictOrderingCompatibility(TopicBindingProperties binding, List<String> violations) {
        if (binding.getOrderingMode() == OrderingMode.STRICT
                && !binding.getRetry().getEffectiveTiers().isEmpty()
                && !binding.getRetry().getTiers().isEmpty()) {
            violations.add(
                    "STRICT ordering mode is incompatible with topic-based retry tiers. "
                    + "Either set orderingMode=RELAXED_IN_RETRY or remove explicit retry tiers "
                    + "(only in-memory tier-0 retries will be used)"
            );
        }
    }

    private void validateCircuitBreakerRequirements(TopicBindingProperties binding, List<String> violations) {
        if (binding.getCircuitBreaker().isEnabled()
                && binding.getCircuitBreaker().getMinimumNumberOfCalls() > binding.getCircuitBreaker().getSlidingWindowSize()) {
            violations.add(
                    "CircuitBreaker minimumNumberOfCalls (%d) cannot exceed slidingWindowSize (%d)"
                            .formatted(binding.getCircuitBreaker().getMinimumNumberOfCalls(),
                                    binding.getCircuitBreaker().getSlidingWindowSize())
            );
        }
    }
}
