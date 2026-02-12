package com.enterprise.eventbus.exception;

import java.util.List;

public final class ConfigurationValidationException extends EventBusException {

    private final List<String> violations;

    public ConfigurationValidationException(String bindingName, List<String> violations) {
        super("Configuration validation failed for binding '%s': %s"
                .formatted(bindingName, String.join("; ", violations)));
        this.violations = List.copyOf(violations);
    }

    public List<String> getViolations() { return violations; }
}
