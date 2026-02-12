package com.enterprise.eventbus.exception;

public final class BindingNotFoundException extends EventBusException {

    public BindingNotFoundException(String bindingName) {
        super("Binding '%s' not found in registry".formatted(bindingName));
    }
}
