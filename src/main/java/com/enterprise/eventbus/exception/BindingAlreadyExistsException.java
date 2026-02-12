package com.enterprise.eventbus.exception;

public final class BindingAlreadyExistsException extends EventBusException {

    public BindingAlreadyExistsException(String bindingName) {
        super("Binding '%s' is already registered".formatted(bindingName));
    }
}
