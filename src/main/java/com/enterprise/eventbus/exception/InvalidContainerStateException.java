package com.enterprise.eventbus.exception;

import com.enterprise.eventbus.model.ContainerState;

public final class InvalidContainerStateException extends EventBusException {

    private final String bindingName;
    private final ContainerState currentState;
    private final String attemptedOperation;

    public InvalidContainerStateException(String bindingName, ContainerState currentState,
                                          String attemptedOperation) {
        super("Cannot %s binding '%s': current state is %s"
                .formatted(attemptedOperation, bindingName, currentState));
        this.bindingName = bindingName;
        this.currentState = currentState;
        this.attemptedOperation = attemptedOperation;
    }

    public String getBindingName() { return bindingName; }
    public ContainerState getCurrentState() { return currentState; }
    public String getAttemptedOperation() { return attemptedOperation; }
}
