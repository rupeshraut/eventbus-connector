package com.enterprise.eventbus.exception;

/**
 * Base exception for all event bus connector errors.
 * Sealed to restrict the exception hierarchy to known subtypes.
 */
public sealed class EventBusException extends RuntimeException
        permits InvalidContainerStateException,
                BindingAlreadyExistsException,
                BindingNotFoundException,
                ConfigurationValidationException,
                BatchProcessingException,
                DltPublishException,
                RetryBudgetExceededException {

    public EventBusException(String message) {
        super(message);
    }

    public EventBusException(String message, Throwable cause) {
        super(message, cause);
    }
}
