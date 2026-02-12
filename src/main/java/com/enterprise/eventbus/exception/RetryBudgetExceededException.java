package com.enterprise.eventbus.exception;

public final class RetryBudgetExceededException extends EventBusException {

    public RetryBudgetExceededException(String message) {
        super(message);
    }
}
