package com.enterprise.eventbus.model;

public enum BatchFailureStrategy {
    SEEK_TO_FAILED,
    DLQ_AND_CONTINUE
}
