package com.enterprise.eventbus.model;

public enum ExceptionRouting {
    NEXT_TIER,
    SKIP_TO_TIER,
    DEAD_LETTER,
    PAUSE_CONTAINER
}
