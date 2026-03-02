package com.enterprise.eventbus.config;

public class CircuitBreakerProperties {

    private boolean enabled;
    private float failureRateThreshold = 50.0F;
    private float slowCallRateThreshold = 80.0F;
    private long slowCallDurationMs = 5000L;
    private long waitDurationInOpenStateMs = 60_000L;
    private int slidingWindowSize = 100;
    private int minimumNumberOfCalls = 10;
    private int permittedNumberOfCallsInHalfOpenState = 5;
    private boolean automaticPauseOnOpen = true;

    // -- Getters / Setters --

    public boolean isEnabled() { return enabled; }
    public void setEnabled(boolean enabled) { this.enabled = enabled; }

    public float getFailureRateThreshold() { return failureRateThreshold; }
    public void setFailureRateThreshold(float failureRateThreshold) { this.failureRateThreshold = failureRateThreshold; }

    public float getSlowCallRateThreshold() { return slowCallRateThreshold; }
    public void setSlowCallRateThreshold(float slowCallRateThreshold) { this.slowCallRateThreshold = slowCallRateThreshold; }

    public long getSlowCallDurationMs() { return slowCallDurationMs; }
    public void setSlowCallDurationMs(long slowCallDurationMs) { this.slowCallDurationMs = slowCallDurationMs; }

    public long getWaitDurationInOpenStateMs() { return waitDurationInOpenStateMs; }
    public void setWaitDurationInOpenStateMs(long waitDurationInOpenStateMs) { this.waitDurationInOpenStateMs = waitDurationInOpenStateMs; }

    public int getSlidingWindowSize() { return slidingWindowSize; }
    public void setSlidingWindowSize(int slidingWindowSize) { this.slidingWindowSize = slidingWindowSize; }

    public int getMinimumNumberOfCalls() { return minimumNumberOfCalls; }
    public void setMinimumNumberOfCalls(int minimumNumberOfCalls) { this.minimumNumberOfCalls = minimumNumberOfCalls; }

    public int getPermittedNumberOfCallsInHalfOpenState() { return permittedNumberOfCallsInHalfOpenState; }
    public void setPermittedNumberOfCallsInHalfOpenState(int val) { this.permittedNumberOfCallsInHalfOpenState = val; }

    public boolean isAutomaticPauseOnOpen() { return automaticPauseOnOpen; }
    public void setAutomaticPauseOnOpen(boolean automaticPauseOnOpen) { this.automaticPauseOnOpen = automaticPauseOnOpen; }
}
