package com.enterprise.eventbus.config;

import jakarta.validation.Valid;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

import java.util.HashMap;
import java.util.Map;

/**
 * Root configuration properties for the Event Bus Connector.
 *
 * <pre>
 * eventbus:
 *   bindings:
 *     orders:
 *       topic: orders.events
 *       group-id: order-processing-group
 *       ...
 * </pre>
 */
@ConfigurationProperties(prefix = "eventbus")
@Validated
public class EventBusProperties {

    @Valid
    private Map<String, TopicBindingProperties> bindings = new HashMap<>();

    /** Global default for graceful shutdown timeout in milliseconds. */
    private long shutdownTimeoutMs = 30_000L;

    /** Hostname for consumer instance identification (auto-detected if blank). */
    private String hostname = "";

    public Map<String, TopicBindingProperties> getBindings() { return bindings; }
    public void setBindings(Map<String, TopicBindingProperties> bindings) { this.bindings = bindings; }

    public long getShutdownTimeoutMs() { return shutdownTimeoutMs; }
    public void setShutdownTimeoutMs(long shutdownTimeoutMs) { this.shutdownTimeoutMs = shutdownTimeoutMs; }

    public String getHostname() { return hostname; }
    public void setHostname(String hostname) { this.hostname = hostname; }
}
