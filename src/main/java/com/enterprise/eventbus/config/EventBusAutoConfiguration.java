package com.enterprise.eventbus.config;

import com.enterprise.eventbus.admin.EventBusAdmin;
import com.enterprise.eventbus.container.ContainerRegistry;
import com.enterprise.eventbus.container.LifecycleManager;
import com.enterprise.eventbus.model.ContainerState;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.SmartLifecycle;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.event.EventListener;
import org.springframework.data.mongodb.repository.config.EnableMongoRepositories;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.HashMap;
import java.util.Map;

/**
 * Auto-configuration for the Event Bus Connector.
 * Bootstraps all bindings from application properties on startup
 * and provides graceful shutdown via SmartLifecycle.
 */
@Configuration(proxyBeanMethods = false)
@EnableConfigurationProperties(EventBusProperties.class)
@ComponentScan(basePackages = "com.enterprise.eventbus")
@EnableMongoRepositories(basePackages = "com.enterprise.eventbus.dlq")
public class EventBusAutoConfiguration {

    private static final Logger log = LoggerFactory.getLogger(EventBusAutoConfiguration.class);

    /**
     * KafkaTemplate for DLT and retry-tier publishing.
     * Configured with acks=all, idempotence, and max retries for zero-loss guarantees.
     */
    @Bean
    public KafkaTemplate<byte[], byte[]> eventBusKafkaTemplate(
            org.springframework.boot.autoconfigure.kafka.KafkaProperties kafkaProperties) {
        Map<String, Object> props = new HashMap<>(kafkaProperties.buildProducerProperties(null));
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        props.put(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE);
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 1);

        var producerFactory = new DefaultKafkaProducerFactory<byte[], byte[]>(props);
        return new KafkaTemplate<>(producerFactory);
    }

    /**
     * Bootstrap all bindings from configuration on application ready.
     */
    @EventListener(ApplicationReadyEvent.class)
    public void bootstrapBindings(ApplicationReadyEvent event) {
        var properties = event.getApplicationContext().getBean(EventBusProperties.class);
        var admin = event.getApplicationContext().getBean(EventBusAdmin.class);

        properties.getBindings().forEach((name, config) -> {
            log.info("Bootstrapping binding '{}' from configuration", name);
            try {
                admin.register(name, config);
            } catch (Exception ex) {
                log.error("Failed to bootstrap binding '{}': {}", name, ex.getMessage(), ex);
                throw ex;
            }
        });

        log.info("Event Bus Connector bootstrapped: {} bindings registered",
                properties.getBindings().size());
    }

    /**
     * Graceful shutdown lifecycle bean. Ensures all containers are stopped
     * and offsets committed before the application shuts down.
     */
    @Bean
    public SmartLifecycle eventBusShutdownLifecycle(ContainerRegistry registry,
                                                     LifecycleManager lifecycleManager,
                                                     EventBusProperties properties) {
        return new SmartLifecycle() {
            private volatile boolean running = false;

            @Override
            public void start() { running = true; }

            @Override
            public void stop() {
                log.info("Event Bus Connector graceful shutdown initiated");
                long timeout = properties.getShutdownTimeoutMs();

                registry.listAll().forEach(reg -> {
                    try {
                        if (reg.state() == ContainerState.RUNNING
                                || reg.state() == ContainerState.PAUSED) {
                            log.info("Stopping binding '{}' during shutdown", reg.bindingName());
                            lifecycleManager.stop(reg.bindingName());
                        }
                    } catch (Exception ex) {
                        log.warn("Error stopping binding '{}' during shutdown: {}",
                                reg.bindingName(), ex.getMessage());
                    }
                });

                running = false;
                log.info("Event Bus Connector shutdown complete");
            }

            @Override
            public boolean isRunning() { return running; }

            @Override
            public int getPhase() { return Integer.MAX_VALUE - 100; }

            @Override
            public boolean isAutoStartup() { return true; }
        };
    }
}
