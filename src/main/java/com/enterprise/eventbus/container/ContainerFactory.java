package com.enterprise.eventbus.container;

import com.enterprise.eventbus.config.ConfigurationValidator;
import com.enterprise.eventbus.config.RetryTierProperties;
import com.enterprise.eventbus.config.TopicBindingProperties;
import com.enterprise.eventbus.dlq.DltDocumentRepository;
import com.enterprise.eventbus.dlq.DltPublisher;
import com.enterprise.eventbus.handler.*;
import com.enterprise.eventbus.listener.BatchRecordListenerAdapter;
import com.enterprise.eventbus.listener.RetryTierListenerAdapter;
import com.enterprise.eventbus.listener.RetryTierPublisher;
import com.enterprise.eventbus.listener.SingleRecordListenerAdapter;
import com.enterprise.eventbus.model.ContainerState;
import com.enterprise.eventbus.model.ListenerType;
import com.enterprise.eventbus.offset.EventBusRebalanceListener;
import com.enterprise.eventbus.offset.PartitionOffsetManager;
import com.enterprise.eventbus.resilience.*;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryRegistry;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.stereotype.Component;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Factory that produces fully-wired {@link ConcurrentMessageListenerContainer} instances
 * from a {@link TopicBindingProperties} configuration.
 *
 * <p>Creates the main consumer container plus retry-tier containers, wires error handlers,
 * Resilience4j decorators, DLT publisher, and rebalance listeners.</p>
 */
@Component
public class ContainerFactory {

    private static final Logger log = LoggerFactory.getLogger(ContainerFactory.class);

    private final ConfigurationValidator validator;
    private final RetryConfigFactory retryConfigFactory;
    private final CircuitBreakerConfigFactory cbConfigFactory;
    private final DltDocumentRepository dltRepository;
    private final KafkaTemplate<byte[], byte[]> kafkaTemplate;
    private final MeterRegistry meterRegistry;
    private final RetryRegistry retryRegistry;
    private final CircuitBreakerRegistry cbRegistry;

    // Optional — user-provided handlers registered as beans
    private final Map<String, RecordHandler<?, ?>> recordHandlers;
    private final Map<String, BatchRecordHandler<?, ?>> batchRecordHandlers;
    private final Map<String, ExceptionClassifier> exceptionClassifiers;
    private final Map<String, DltAlertHandler> alertHandlers;

    public ContainerFactory(ConfigurationValidator validator,
                            RetryConfigFactory retryConfigFactory,
                            CircuitBreakerConfigFactory cbConfigFactory,
                            DltDocumentRepository dltRepository,
                            KafkaTemplate<byte[], byte[]> kafkaTemplate,
                            MeterRegistry meterRegistry,
                            Map<String, RecordHandler<?, ?>> recordHandlers,
                            Map<String, BatchRecordHandler<?, ?>> batchRecordHandlers,
                            Map<String, ExceptionClassifier> exceptionClassifiers,
                            Map<String, DltAlertHandler> alertHandlers) {
        this.validator = validator;
        this.retryConfigFactory = retryConfigFactory;
        this.cbConfigFactory = cbConfigFactory;
        this.dltRepository = dltRepository;
        this.kafkaTemplate = kafkaTemplate;
        this.meterRegistry = meterRegistry;
        this.retryRegistry = RetryRegistry.ofDefaults();
        this.cbRegistry = CircuitBreakerRegistry.ofDefaults();
        this.recordHandlers = recordHandlers;
        this.batchRecordHandlers = batchRecordHandlers;
        this.exceptionClassifiers = exceptionClassifiers;
        this.alertHandlers = alertHandlers;
    }

    /**
     * Creates a complete container registration for a binding.
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    public ContainerRegistration create(String bindingName, TopicBindingProperties config) {
        // Validate configuration coherence
        validator.validate(bindingName, config);

        log.info("Creating containers for binding '{}': topic={}, type={}, concurrency={}",
                bindingName, config.getTopic(), config.getListenerType(), config.getConcurrency());

        // Build consumer factory
        var consumerFactory = buildConsumerFactory(bindingName, config);

        // Build Resilience4j components
        Retry retry = retryConfigFactory.createRetry(
                bindingName, config.getRetry().getTier0(), retryRegistry);
        CircuitBreaker circuitBreaker = cbConfigFactory.create(
                bindingName, config.getCircuitBreaker(), cbRegistry);
        var resilienceDecorator = new ResilienceDecorator(retry, circuitBreaker);

        // Build exception classifier
        ExceptionClassifier classifier = exceptionClassifiers.getOrDefault(
                bindingName, new DefaultExceptionClassifier(config.getRetry()));

        // Build retry tier publisher
        List<RetryTierProperties> effectiveTiers = config.getRetry().getEffectiveTiers();
        var retryTierPublisher = new RetryTierPublisher(
                bindingName, config.getTopic(), effectiveTiers, kafkaTemplate);

        // Build DLT publisher
        DltAlertHandler alertHandler = alertHandlers.get(bindingName);
        var dltPublisher = new DltPublisher(
                bindingName, config, kafkaTemplate, dltRepository, alertHandler, meterRegistry);

        // Build safety valve
        var safetyValve = new PollIntervalSafetyValve(
                bindingName, config.getPollIntervalSafetyBudgetMs());

        // Build main container
        var containerProps = new ContainerProperties(config.getTopic());
        containerProps.setAckMode(config.getAckMode());
        containerProps.setGroupId(config.getGroupId());

        // Rebalance listener
        var offsetManager = new PartitionOffsetManager();
        containerProps.setConsumerRebalanceListener(
                new EventBusRebalanceListener(bindingName, () -> offsetManager));

        var mainContainer = new ConcurrentMessageListenerContainer<>(consumerFactory, containerProps);
        mainContainer.setConcurrency(config.getConcurrency());
        mainContainer.setAutoStartup(false); // Lifecycle manager controls startup
        mainContainer.setBeanName("eventbus-" + bindingName);

        // Wire listener based on type
        if (config.getListenerType() == ListenerType.SINGLE) {
            RecordHandler handler = recordHandlers.getOrDefault(bindingName, record -> {
                throw new UnsupportedOperationException(
                        "No RecordHandler registered for binding: " + bindingName);
            });
            var listener = new SingleRecordListenerAdapter(
                    bindingName, handler, resilienceDecorator, classifier,
                    dltPublisher, retryTierPublisher, safetyValve, meterRegistry);
            mainContainer.setupMessageListener(listener);
        } else {
            BatchRecordHandler handler = batchRecordHandlers.getOrDefault(bindingName, record -> {
                throw new UnsupportedOperationException(
                        "No BatchRecordHandler registered for binding: " + bindingName);
            });
            var listener = new BatchRecordListenerAdapter(
                    bindingName, handler, resilienceDecorator, classifier,
                    dltPublisher, retryTierPublisher, safetyValve,
                    config.getBatchFailureStrategy(), meterRegistry);
            containerProps.setMessageListener(listener);
        }

        // Build retry tier containers
        var retryContainers = buildRetryTierContainers(
                bindingName, config, effectiveTiers, resilienceDecorator,
                classifier, retryTierPublisher, dltPublisher);

        return new ContainerRegistration(
                bindingName, config, mainContainer, retryContainers,
                circuitBreaker, ContainerState.CREATED
        );
    }

    private ConsumerFactory<byte[], byte[]> buildConsumerFactory(String bindingName,
                                                                   TopicBindingProperties config) {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.GROUP_ID_CONFIG, config.getGroupId());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, config.getEffectiveMaxPollRecords());
        props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, (int) config.getMaxPollIntervalMs());
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, (int) config.getSessionTimeoutMs());
        props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, (int) config.getHeartbeatIntervalMs());
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);

        // Static membership
        if (config.isStaticMembership()) {
            String instanceId = generateInstanceId(bindingName);
            props.put(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, instanceId);
            log.info("Static membership enabled for '{}': instanceId={}", bindingName, instanceId);
        }

        return new DefaultKafkaConsumerFactory<>(props);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private List<ConcurrentMessageListenerContainer<?, ?>> buildRetryTierContainers(
            String bindingName,
            TopicBindingProperties config,
            List<RetryTierProperties> tiers,
            ResilienceDecorator resilienceDecorator,
            ExceptionClassifier classifier,
            RetryTierPublisher retryTierPublisher,
            DltPublisher dltPublisher) {

        var containers = new ArrayList<ConcurrentMessageListenerContainer<?, ?>>();

        for (int i = 0; i < tiers.size(); i++) {
            var tierConfig = tiers.get(i);
            int tierNumber = i + 1;
            String retryTopic = tierConfig.resolveTopicName(config.getTopic());

            var consumerFactory = buildConsumerFactory(bindingName + "-retry-" + tierNumber, config);

            var containerProps = new ContainerProperties(retryTopic);
            containerProps.setAckMode(config.getAckMode());
            containerProps.setGroupId(config.getGroupId() + "-retry-" + tierNumber);

            var container = new ConcurrentMessageListenerContainer<>(consumerFactory, containerProps);
            container.setConcurrency(tierConfig.getConcurrency());
            container.setAutoStartup(false);
            container.setBeanName("eventbus-" + bindingName + "-retry-" + tierNumber);

            var tierListener = new RetryTierListenerAdapter(
                    bindingName, tierNumber, tierConfig, resilienceDecorator,
                    classifier, retryTierPublisher, dltPublisher, container, meterRegistry);
            container.setupMessageListener(tierListener);

            containers.add(container);
            log.info("Created retry tier {} container for '{}': topic={}, delay={}ms",
                    tierNumber, bindingName, retryTopic, tierConfig.getDelayMs());
        }

        return List.copyOf(containers);
    }

    private String generateInstanceId(String bindingName) {
        try {
            String hostname = InetAddress.getLocalHost().getHostName();
            return bindingName + "-" + hostname + "-" + ProcessHandle.current().pid();
        } catch (Exception e) {
            return bindingName + "-" + ProcessHandle.current().pid();
        }
    }
}
