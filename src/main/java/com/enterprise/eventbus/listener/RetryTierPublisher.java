package com.enterprise.eventbus.listener;

import com.enterprise.eventbus.config.RetryTierProperties;
import com.enterprise.eventbus.dlq.DltHeaderEnricher;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Publishes records to retry-tier topics with appropriate header enrichment.
 * Synchronous send to guarantee no message loss.
 */
public class RetryTierPublisher {

    private static final Logger log = LoggerFactory.getLogger(RetryTierPublisher.class);
    private static final long SEND_TIMEOUT_SECONDS = 30;

    private final String bindingName;
    private final String baseTopic;
    private final List<RetryTierProperties> tiers;
    private final KafkaTemplate<byte[], byte[]> kafkaTemplate;

    public RetryTierPublisher(String bindingName,
                               String baseTopic,
                               List<RetryTierProperties> tiers,
                               KafkaTemplate<byte[], byte[]> kafkaTemplate) {
        this.bindingName = bindingName;
        this.baseTopic = baseTopic;
        this.tiers = tiers;
        this.kafkaTemplate = kafkaTemplate;
    }

    /**
     * Publish a failed record to a specific retry tier.
     *
     * @param record     the original consumer record
     * @param tierNumber 1-based tier number
     * @param attempt    attempt number within this tier
     * @param exception  the exception that caused the failure
     */
    public void publishToTier(ConsumerRecord<?, ?> record, int tierNumber,
                               int attempt, Throwable exception) {
        if (tierNumber < 1 || tierNumber > tiers.size()) {
            log.error("Invalid retry tier {} for binding '{}'. Max tier: {}. "
                    + "This record should go to DLT.", tierNumber, bindingName, tiers.size());
            return;
        }

        var tierConfig = tiers.get(tierNumber - 1);
        String retryTopic = tierConfig.resolveTopicName(baseTopic);

        var headers = DltHeaderEnricher.createRetryHeaders(
                record, bindingName, tierNumber, attempt, exception);

        byte[] keyBytes = extractBytes(record.key());
        byte[] valueBytes = extractBytes(record.value());

        var producerRecord = new ProducerRecord<>(retryTopic, null,
                record.timestamp(), keyBytes, valueBytes, headers);

        try {
            kafkaTemplate.send(producerRecord).get(SEND_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            log.info("Published to retry tier {} topic [{}] for binding '{}': offset={}",
                    tierNumber, retryTopic, bindingName, record.offset());
        } catch (Exception ex) {
            log.error("CRITICAL: Failed to publish to retry tier {} topic [{}] for binding '{}'. "
                    + "Record will NOT be acknowledged.", tierNumber, retryTopic, bindingName, ex);
            throw new RuntimeException("Failed to publish to retry tier " + tierNumber, ex);
        }
    }

    /**
     * Check if a higher tier exists beyond the given tier number.
     */
    public boolean hasNextTier(int currentTier) {
        return currentTier < tiers.size();
    }

    /**
     * Get the total number of configured retry tiers.
     */
    public int getTierCount() {
        return tiers.size();
    }

    private byte[] extractBytes(Object value) {
        if (value == null) {
            return new byte[0];
        }
        if (value instanceof byte[] bytes) {
            return bytes;
        }
        return value.toString().getBytes(StandardCharsets.UTF_8);
    }
}
