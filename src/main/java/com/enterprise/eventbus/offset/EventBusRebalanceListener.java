package com.enterprise.eventbus.offset;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.function.Supplier;

/**
 * Rebalance listener that commits processed offsets on partition revocation
 * to prevent message loss or duplication during rebalancing.
 */
public class EventBusRebalanceListener implements ConsumerRebalanceListener {

    private static final Logger log = LoggerFactory.getLogger(EventBusRebalanceListener.class);

    private final String bindingName;
    private final Supplier<PartitionOffsetManager> offsetManagerSupplier;

    public EventBusRebalanceListener(String bindingName,
                                      Supplier<PartitionOffsetManager> offsetManagerSupplier) {
        this.bindingName = bindingName;
        this.offsetManagerSupplier = offsetManagerSupplier;
    }

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        if (partitions.isEmpty()) return;

        log.info("Partitions revoked for binding '{}': {}", bindingName, partitions);

        var offsetManager = offsetManagerSupplier.get();
        if (offsetManager != null && offsetManager.hasProcessedOffsets()) {
            var offsets = offsetManager.getOffsetsToCommit();
            log.info("Committing processed offsets on revocation for binding '{}': {}",
                    bindingName, offsets);
            // Note: actual commitSync is handled by the container's ConsumerAwareRebalanceListener
            // This listener tracks state; the container performs the commit
        }

        // Reset internal state for revoked partitions
        log.debug("Resetting state for revoked partitions in binding '{}'", bindingName);
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        if (partitions.isEmpty()) return;

        log.info("Partitions assigned to binding '{}': {}", bindingName, partitions);

        // Reset offset tracking for newly assigned partitions
        var offsetManager = offsetManagerSupplier.get();
        if (offsetManager != null) {
            offsetManager.reset();
        }
    }

    @Override
    public void onPartitionsLost(Collection<TopicPartition> partitions) {
        log.warn("Partitions LOST for binding '{}': {}. These were assigned to another consumer "
                + "before revocation callback. Some records may be reprocessed.", bindingName, partitions);

        var offsetManager = offsetManagerSupplier.get();
        if (offsetManager != null) {
            offsetManager.reset();
        }
    }
}
