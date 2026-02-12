package com.enterprise.eventbus.observability;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.MDC;

/**
 * Propagates structured logging context (MDC) for every record processed.
 * Thread-confined — safe for consumer thread usage.
 */
public final class MdcContextPropagator {

    private MdcContextPropagator() {}

    private static final String BINDING = "eventbus.binding";
    private static final String TOPIC = "eventbus.topic";
    private static final String PARTITION = "eventbus.partition";
    private static final String OFFSET = "eventbus.offset";
    private static final String GROUP = "eventbus.group";

    public static void setContext(String bindingName, ConsumerRecord<?, ?> record) {
        MDC.put(BINDING, bindingName);
        MDC.put(TOPIC, record.topic());
        MDC.put(PARTITION, String.valueOf(record.partition()));
        MDC.put(OFFSET, String.valueOf(record.offset()));
    }

    public static void setContext(String bindingName, String topic, int partition, long offset) {
        MDC.put(BINDING, bindingName);
        MDC.put(TOPIC, topic);
        MDC.put(PARTITION, String.valueOf(partition));
        MDC.put(OFFSET, String.valueOf(offset));
    }

    public static void clear() {
        MDC.remove(BINDING);
        MDC.remove(TOPIC);
        MDC.remove(PARTITION);
        MDC.remove(OFFSET);
        MDC.remove(GROUP);
    }
}
