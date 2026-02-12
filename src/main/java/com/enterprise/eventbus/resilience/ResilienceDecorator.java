package com.enterprise.eventbus.resilience;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.retry.Retry;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Consumer;

/**
 * Composes Resilience4j decorators around handler invocations.
 * Decoration order (innermost to outermost): Handler → Retry → CircuitBreaker.
 *
 * <p>The Retry wraps the handler directly for transient failures.
 * The CircuitBreaker wraps the retry to prevent burning retries against a down dependency.</p>
 */
public class ResilienceDecorator {

    private static final Logger log = LoggerFactory.getLogger(ResilienceDecorator.class);

    private final Retry retry;
    private final CircuitBreaker circuitBreaker;

    public ResilienceDecorator(Retry retry, CircuitBreaker circuitBreaker) {
        this.retry = retry;
        this.circuitBreaker = circuitBreaker;
    }

    /**
     * Decorates a record processing action with retry and optional circuit breaker.
     *
     * @param action the handler action to decorate
     * @param record the consumer record (for logging context)
     * @param <K>    key type
     * @param <V>    value type
     * @return decorated runnable that throws on exhaustion
     */
    public <K, V> Runnable decorate(ThrowingConsumer<ConsumerRecord<K, V>> action,
                                     ConsumerRecord<K, V> record) {
        // Inner: wrap action in retry
        Runnable retried = Retry.decorateRunnable(retry, () -> {
            try {
                action.accept(record);
            } catch (Exception e) {
                throw (e instanceof RuntimeException re) ? re : new RuntimeException(e);
            }
        });

        // Outer: wrap retry in circuit breaker if present
        if (circuitBreaker != null) {
            return CircuitBreaker.decorateRunnable(circuitBreaker, retried);
        }

        return retried;
    }

    /**
     * Execute the decorated handler. Throws the terminal exception on failure.
     */
    public <K, V> void execute(ThrowingConsumer<ConsumerRecord<K, V>> action,
                                ConsumerRecord<K, V> record) throws Exception {
        try {
            decorate(action, record).run();
        } catch (RuntimeException ex) {
            // Unwrap if the cause is a checked exception
            if (ex.getCause() instanceof Exception checked) {
                throw checked;
            }
            throw ex;
        }
    }

    /**
     * Functional interface that allows checked exceptions from handler invocations.
     */
    @FunctionalInterface
    public interface ThrowingConsumer<T> {
        void accept(T t) throws Exception;
    }
}
