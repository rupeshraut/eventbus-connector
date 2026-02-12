package com.enterprise.eventbus.dlq;

import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Provides statistics and analytics for dead-letter records via MongoDB aggregation.
 */
@Service
public class DltStatsService {

    private final DltDocumentRepository repository;
    private final MongoTemplate mongoTemplate;

    public DltStatsService(DltDocumentRepository repository, MongoTemplate mongoTemplate) {
        this.repository = repository;
        this.mongoTemplate = mongoTemplate;
    }

    /**
     * Returns comprehensive DLT statistics for a binding.
     */
    public DltStats getStats(String bindingName) {
        long totalCount = repository.countByBindingName(bindingName);
        var oldest = repository.findFirstByBindingNameOrderByCreatedAtAsc(bindingName);
        var newest = repository.findFirstByBindingNameOrderByCreatedAtDesc(bindingName);

        var exceptionBreakdown = getExceptionBreakdown(bindingName);

        return new DltStats(
                bindingName,
                totalCount,
                oldest != null ? oldest.getCreatedAt() : null,
                newest != null ? newest.getCreatedAt() : null,
                exceptionBreakdown
        );
    }

    private Map<String, Long> getExceptionBreakdown(String bindingName) {
        var aggregation = Aggregation.newAggregation(
                Aggregation.match(Criteria.where("bindingName").is(bindingName)),
                Aggregation.group("lastExceptionClass").count().as("count"),
                Aggregation.project("count").and("_id").as("exceptionClass")
        );

        var results = mongoTemplate.aggregate(
                aggregation, "eventbus_dlt_records", ExceptionCount.class);

        return results.getMappedResults().stream()
                .collect(Collectors.toMap(
                        ExceptionCount::getExceptionClass,
                        ExceptionCount::getCount
                ));
    }

    // -- Result types --

    public record DltStats(
            String bindingName,
            long totalCount,
            Instant oldestRecord,
            Instant newestRecord,
            Map<String, Long> exceptionBreakdown
    ) {}

    // Used by MongoDB aggregation
    public static class ExceptionCount {
        private String exceptionClass;
        private long count;

        public String getExceptionClass() { return exceptionClass; }
        public void setExceptionClass(String exceptionClass) { this.exceptionClass = exceptionClass; }
        public long getCount() { return count; }
        public void setCount(long count) { this.count = count; }
    }
}
