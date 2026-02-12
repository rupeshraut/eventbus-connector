package com.enterprise.eventbus.offset;

import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface IdempotencyRepository extends MongoRepository<IdempotencyRecord, String> {

    boolean existsByTopicAndPartitionAndOffset(String topic, int partition, long offset);
}
