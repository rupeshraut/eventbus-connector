package com.enterprise.eventbus.config;

import com.enterprise.eventbus.model.DltStrategy;

public class DltProperties {

    private String topicSuffix = "DLT";
    private DltStrategy strategy = DltStrategy.STORE_ONLY;
    private DltReplayProperties replay = new DltReplayProperties();

    // -- Getters / Setters --

    public String getTopicSuffix() { return topicSuffix; }
    public void setTopicSuffix(String topicSuffix) { this.topicSuffix = topicSuffix; }

    public DltStrategy getStrategy() { return strategy; }
    public void setStrategy(DltStrategy strategy) { this.strategy = strategy; }

    public DltReplayProperties getReplay() { return replay; }
    public void setReplay(DltReplayProperties replay) { this.replay = replay; }

    public String resolveTopicName(String baseTopic) {
        return baseTopic + "." + topicSuffix;
    }

    public static class DltReplayProperties {
        private int maxReplays = 3;
        private long cooldownMs = 3_600_000L;
        private int batchSize = 100;

        public int getMaxReplays() { return maxReplays; }
        public void setMaxReplays(int maxReplays) { this.maxReplays = maxReplays; }

        public long getCooldownMs() { return cooldownMs; }
        public void setCooldownMs(long cooldownMs) { this.cooldownMs = cooldownMs; }

        public int getBatchSize() { return batchSize; }
        public void setBatchSize(int batchSize) { this.batchSize = batchSize; }
    }
}
