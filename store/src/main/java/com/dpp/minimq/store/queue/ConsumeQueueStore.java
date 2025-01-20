/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.dpp.minimq.store.queue;

import com.dpp.minimq.common.TopicConfig;
import com.dpp.minimq.common.attribute.CQType;
import com.dpp.minimq.common.message.Message;
import com.dpp.minimq.common.utils.QueueTypeUtils;
import com.dpp.minimq.store.ConsumeQueue;
import com.dpp.minimq.store.DefaultMessageStore;
import com.dpp.minimq.store.config.MessageStoreConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.dpp.minimq.store.config.StorePathConfigHelper.getStorePathConsumeQueue;

/**
 * 是 RocketMQ 中用于存储和管理消费队列（Consume Queue）的组件。
 * 消费队列是一种逻辑上的数据结构，它存储了消息在存储文件（如 CommitLog）中的位置信息，主要包括消息的物理偏移量（在 CommitLog 中的位置）、消息大小和消息的 Tag 的哈希值。
 * 消费者根据消费队列中的信息可以方便地找到消息在 CommitLog 中的位置，进而拉取并消费消息。
 */
public class ConsumeQueueStore {
    private static final Logger log = LoggerFactory.getLogger(ConsumeQueueStore.class);

    protected final DefaultMessageStore messageStore;
    protected final MessageStoreConfig messageStoreConfig;
    protected final QueueOffsetAssigner queueOffsetAssigner = new QueueOffsetAssigner();
    protected final ConcurrentMap<String/* topic */, ConcurrentMap<Integer/* queueId */, ConsumeQueueInterface>> consumeQueueTable;

    // Should be careful, do not change the topic config
    // TopicConfigManager is more suitable here.
    private ConcurrentMap<String, TopicConfig> topicConfigTable;

    public ConsumeQueueStore(DefaultMessageStore messageStore, MessageStoreConfig messageStoreConfig) {
        this.messageStore = messageStore;
        this.messageStoreConfig = messageStoreConfig;
        this.consumeQueueTable = new ConcurrentHashMap<>(32);
    }

    public void setTopicConfigTable(ConcurrentMap<String, TopicConfig> topicConfigTable) {
        this.topicConfigTable = topicConfigTable;
    }

    private FileQueueLifeCycle getLifeCycle(String topic, int queueId) {
        return (FileQueueLifeCycle) findOrCreateConsumeQueue(topic, queueId);
    }

    public long rollNextFile(ConsumeQueueInterface consumeQueue, final long offset) {
        FileQueueLifeCycle fileQueueLifeCycle = getLifeCycle(consumeQueue.getTopic(), consumeQueue.getQueueId());
        return fileQueueLifeCycle.rollNextFile(offset);
    }

    public void correctMinOffset(ConsumeQueueInterface consumeQueue, long minCommitLogOffset) {
        consumeQueue.correctMinOffset(minCommitLogOffset);
    }

    private ConsumeQueueInterface createConsumeQueueByType(CQType cqType, String topic, int queueId, String storePath) {
        if (Objects.equals(CQType.SimpleCQ, cqType)) {
            return new ConsumeQueue(
                    topic,
                    queueId,
                    storePath,
                    this.messageStoreConfig.getMappedFileSizeConsumeQueue(),
                    this.messageStore);
        } else if (Objects.equals(CQType.BatchCQ, cqType)) {
            //TODO 暂不考虑批量消息
            return null;
        } else {
            throw new RuntimeException(String.format("queue type %s is not supported.", cqType.toString()));
        }
    }

    private void queueTypeShouldBe(String topic, CQType cqTypeExpected) {
        TopicConfig topicConfig = this.topicConfigTable == null ? null : this.topicConfigTable.get(topic);

        CQType cqTypeActual = QueueTypeUtils.getCQType(Optional.ofNullable(topicConfig));

        if (!Objects.equals(cqTypeExpected, cqTypeActual)) {
            throw new RuntimeException(String.format("The queue type of topic: %s should be %s, but is %s", topic, cqTypeExpected, cqTypeActual));
        }
    }

    public long getMaxOffsetInConsumeQueue() {
        long maxPhysicOffset = -1L;
        for (ConcurrentMap<Integer, ConsumeQueueInterface> maps : this.consumeQueueTable.values()) {
            for (ConsumeQueueInterface logic : maps.values()) {
                if (logic.getMaxPhysicOffset() > maxPhysicOffset) {
                    maxPhysicOffset = logic.getMaxPhysicOffset();
                }
            }
        }
        return maxPhysicOffset;
    }

    public void checkSelf(ConsumeQueueInterface consumeQueue) {
        FileQueueLifeCycle fileQueueLifeCycle = getLifeCycle(consumeQueue.getTopic(), consumeQueue.getQueueId());
        fileQueueLifeCycle.checkSelf();
    }

    public void checkSelf() {
        for (Map.Entry<String, ConcurrentMap<Integer, ConsumeQueueInterface>> topicEntry : this.consumeQueueTable.entrySet()) {
            for (Map.Entry<Integer, ConsumeQueueInterface> cqEntry : topicEntry.getValue().entrySet()) {
                this.checkSelf(cqEntry.getValue());
            }
        }
    }

    public boolean flush(ConsumeQueueInterface consumeQueue, int flushLeastPages) {
        FileQueueLifeCycle fileQueueLifeCycle = getLifeCycle(consumeQueue.getTopic(), consumeQueue.getQueueId());
        return fileQueueLifeCycle.flush(flushLeastPages);
    }

    public void destroy(ConsumeQueueInterface consumeQueue) {
        FileQueueLifeCycle fileQueueLifeCycle = getLifeCycle(consumeQueue.getTopic(), consumeQueue.getQueueId());
        fileQueueLifeCycle.destroy();
    }

    public int deleteExpiredFile(ConsumeQueueInterface consumeQueue, long minCommitLogPos) {
        FileQueueLifeCycle fileQueueLifeCycle = getLifeCycle(consumeQueue.getTopic(), consumeQueue.getQueueId());
        return fileQueueLifeCycle.deleteExpiredFile(minCommitLogPos);
    }

    public void truncateDirtyLogicFiles(ConsumeQueueInterface consumeQueue, long phyOffset) {
        FileQueueLifeCycle fileQueueLifeCycle = getLifeCycle(consumeQueue.getTopic(), consumeQueue.getQueueId());
        fileQueueLifeCycle.truncateDirtyLogicFiles(phyOffset);
    }

    public void swapMap(ConsumeQueueInterface consumeQueue, int reserveNum, long forceSwapIntervalMs,
                        long normalSwapIntervalMs) {
        FileQueueLifeCycle fileQueueLifeCycle = getLifeCycle(consumeQueue.getTopic(), consumeQueue.getQueueId());
        fileQueueLifeCycle.swapMap(reserveNum, forceSwapIntervalMs, normalSwapIntervalMs);
    }

    public void cleanSwappedMap(ConsumeQueueInterface consumeQueue, long forceCleanSwapIntervalMs) {
        FileQueueLifeCycle fileQueueLifeCycle = getLifeCycle(consumeQueue.getTopic(), consumeQueue.getQueueId());
        fileQueueLifeCycle.cleanSwappedMap(forceCleanSwapIntervalMs);
    }

    public boolean isFirstFileAvailable(ConsumeQueueInterface consumeQueue) {
        FileQueueLifeCycle fileQueueLifeCycle = getLifeCycle(consumeQueue.getTopic(), consumeQueue.getQueueId());
        return fileQueueLifeCycle.isFirstFileAvailable();
    }

    public boolean isFirstFileExist(ConsumeQueueInterface consumeQueue) {
        FileQueueLifeCycle fileQueueLifeCycle = getLifeCycle(consumeQueue.getTopic(), consumeQueue.getQueueId());
        return fileQueueLifeCycle.isFirstFileExist();
    }

    public ConsumeQueueInterface findOrCreateConsumeQueue(String topic, int queueId) {
        return doFindOrCreateConsumeQueue(topic, queueId);
    }

    private ConsumeQueueInterface doFindOrCreateConsumeQueue(String topic, int queueId) {
        ConcurrentMap<Integer, ConsumeQueueInterface> map = consumeQueueTable.get(topic);
        if (null == map) {
            ConcurrentMap<Integer, ConsumeQueueInterface> newMap = new ConcurrentHashMap<>(128);
            ConcurrentMap<Integer, ConsumeQueueInterface> oldMap = consumeQueueTable.putIfAbsent(topic, newMap);
            if (oldMap != null) {
                map = oldMap;
            } else {
                map = newMap;
            }
        }

        ConsumeQueueInterface logic = map.get(queueId);
        if (logic != null) {
            return logic;
        }

        ConsumeQueueInterface newLogic;

        Optional<TopicConfig> topicConfig = this.getTopicConfig(topic);
        // TODO maybe the topic has been deleted.
        if (Objects.equals(CQType.BatchCQ, QueueTypeUtils.getCQType(topicConfig))) {
            log.info("暂不支持批量消息队列");
            //TODO 暂不考虑批量消费队列的场景
            newLogic = null;
        } else {
            newLogic = new ConsumeQueue(
                    topic,
                    queueId,
                    getStorePathConsumeQueue(this.messageStoreConfig.getStorePathRootDir()),
                    this.messageStoreConfig.getMappedFileSizeConsumeQueue(),
                    this.messageStore);
        }

        ConsumeQueueInterface oldLogic = map.putIfAbsent(queueId, newLogic);
        if (oldLogic != null) {
            logic = oldLogic;
        } else {
            logic = newLogic;
        }

        return logic;
    }

    public Long getMaxOffset(String topic, int queueId) {
        return this.queueOffsetAssigner.currentQueueOffset(topic + "-" + queueId);
    }

    public void setTopicQueueTable(ConcurrentMap<String, Long> topicQueueTable) {
        this.queueOffsetAssigner.setTopicQueueTable(topicQueueTable);
    }

    public ConcurrentMap getTopicQueueTable() {
        return this.queueOffsetAssigner.getTopicQueueTable();
    }

    public void setBatchTopicQueueTable(ConcurrentMap<String, Long> batchTopicQueueTable) {
        this.queueOffsetAssigner.setBatchTopicQueueTable(batchTopicQueueTable);
    }

    public void assignQueueOffset(Message msg, short messageNum) {
        ConsumeQueueInterface consumeQueue = findOrCreateConsumeQueue(msg.getTopic(), msg.getQueueId());
        consumeQueue.assignQueueOffset(this.queueOffsetAssigner, msg, messageNum);
    }

    public void updateQueueOffset(String topic, int queueId, long offset) {
        String topicQueueKey = topic + "-" + queueId;
        this.queueOffsetAssigner.updateQueueOffset(topicQueueKey, offset);
    }

    public void removeTopicQueueTable(String topic, Integer queueId) {
        this.queueOffsetAssigner.remove(topic, queueId);
    }

    public ConcurrentMap<String, ConcurrentMap<Integer, ConsumeQueueInterface>> getConsumeQueueTable() {
        return consumeQueueTable;
    }

    private void putConsumeQueue(final String topic, final int queueId, final ConsumeQueueInterface consumeQueue) {
        ConcurrentMap<Integer/* queueId */, ConsumeQueueInterface> map = this.consumeQueueTable.get(topic);
        if (null == map) {
            map = new ConcurrentHashMap<>();
            map.put(queueId, consumeQueue);
            this.consumeQueueTable.put(topic, map);
        } else {
            map.put(queueId, consumeQueue);
        }
    }

    public Optional<TopicConfig> getTopicConfig(String topic) {
        if (this.topicConfigTable == null) {
            return Optional.empty();
        }

        return Optional.ofNullable(this.topicConfigTable.get(topic));
    }

    public long getTotalSize() {
        long totalSize = 0;
        for (ConcurrentMap<Integer, ConsumeQueueInterface> maps : this.consumeQueueTable.values()) {
            for (ConsumeQueueInterface logic : maps.values()) {
                totalSize += logic.getTotalSize();
            }
        }
        return totalSize;
    }
}
