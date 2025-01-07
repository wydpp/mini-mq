package com.dpp.minimq.common.statictopic;

/**
 * @author dpp
 * @date 2024/12/30
 * @Description 主要用于逻辑队列与物理队列的映射关系管理。
 * RocketMQ 使用逻辑队列来抽象化消息队列的管理，而物理队列则是底层的实际存储结构。
 * 为了理解 LogicQueueMappingItem 的作用，我们需要从 RocketMQ 的消息队列管理机制入手。
 *
 * 逻辑队列：在 RocketMQ 中，逻辑队列是一个抽象的概念，用来表示一个消息队列的逻辑结构。一个逻辑队列可以对应多个物理队列，它的数量和配置可以在 RocketMQ 的运行时动态调整。
 *
 * 物理队列：物理队列是真正的数据存储结构，实际存放消息数据。每个物理队列对应磁盘上的一块区域，负责存储某些特定消息。
 */
public class LogicQueueMappingItem {
    /**
     * 版本号，表示队列映射的生成次数或更新次数
     */
    private int gen;
    /**
     * 队列id
     */
    private int queueId;
    /**
     * broker名称
     */
    private String brokerName;
    /**
     * the start of the logic offset 起始的逻辑队列偏移量
     */
    private long logicOffset;
    /**
     * the start of the physical offset 起始的物理队列偏移量
     */
    private long startOffset;
    /**
     * the end of the physical offset
     */
    private long endOffset;

    public int getGen() {
        return gen;
    }

    public void setGen(int gen) {
        this.gen = gen;
    }

    public int getQueueId() {
        return queueId;
    }

    public void setQueueId(int queueId) {
        this.queueId = queueId;
    }

    public String getBrokerName() {
        return brokerName;
    }

    public void setBrokerName(String brokerName) {
        this.brokerName = brokerName;
    }

    public long getLogicOffset() {
        return logicOffset;
    }

    public void setLogicOffset(long logicOffset) {
        this.logicOffset = logicOffset;
    }

    public long getStartOffset() {
        return startOffset;
    }

    public void setStartOffset(long startOffset) {
        this.startOffset = startOffset;
    }

    public long getEndOffset() {
        return endOffset;
    }

    public void setEndOffset(long endOffset) {
        this.endOffset = endOffset;
    }
}
