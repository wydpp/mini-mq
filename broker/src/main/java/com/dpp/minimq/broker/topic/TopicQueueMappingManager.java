package com.dpp.minimq.broker.topic;

import com.dpp.minimq.broker.BrokerController;
import com.dpp.minimq.common.rpc.TopicQueueRequestHeader;
import com.dpp.minimq.common.rpc.TopicRequestHeader;
import com.dpp.minimq.common.statictopic.LogicQueueMappingItem;
import com.dpp.minimq.common.statictopic.TopicQueueMappingContext;
import com.dpp.minimq.common.statictopic.TopicQueueMappingDetail;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author dpp
 * @date 2024/12/30
 * @Description topic队列管理器
 * 在 RocketMQ 中，Topic 是消息的逻辑分类，而 Queue 是消息的物理存储单元。
 * 一个 Topic 可以包含多个 Queue，这些 Queue 分布在不同的 Broker 上。
 * 为了高效地管理这些映射关系，RocketMQ 引入了 TopicQueueMappingManager。
 */
public class TopicQueueMappingManager {

    private static final Logger log = LoggerFactory.getLogger(TopicQueueMappingManager.class);

    private BrokerController brokerController;
    // topic队列映射表
    private final ConcurrentHashMap<String, TopicQueueMappingDetail> topicQueueMappingTable = new ConcurrentHashMap<>();

    public TopicQueueMappingManager(BrokerController brokerController) {
        this.brokerController = brokerController;
    }

    public TopicQueueMappingDetail getTopicQueueMappingDetail(String topic) {
        return topicQueueMappingTable.get(topic);
    }

    public TopicQueueMappingContext buildTopicQueueMappingContext(TopicRequestHeader requestHeader) {
        return buildTopicQueueMappingContext(requestHeader, false);
    }

    public TopicQueueMappingContext buildTopicQueueMappingContext(TopicRequestHeader requestHeader, boolean selectOneWhenMiss) {
        String topic = requestHeader.getTopic();
        Integer globalId = null;
        if (requestHeader instanceof TopicQueueRequestHeader) {
            globalId = ((TopicQueueRequestHeader) requestHeader).getQueueId();
        }
        TopicQueueMappingDetail mappingDetail = getTopicQueueMappingDetail(topic);
        if (mappingDetail == null) {
            //当前没找到对应的主题
            log.info("主题未找到，topic={}", topic);
            return new TopicQueueMappingContext(topic, null, null, null, null);
        }
        if (globalId == null) {
            log.info("队列id是null,topic={}", topic);
            return new TopicQueueMappingContext(topic, null, mappingDetail, null, null);
        }
        if (globalId < 0 && !selectOneWhenMiss) {
            return new TopicQueueMappingContext(topic, globalId, mappingDetail, null, null);
        }
        if (globalId < 0) {
            try {
                if (!mappingDetail.getHostedQueues().isEmpty()) {
                    //获取一个队列id
                    globalId = mappingDetail.getHostedQueues().keySet().iterator().next();
                }
            } catch (Throwable ignored) {
            }
        }
        if (globalId < 0) {
            return new TopicQueueMappingContext(topic, globalId, mappingDetail, null, null);
        }

        List<LogicQueueMappingItem> mappingItemList = TopicQueueMappingDetail.getMappingInfo(mappingDetail, globalId);
        LogicQueueMappingItem leaderItem = null;
        if (mappingItemList != null && !mappingItemList.isEmpty()) {
            //队列中最后一个作为leaderItem
            leaderItem = mappingItemList.get(mappingItemList.size() - 1);
        }
        return new TopicQueueMappingContext(topic, globalId, mappingDetail, mappingItemList, leaderItem);
    }
}
