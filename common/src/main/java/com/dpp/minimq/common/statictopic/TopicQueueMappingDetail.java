package com.dpp.minimq.common.statictopic;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * @author dpp
 * @date 2024/12/30
 * @Description topic队列信息
 */
public class TopicQueueMappingDetail {

    public static final int LEVEL_0 = 0;
    /**
     * 主题
     */
    private String topic;
    /**
     * broker名称
     */
    private String brokerName;
    /**
     * 队列数量
     */
    private int totalQueues;

    //the mapping info in current broker, do not register to nameserver
    /**
     * 一个队列id可能需要映射到多个逻辑队列，例如：一个全局ID可以映射到多个Broker上的逻辑队列，以实现负载均衡或容错
     * Broker可以根据 hostQueues中的映射信息动态调整消息的路由策略
     * hostQueues是broker的本地缓存，不会上报到nameServer
     */

    private ConcurrentMap<Integer, List<LogicQueueMappingItem>> hostedQueues = new ConcurrentHashMap<>();

    public static boolean putMappingInfo(TopicQueueMappingDetail mappingDetail, Integer globalId, List<LogicQueueMappingItem> mappingInfo) {
        if (mappingInfo.isEmpty()) {
            return true;
        }
        mappingDetail.hostedQueues.put(globalId, mappingInfo);
        return true;
    }

    public static List<LogicQueueMappingItem> getMappingInfo(TopicQueueMappingDetail mappingDetail, Integer globalId) {
        return mappingDetail.hostedQueues.get(globalId);
    }

    public static ConcurrentMap<Integer, Integer> buildIdMap(TopicQueueMappingDetail mappingDetail, int level) {
        //level 0 means current leader in this broker
        //level 1 means previous leader in this broker, reserved for
        assert level == LEVEL_0 ;

        if (mappingDetail.hostedQueues == null || mappingDetail.hostedQueues.isEmpty()) {
            return new ConcurrentHashMap<>();
        }
        ConcurrentMap<Integer, Integer> tmpIdMap = new ConcurrentHashMap<>();
        for (Map.Entry<Integer, List<LogicQueueMappingItem>> entry: mappingDetail.hostedQueues.entrySet()) {
            Integer globalId =  entry.getKey();
            List<LogicQueueMappingItem> items = entry.getValue();
            if (level == LEVEL_0
                    && items.size() >= 1) {
                LogicQueueMappingItem curr = items.get(items.size() - 1);
                if (mappingDetail.getBrokerName().equals(curr.getBrokerName())) {
                    tmpIdMap.put(globalId, curr.getQueueId());
                }
            }
        }
        return tmpIdMap;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getBrokerName() {
        return brokerName;
    }

    public void setBrokerName(String brokerName) {
        this.brokerName = brokerName;
    }

    public int getTotalQueues() {
        return totalQueues;
    }

    public void setTotalQueues(int totalQueues) {
        this.totalQueues = totalQueues;
    }

    public ConcurrentMap<Integer, List<LogicQueueMappingItem>> getHostedQueues() {
        return hostedQueues;
    }
}
