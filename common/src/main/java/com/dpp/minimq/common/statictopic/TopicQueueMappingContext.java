package com.dpp.minimq.common.statictopic;

import java.util.List;

/**
 * @author dpp
 * @date 2024/12/30
 * @Description
 */
public class TopicQueueMappingContext {

    private String topic;

    private Integer globalId;

    private TopicQueueMappingDetail mappingDetail;

    private List<LogicQueueMappingItem> mappingItemList;

    private LogicQueueMappingItem leaderItem;

    private LogicQueueMappingItem currentItem;

    public TopicQueueMappingContext(String topic, Integer globalId, TopicQueueMappingDetail mappingDetail, List<LogicQueueMappingItem> mappingItemList, LogicQueueMappingItem leaderItem) {
        this.topic = topic;
        this.globalId = globalId;
        this.mappingDetail = mappingDetail;
        this.mappingItemList = mappingItemList;
        this.leaderItem = leaderItem;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public Integer getGlobalId() {
        return globalId;
    }

    public void setGlobalId(Integer globalId) {
        this.globalId = globalId;
    }

    public TopicQueueMappingDetail getMappingDetail() {
        return mappingDetail;
    }

    public void setMappingDetail(TopicQueueMappingDetail mappingDetail) {
        this.mappingDetail = mappingDetail;
    }

    public List<LogicQueueMappingItem> getMappingItemList() {
        return mappingItemList;
    }

    public void setMappingItemList(List<LogicQueueMappingItem> mappingItemList) {
        this.mappingItemList = mappingItemList;
    }

    public LogicQueueMappingItem getLeaderItem() {
        return leaderItem;
    }

    public void setLeaderItem(LogicQueueMappingItem leaderItem) {
        this.leaderItem = leaderItem;
    }

    public LogicQueueMappingItem getCurrentItem() {
        return currentItem;
    }

    public void setCurrentItem(LogicQueueMappingItem currentItem) {
        this.currentItem = currentItem;
    }
}
