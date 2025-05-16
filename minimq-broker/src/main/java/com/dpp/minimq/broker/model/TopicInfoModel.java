package com.dpp.minimq.broker.model;

import java.util.List;

/**
 * @author dpp
 * @date 2025/5/16
 * @Description topic信息
 */
public class TopicInfoModel {
    private String topic;
    private Long createAt;
    private Long updateAt;
    private List<TopicQueueModel> queueList;

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public Long getCreateAt() {
        return createAt;
    }

    public void setCreateAt(Long createAt) {
        this.createAt = createAt;
    }

    public Long getUpdateAt() {
        return updateAt;
    }

    public void setUpdateAt(Long updateAt) {
        this.updateAt = updateAt;
    }

    public List<TopicQueueModel> getQueueList() {
        return queueList;
    }

    public void setQueueList(List<TopicQueueModel> queueList) {
        this.queueList = queueList;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("MiniMqTopicModel{");
        sb.append("topic='").append(topic).append('\'');
        sb.append(", createAt=").append(createAt);
        sb.append(", updateAt=").append(updateAt);
        sb.append(", queueList=").append(queueList);
        sb.append('}');
        return sb.toString();
    }
}
