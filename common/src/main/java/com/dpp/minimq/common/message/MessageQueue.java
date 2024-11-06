package com.dpp.minimq.common.message;

import java.io.Serializable;

/**
 * @author dpp
 * @date 2024/11/6
 * @Description topic队列
 */
public class MessageQueue implements Serializable {
    private static final long serialVersionUID = 6191200464116433425L;
    /**
     * 主题名称
     */
    private String topic;

    private String brokerName;
    /**
     * 队列id
     */
    private int queueId;

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

    public int getQueueId() {
        return queueId;
    }

    public void setQueueId(int queueId) {
        this.queueId = queueId;
    }
}
