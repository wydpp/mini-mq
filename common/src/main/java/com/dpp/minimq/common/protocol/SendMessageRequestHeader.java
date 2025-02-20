package com.dpp.minimq.common.protocol;

import com.dpp.minimq.common.rpc.TopicQueueRequestHeader;
import com.dpp.minimq.remoting.protocol.RemotingCommand;

/**
 * @author dpp
 * @date 2024/12/30
 * @Description
 */
public class SendMessageRequestHeader extends TopicQueueRequestHeader {
    /**
     * 生产者组
     */
    private String producerGroup;
    /**
     * 主题
     */
    private String topic;
    /**
     * 队列 id
     */
    private Integer queueId;

    public String getProducerGroup() {
        return producerGroup;
    }

    public void setProducerGroup(String producerGroup) {
        this.producerGroup = producerGroup;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public Integer getQueueId() {
        return queueId;
    }

    public void setQueueId(Integer queueId) {
        this.queueId = queueId;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("SendMessageRequestHeader{");
        sb.append("producerGroup='").append(producerGroup).append('\'');
        sb.append(", topic='").append(topic).append('\'');
        sb.append(", queueId=").append(queueId);
        sb.append('}');
        return sb.toString();
    }

    public static SendMessageRequestHeader parseRequestHeader(RemotingCommand request) {
        switch (request.getCode()) {
            case RequestCode.SEND_MESSAGE:
                SendMessageRequestHeader sendMessageRequestHeader = new SendMessageRequestHeader();
                sendMessageRequestHeader.setProducerGroup(request.getExtField("producerGroup"));
                sendMessageRequestHeader.setTopic(request.getExtField("topic"));
                sendMessageRequestHeader.setQueueId(Integer.parseInt(request.getExtField("queueId")));
                return sendMessageRequestHeader;
            default:
                return null;
        }
    }
}
