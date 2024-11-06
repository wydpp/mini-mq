package com.dpp.minimq.common.message;

import java.io.Serializable;
import java.util.Map;

/**
 * @author dpp
 * @date 2024/11/6
 * @Description 消息对象
 */
public class Message implements Serializable {
    private static final long serialVersionUID = 5720810158625748049L;
    //主题
    private String topic;

    private Map<String,String> properties;
    /**
     * 消息内容的字节数组
     */
    private byte[] body;
    //消息唯一id
    private long msgId;

    /**
     * 托管消息的Broker名称
     */
    private String brokerName;
    /**
     * 消息所在的队列id
     */
    private int queueId;
    /**
     * 消息在存储时的字节大小
     */
    private int storeSize;
    /**
     * 消息生成的时间戳
     */
    private long bornTimestamp;
    /**
     * 消息存储的时间戳
     */
    private long storeTimestamp;

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public void setProperties(Map<String, String> properties) {
        this.properties = properties;
    }

    public byte[] getBody() {
        return body;
    }

    public void setBody(byte[] body) {
        this.body = body;
    }

    public long getMsgId() {
        return msgId;
    }

    public void setMsgId(long msgId) {
        this.msgId = msgId;
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

    public int getStoreSize() {
        return storeSize;
    }

    public void setStoreSize(int storeSize) {
        this.storeSize = storeSize;
    }

    public long getBornTimestamp() {
        return bornTimestamp;
    }

    public void setBornTimestamp(long bornTimestamp) {
        this.bornTimestamp = bornTimestamp;
    }

    public long getStoreTimestamp() {
        return storeTimestamp;
    }

    public void setStoreTimestamp(long storeTimestamp) {
        this.storeTimestamp = storeTimestamp;
    }
}
