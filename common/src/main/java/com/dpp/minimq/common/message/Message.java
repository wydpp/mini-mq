package com.dpp.minimq.common.message;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
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

    private String propertiesString;

    private ByteBuffer encodeBuff;
    /**
     * 消息在队列中的偏移量
     * 每个消息队列都维护了一个 queueOffset 序列，用来唯一标识消息在该队列中的位置
     * 它是一个从 0 开始的长整型数字，随着消息的存储和消费不断递增。
     */
    private long queueOffset;
    /**
     * 是一个整数类型的属性，用于存储消息的系统标志。
     * 它是一个 32 位的整数，其中不同的位代表不同的消息属性或状态，
     * 通过位运算来设置和获取各种消息的特性，如是否为事务消息、是否压缩、是否有事务准备类型、是否已提交、是否已回滚等。
     */
    private int sysFlag;

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

    public String getPropertiesString() {
        return propertiesString;
    }

    public void setPropertiesString(String propertiesString) {
        this.propertiesString = propertiesString;
    }

    public ByteBuffer getEncodeBuff() {
        return encodeBuff;
    }

    public void setEncodeBuff(ByteBuffer encodeBuff) {
        this.encodeBuff = encodeBuff;
    }

    public long getQueueOffset() {
        return queueOffset;
    }

    public void setQueueOffset(long queueOffset) {
        this.queueOffset = queueOffset;
    }

    public int getSysFlag() {
        return sysFlag;
    }

    public void setSysFlag(int sysFlag) {
        this.sysFlag = sysFlag;
    }

    public String getProperty(final String name) {
        if (null == this.properties) {
            this.properties = new HashMap<>();
        }

        return this.properties.get(name);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Message{");
        sb.append("topic='").append(topic).append('\'');
        sb.append(", properties=").append(properties);
        sb.append(", body=").append(Arrays.toString(body));
        sb.append(", msgId=").append(msgId);
        sb.append(", brokerName='").append(brokerName).append('\'');
        sb.append(", queueId=").append(queueId);
        sb.append(", storeSize=").append(storeSize);
        sb.append(", bornTimestamp=").append(bornTimestamp);
        sb.append(", storeTimestamp=").append(storeTimestamp);
        sb.append(", propertiesString='").append(propertiesString).append('\'');
        sb.append(", encodeBuff=").append(encodeBuff);
        sb.append(", queueOffset=").append(queueOffset);
        sb.append('}');
        return sb.toString();
    }
}
