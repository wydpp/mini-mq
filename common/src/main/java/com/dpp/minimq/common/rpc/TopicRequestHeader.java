package com.dpp.minimq.common.rpc;

/**
 * @author dpp
 * @date 2024/12/30
 * @Description
 */
public abstract class TopicRequestHeader {
    /**
     * 获取主题
     * @return
     */
    public abstract String getTopic();

    /**
     * 设置主题
     * @param topic
     */
    public abstract void setTopic(String topic);
}
