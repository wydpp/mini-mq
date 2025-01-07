package com.dpp.minimq.broker.topic;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.dpp.minimq.broker.BrokerController;
import com.dpp.minimq.broker.BrokerPathConfigHelper;
import com.dpp.minimq.common.MixAll;
import com.dpp.minimq.common.TopicConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author dpp
 * @date 2024/12/31
 * @Description
 */
public class TopicConfigManager {

    private static final Logger log = LoggerFactory.getLogger(TopicConfigManager.class);

    private static final long LOCK_TIMEOUT_MILLIS = 3000;
    private static final int SCHEDULE_TOPIC_QUEUE_NUM = 18;

    /**
     * 主题配置表
     */
    private final ConcurrentMap<String, TopicConfig> topicConfigTable =
            new ConcurrentHashMap<>(1024);

    private transient BrokerController brokerController;

    private transient final Lock topicConfigTableLock = new ReentrantLock();

    public TopicConfigManager(BrokerController brokerController) {
        this.brokerController = brokerController;
    }

    /**
     * 获取主题配置
     *
     * @param topic
     * @return
     */
    public TopicConfig selectTopicConfig(final String topic) {
        return this.topicConfigTable.get(topic);
    }

    /**
     * 获取或者创建topicConfig
     * @param topicConfig
     * @return
     */
    public TopicConfig createTopicIfAbsent(TopicConfig topicConfig) {
        if (topicConfig == null) {
            log.info("topicConfig is null");
            throw new IllegalArgumentException("TopicName");
        }
        try {
            if (this.topicConfigTableLock.tryLock(LOCK_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)) {
                try {
                    TopicConfig existedTopicConfig = this.topicConfigTable.get(topicConfig.getTopicName());
                    if (existedTopicConfig != null) {
                        return existedTopicConfig;
                    }
                    log.info("Create new topic [{}] config:[{}]", topicConfig.getTopicName(), topicConfig);
                    this.topicConfigTable.put(topicConfig.getTopicName(), topicConfig);
                }finally {
                    this.topicConfigTableLock.unlock();
                }
            }
        } catch (Exception e) {
            log.error("createTopicIfAbsent", e);
        }
        return this.topicConfigTable.get(topicConfig.getTopicName());
    }

    /**
     * 持久化主题信息到文件中
     */
    public synchronized void persist() {
        String jsonString = this.encode();
        if (jsonString != null) {
            String fileName = this.configFilePath();
            try {
                MixAll.string2File(jsonString, fileName);
            } catch (IOException e) {
                log.error("persist file " + fileName + " exception", e);
            }
        }
    }

    public String encode() {
        //简单的转成json字符串进行存储
        return JSONObject.toJSONString(this.topicConfigTable,true);
    }

    /**
     * 存储文件路径
     * @return
     */
    public String configFilePath() {
        return BrokerPathConfigHelper.getTopicConfigPath(this.brokerController.getMessageStoreConfig().getStorePathRootDir());
    }

}
