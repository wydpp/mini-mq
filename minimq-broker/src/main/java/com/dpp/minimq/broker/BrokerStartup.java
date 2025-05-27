package com.dpp.minimq.broker;

import com.dpp.minimq.broker.cache.CommonCache;
import com.dpp.minimq.broker.config.GlobalPropertiesLoader;
import com.dpp.minimq.broker.config.TopicInfoModelLoader;
import com.dpp.minimq.broker.constants.BrokerConstants;
import com.dpp.minimq.broker.core.CommitLogAppendHandler;
import com.dpp.minimq.broker.model.TopicInfoModel;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.Time;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * @author dpp
 * @date 2025/5/16
 * @Description
 */
public class BrokerStartup {

    private static GlobalPropertiesLoader globalPropertiesLoader;

    private static TopicInfoModelLoader topicInfoModelLoader;

    private static CommitLogAppendHandler commitLogAppendHandler;

    private static void initProperties() throws IOException {
        globalPropertiesLoader = new GlobalPropertiesLoader();
        globalPropertiesLoader.loadProperties();
        topicInfoModelLoader = new TopicInfoModelLoader();
        topicInfoModelLoader.loaderProperties();
        topicInfoModelLoader.startRefreshTopicInfoTask();
        commitLogAppendHandler = new CommitLogAppendHandler();
        for (TopicInfoModel topicInfoModel : CommonCache.getTopicInfoModelMap().values()) {
            commitLogAppendHandler.prepareMMapLoading(topicInfoModel.getTopic());
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        // 1. 加载配置
        initProperties();
        // 2. 模拟初始化映射
        String topic = "order_cancel_topic";
        String[] strings = new String[]{"Hello jerry", "Hello mark", "How old are you", "18 years old"};
        for (String string : strings) {
            commitLogAppendHandler.appendMessage(topic, string.getBytes(StandardCharsets.UTF_8));
            TimeUnit.SECONDS.sleep(BrokerConstants.DEFAULT_REFRESH_TOPIC_INFO_INTERVAL+1);
        }
        String message = commitLogAppendHandler.readMessage(topic, 0, 1024);
        System.out.println(message);

    }
}
