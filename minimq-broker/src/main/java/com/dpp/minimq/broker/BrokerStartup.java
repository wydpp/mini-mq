package com.dpp.minimq.broker;

import com.dpp.minimq.broker.cache.CommonCache;
import com.dpp.minimq.broker.config.GlobalPropertiesLoader;
import com.dpp.minimq.broker.config.TopicInfoModelLoader;
import com.dpp.minimq.broker.constants.BrokerConstants;
import com.dpp.minimq.broker.core.MessageAppendHandler;
import com.dpp.minimq.broker.model.TopicInfoModel;

import java.io.IOException;
import java.util.List;

/**
 * @author dpp
 * @date 2025/5/16
 * @Description
 */
public class BrokerStartup {

    private static GlobalPropertiesLoader globalPropertiesLoader;

    private static TopicInfoModelLoader topicInfoModelLoader;

    private static MessageAppendHandler messageAppendHandler;

    private static void initProperties() throws IOException {
        globalPropertiesLoader = new GlobalPropertiesLoader();
        globalPropertiesLoader.loadProperties();
        topicInfoModelLoader = new TopicInfoModelLoader();
        topicInfoModelLoader.loaderProperties();
        messageAppendHandler = new MessageAppendHandler();
        List<TopicInfoModel> topicInfoModels = CommonCache.getTopicInfoModels();
        for (TopicInfoModel topicInfoModel : topicInfoModels) {
            String filePath = CommonCache.getGlobalProperties().getMiniMqHome()
                    + BrokerConstants.BASE_STORE_PATH
                    + "/" + topicInfoModel.getTopic()
                    + "/00000001";
            messageAppendHandler.prepareMMapLoading(filePath, topicInfoModel.getTopic());
        }
    }

    public static void main(String[] args) throws IOException {
        // 1. 加载配置
        initProperties();
        // 2. 模拟初始化映射
        String topic = "order_cancel_topic";
        messageAppendHandler.appendMessage(topic, "this is order_cancel_topic");
        String message = messageAppendHandler.readMessage(topic, 0, 10);
        System.out.println(message);

    }
}
