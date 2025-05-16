package com.dpp.minimq.broker.cache;

import com.dpp.minimq.broker.config.GlobalProperties;
import com.dpp.minimq.broker.model.TopicInfoModel;

import java.util.List;

/**
 * @author dpp
 * @date 2025/5/16
 * @Description
 */
public class CommonCache {

    public static GlobalProperties globalProperties;

    private static List<TopicInfoModel> topicInfoModels;

    public static GlobalProperties getGlobalProperties() {
        return globalProperties;
    }

    public static void setGlobalProperties(GlobalProperties globalProperties) {
        CommonCache.globalProperties = globalProperties;
    }

    public static List<TopicInfoModel> getTopicInfoModels() {
        return topicInfoModels;
    }

    public static void setTopicInfoModels(List<TopicInfoModel> topicInfoModels) {
        CommonCache.topicInfoModels = topicInfoModels;
    }
}
