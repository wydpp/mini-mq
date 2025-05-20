package com.dpp.minimq.broker.cache;

import com.dpp.minimq.broker.config.GlobalProperties;
import com.dpp.minimq.broker.model.TopicInfoModel;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author dpp
 * @date 2025/5/16
 * @Description
 */
public class CommonCache {

    public static GlobalProperties globalProperties;

    private static List<TopicInfoModel> topicInfoModels;

    private static Map<String,TopicInfoModel> topicInfoModelMap = new HashMap<>();

    public static GlobalProperties getGlobalProperties() {
        return globalProperties;
    }

    public static void setGlobalProperties(GlobalProperties globalProperties) {
        CommonCache.globalProperties = globalProperties;
    }

    public static void setTopicInfoModels(List<TopicInfoModel> topicInfoModels) {
        CommonCache.topicInfoModels = topicInfoModels;
    }

    public static Map<String, TopicInfoModel> getTopicInfoModelMap() {
        return topicInfoModelMap;
    }

    public static void setTopicInfoModelMap(Map<String, TopicInfoModel> topicInfoModelMap) {
        CommonCache.topicInfoModelMap = topicInfoModelMap;
    }
}
