package com.dpp.minimq.broker.config;

import com.alibaba.fastjson.JSON;
import com.dpp.minimq.broker.cache.CommonCache;
import com.dpp.minimq.broker.model.TopicInfoModel;
import com.dpp.minimq.broker.utils.FileContentReaderUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * @author dpp
 * @date 2025/5/16
 * @Description
 */
public class TopicInfoModelLoader {

    private static final Logger LOGGER = LoggerFactory.getLogger(TopicInfoModelLoader.class);

    public void loaderProperties() {
        GlobalProperties globalProperties = CommonCache.getGlobalProperties();
        String bashPath = globalProperties.getMiniMqHome();
        if (bashPath == null) {
            throw new IllegalArgumentException("miniMqHome is null");
        }
        String topicInfoPath = bashPath + "/broker/config/minimq-topic.json";
        String string = FileContentReaderUtils.readFromFile(topicInfoPath);
        List<TopicInfoModel> topicInfoModels = JSON.parseArray(string, TopicInfoModel.class);
        CommonCache.setTopicInfoModels(topicInfoModels);
        LOGGER.info("load topicInfoModel success! {}", topicInfoModels);
    }
}
