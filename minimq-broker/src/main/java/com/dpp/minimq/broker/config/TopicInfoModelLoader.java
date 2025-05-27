package com.dpp.minimq.broker.config;

import com.alibaba.fastjson.JSON;
import com.dpp.minimq.broker.cache.CommonCache;
import com.dpp.minimq.broker.constants.BrokerConstants;
import com.dpp.minimq.broker.model.TopicInfoModel;
import com.dpp.minimq.broker.utils.FileContentUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * @author dpp
 * @date 2025/5/16
 * @Description
 */
public class TopicInfoModelLoader {

    private static final Logger LOGGER = LoggerFactory.getLogger(TopicInfoModelLoader.class);

    private String filePath;

    public void loaderProperties() {
        GlobalProperties globalProperties = CommonCache.getGlobalProperties();
        String bashPath = globalProperties.getMiniMqHome();
        if (bashPath == null) {
            throw new IllegalArgumentException("miniMqHome is null");
        }
        filePath = bashPath + "/broker/config/minimq-topic.json";
        String string = FileContentUtil.readFromFile(filePath);
        List<TopicInfoModel> topicInfoModels = JSON.parseArray(string, TopicInfoModel.class);
        CommonCache.setTopicInfoModels(topicInfoModels);
        LOGGER.info("load topicInfoModel success! {}", topicInfoModels);
    }

    /**
     * 定时刷新topicInfoModel
     */
    public void startRefreshTopicInfoTask(){
        //每隔5s将内存中的配置刷新到磁盘文件中
        CommonThreadPoolConfig.refreshTopicInfoThreadPoolExecutor.execute(() -> {
            do {
                try {
                    TimeUnit.SECONDS.sleep(BrokerConstants.DEFAULT_REFRESH_TOPIC_INFO_INTERVAL);
                    List<TopicInfoModel> topicInfoModels = CommonCache.getTopicInfoModels();
                    if (topicInfoModels != null) {
                        FileContentUtil.overWriteToFile(filePath, JSON.toJSONString(topicInfoModels));
                        LOGGER.info("更新topicInfoModel成功!");
                    }
                } catch (Exception e) {
                    LOGGER.error("refreshTopicInfoModel error", e);
                    throw new RuntimeException(e);
                }
            }while (true);
        });
    }
}
