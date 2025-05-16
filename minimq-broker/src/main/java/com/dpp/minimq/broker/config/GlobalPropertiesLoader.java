package com.dpp.minimq.broker.config;

import com.dpp.minimq.broker.cache.CommonCache;
import com.dpp.minimq.broker.constants.BrokerConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author dpp
 * @date 2025/5/16
 * @Description
 */
public class GlobalPropertiesLoader {

    private static final Logger LOGGER = LoggerFactory.getLogger(GlobalPropertiesLoader.class);

    public void loadProperties() {
        // 1. 从jvm属性中获取mini_mq_home
        String miniMqHome = System.getenv(BrokerConstants.MINI_MQ_HOME);
        if(miniMqHome == null) {
            throw new IllegalArgumentException("mini_mq_home is null");
        }
        // 2. 加载配置
        GlobalProperties globalProperties = new GlobalProperties();
        globalProperties.setMiniMqHome(miniMqHome);
        CommonCache.setGlobalProperties(globalProperties);
        LOGGER.info("load globalProperties success! {}", globalProperties);
    }
}
