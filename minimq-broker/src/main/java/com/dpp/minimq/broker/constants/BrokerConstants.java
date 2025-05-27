package com.dpp.minimq.broker.constants;

/**
 * @author dpp
 * @date 2025/5/16
 * @Description
 */
public class BrokerConstants {
    /**
     * 消息存储路径-环境变量
     */
    public static final String MINI_MQ_HOME = "mini_mq_home";

    public static final String BASE_STORE_PATH = "/broker/store";

    public static final Long COMMITLONG_DEFAULT_MMAP_SIZE = 1 * 1024L; //1kb

    public static final Integer DEFAULT_REFRESH_TOPIC_INFO_INTERVAL = 5; //5s
}
