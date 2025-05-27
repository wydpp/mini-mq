package com.dpp.minimq.broker.config;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @author dpp
 * @date 2025/5/27
 * @Description 线程池配置
 */
public class CommonThreadPoolConfig {
    /**
     * 用于将topic信息刷新到磁盘中的线程池
     */
    public static ThreadPoolExecutor refreshTopicInfoThreadPoolExecutor =
            new ThreadPoolExecutor(1,
                    1,
                    30L,
                    TimeUnit.SECONDS,
                    new ArrayBlockingQueue<>(10),
                    r -> {
                        Thread thread = new Thread(r);
                        thread.setName("refresh-topic-info-thread");
                        return thread;
                    });
}
