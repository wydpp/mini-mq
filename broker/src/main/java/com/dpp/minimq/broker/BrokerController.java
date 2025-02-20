package com.dpp.minimq.broker;

import com.dpp.minimq.broker.latency.BrokerFixedThreadPoolExecutor;
import com.dpp.minimq.broker.processor.SendMessageProcessor;
import com.dpp.minimq.broker.topic.TopicConfigManager;
import com.dpp.minimq.broker.topic.TopicQueueMappingManager;
import com.dpp.minimq.common.BrokerConfig;
import com.dpp.minimq.common.protocol.RequestCode;
import com.dpp.minimq.remoting.RemotingServer;
import com.dpp.minimq.remoting.netty.NettyRemotingServer;
import com.dpp.minimq.remoting.netty.NettyServerConfig;
import com.dpp.minimq.store.MessageStore;
import com.dpp.minimq.store.config.MessageStoreConfig;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author dpp
 * @date 2024/11/6
 * @Description
 */
public class BrokerController {
    /**
     * broker配置
     */
    private final BrokerConfig brokerConfig;
    /**
     * nettyServerClient配置
     */
    private final NettyServerConfig nettyServerConfig;

    /**
     * 数据存储配置
     */
    private final MessageStoreConfig messageStoreConfig;
    /**
     * broker netty 服务端，用来接收和处理客户端发送的消息
     */
    private RemotingServer remotingServer;
    /**
     * 主题队列映射管理器
     */
    private TopicQueueMappingManager topicQueueMappingManager;

    private TopicConfigManager topicConfigManager;

    private MessageStore messageStore;

    protected ExecutorService sendMessageExecutor;

    public BrokerController(BrokerConfig brokerConfig, NettyServerConfig nettyServerConfig, MessageStoreConfig messageStoreConfig) {
        this.brokerConfig = brokerConfig;
        this.nettyServerConfig = nettyServerConfig;
        this.messageStoreConfig = messageStoreConfig;
        this.topicConfigManager = new TopicConfigManager(this);
    }

    public boolean initialize() throws CloneNotSupportedException {
        initializeRemotingServer();
        initializeResources();
        //注册处理器
        registerProcessor();
        return true;
    }

    protected void initializeRemotingServer() throws CloneNotSupportedException {
        this.remotingServer = new NettyRemotingServer(this.nettyServerConfig);
    }

    protected void initializeResources() {
        this.sendMessageExecutor = new BrokerFixedThreadPoolExecutor(
                this.brokerConfig.getSendMessageThreadPoolNums(),
                this.brokerConfig.getSendMessageThreadPoolNums(),
                1000 * 60,
                TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(this.brokerConfig.getSendThreadPoolQueueCapacity()),
                new ThreadFactory() {
                    AtomicInteger num = new AtomicInteger(0);

                    @Override
                    public Thread newThread(Runnable r) {
                        Thread thread = new Thread();
                        thread.setName("SendMessageThread_" + num.getAndIncrement());
                        return thread;
                    }
                });
    }

    private void registerProcessor() {
        remotingServer.registerProcessor(RequestCode.SEND_MESSAGE, new SendMessageProcessor(this), sendMessageExecutor);
    }

    public void start() throws Exception {
        if (remotingServer != null) {
            //启动netty服务端
            remotingServer.start();
        }
    }

    public void shutdown() {
        if (remotingServer != null) {
            this.remotingServer.shutDown();
        }
    }

    public BrokerConfig getBrokerConfig() {
        return brokerConfig;
    }

    public NettyServerConfig getNettyServerConfig() {
        return nettyServerConfig;
    }

    public String getBrokerAddr() {
        return this.brokerConfig.getBrokerIP1() + ":" + this.nettyServerConfig.getListenPort();
    }

    public TopicQueueMappingManager getTopicQueueMappingManager() {
        return topicQueueMappingManager;
    }

    public MessageStoreConfig getMessageStoreConfig() {
        return messageStoreConfig;
    }

    public TopicConfigManager getTopicConfigManager() {
        return topicConfigManager;
    }

    public MessageStore getMessageStore() {
        return messageStore;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("BrokerController{");
        sb.append("brokerConfig=").append(brokerConfig);
        sb.append(", nettyServerConfig=").append(nettyServerConfig);
        sb.append(", messageStoreConfig=").append(messageStoreConfig);
        sb.append(", remotingServer=").append(remotingServer);
        sb.append(", topicQueueMappingManager=").append(topicQueueMappingManager);
        sb.append(", topicConfigManager=").append(topicConfigManager);
        sb.append(", messageStore=").append(messageStore);
        sb.append('}');
        return sb.toString();
    }
}
