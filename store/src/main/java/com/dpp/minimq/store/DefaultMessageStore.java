package com.dpp.minimq.store;

import com.dpp.minimq.common.BrokerConfig;
import com.dpp.minimq.common.message.Message;
import com.dpp.minimq.store.config.MessageStoreConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 默认的 消息存储实现
 * @author dpp
 * @date 2025/1/7
 * @Description
 */
public class DefaultMessageStore implements MessageStore{

    private static final Logger log = LoggerFactory.getLogger(DefaultMessageStore.class);

    private MessageStoreConfig messageStoreConfig;

    private final BrokerConfig brokerConfig;

    private final TransientStorePool transientStorePool;

    public DefaultMessageStore(BrokerConfig brokerConfig, MessageStoreConfig messageStoreConfig) {
        this.brokerConfig = brokerConfig;
        this.messageStoreConfig = messageStoreConfig;
        this.transientStorePool = new TransientStorePool(messageStoreConfig);
    }

    @Override
    public boolean load() {
        return false;
    }

    @Override
    public void start() throws Exception {

    }

    @Override
    public void shutdown() throws Exception {

    }

    @Override
    public void destroy() {

    }

    @Override
    public PutMessageResult putMessage(Message msg) {
        return null;
    }

    public MessageStoreConfig getMessageStoreConfig() {
        return messageStoreConfig;
    }

    public BrokerConfig getBrokerConfig() {
        return brokerConfig;
    }

    public TransientStorePool getTransientStorePool() {
        return transientStorePool;
    }
}
