package com.dpp.minimq.store;

import com.dpp.minimq.common.BrokerConfig;
import com.dpp.minimq.common.SystemClock;
import com.dpp.minimq.common.message.Message;
import com.dpp.minimq.store.config.MessageStoreConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * 默认的 消息存储实现
 *
 * @author dpp
 * @date 2025/1/7
 * @Description
 */
public class DefaultMessageStore implements MessageStore {

    private static final Logger log = LoggerFactory.getLogger(DefaultMessageStore.class);

    private MessageStoreConfig messageStoreConfig;

    private final BrokerConfig brokerConfig;

    private final TransientStorePool transientStorePool;

    private final SystemClock systemClock = new SystemClock();

    private final CommitLog commitLog;

    private final AllocateMappedFileService allocateMappedFileService;

    public DefaultMessageStore(BrokerConfig brokerConfig, MessageStoreConfig messageStoreConfig) {
        this.brokerConfig = brokerConfig;
        this.messageStoreConfig = messageStoreConfig;
        this.transientStorePool = new TransientStorePool(messageStoreConfig);
        this.commitLog = new CommitLog(this);
        this.allocateMappedFileService = new AllocateMappedFileService(this);
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
    public CompletableFuture<PutMessageResult> asyncPutMessage(final Message msg) {
        long beginTime = this.getSystemClock().now();
        //异步写入消息到CommitLog
        CompletableFuture<PutMessageResult> putResultFuture = this.commitLog.asyncPutMessage(msg);
        putResultFuture.thenAccept(result -> {
            long eclipseTime = this.getSystemClock().now() - beginTime;
            if (eclipseTime > 500) {//大于500ms的异步消息，打印告警
                log.warn("DefaultMessageStore#putMessage: CommitLog#putMessage cost {}ms, topic={}, bodyLength={}",
                        eclipseTime, msg.getTopic(), msg.getBody().length);
            }
        });
        return putResultFuture;
    }

    @Override
    public PutMessageResult putMessage(Message msg) {
        return waitForPutMessageResult(asyncPutMessage(msg));
    }

    private PutMessageResult waitForPutMessageResult(CompletableFuture<PutMessageResult> putMessageResultFuture) {
        try {
            int putMessageTimeout =
                    Math.max(this.messageStoreConfig.getSyncFlushTimeout(),
                            this.messageStoreConfig.getSlaveTimeout()) + 5000;
            return putMessageResultFuture.get(putMessageTimeout, TimeUnit.MILLISECONDS);
        } catch (ExecutionException | InterruptedException e) {
            return new PutMessageResult(PutMessageStatus.UNKNOWN_ERROR, null);
        } catch (TimeoutException e) {
            //获取结果超时了，打印告警
            log.error("usually it will never timeout, putMessageTimeout is much bigger than slaveTimeout and "
                    + "flushTimeout so the result can be got anyway, but in some situations timeout will happen like full gc "
                    + "process hangs or other unexpected situations.");
            return new PutMessageResult(PutMessageStatus.UNKNOWN_ERROR, null);
        }
    }

    public void assignOffset(Message msg, short messageNum) {
        //this.consumeQueueStore.assignQueueOffset(msg, messageNum);
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

    public SystemClock getSystemClock() {
        return systemClock;
    }

    public AllocateMappedFileService getAllocateMappedFileService() {
        return allocateMappedFileService;
    }
}
