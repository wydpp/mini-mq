package com.dpp.minimq.store;

import com.dpp.minimq.common.message.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;

/**
 * CommitLog 类是消息存储的核心组件之一，它负责持久化存储所有生产者发送的消息。
 * CommitLog 是一个按顺序写入的日志文件，消息会被连续地追加到该日志文件中。
 * 当消费者消费消息时，它会从 CommitLog 中读取数据。
 *
 * CommitLog 是 RocketMQ 用来存储消息的“物理日志文件”，并且支持高效的消息写入和读取。
 * 它使用 MappedByteBuffer 进行文件映射，以实现快速随机读写。
 *
 * @author dpp
 * @date 2025/1/7
 * @Description
 */
public class CommitLog implements Swappable{
    private static final Logger log = LoggerFactory.getLogger(CommitLog.class);

    protected final TopicQueueLock topicQueueLock;

    protected final PutMessageLock putMessageLock;

    private final AppendMessageCallback appendMessageCallback;

    protected final DefaultMessageStore defaultMessageStore;

    protected int commitLogSize;

    public CommitLog(final DefaultMessageStore messageStore) {
        this.defaultMessageStore = messageStore;
        this.topicQueueLock = new TopicQueueLock();
        this.putMessageLock = new PutMessageReentrantLock();
        this.appendMessageCallback = new DefaultAppendMessageCallback();
        this.commitLogSize = messageStore.getMessageStoreConfig().getMappedFileSizeCommitLog();
    }

    /**
     * 异步写入消息到CommitLog
     * @param msg
     * @return
     */
    public CompletableFuture<PutMessageResult> asyncPutMessage(final Message msg) {
        msg.setStoreTimestamp(System.currentTimeMillis());
        AppendMessageResult result = null;
        String topic = msg.getTopic();
        String topicQueueKey = generateKey(msg);


    }

    private String generateKey(Message msg) {
        StringBuffer keyBuilder = new StringBuffer();
        keyBuilder.append(msg.getTopic()).append("-").append(msg.getQueueId());
        return keyBuilder.toString();
    }


    @Override
    public void swapMap(int reserveNum, long forceSwapIntervalMs, long normalSwapIntervalMs) {

    }

    @Override
    public void cleanSwappedMap(long forceCleanSwapIntervalMs) {

    }

    class DefaultAppendMessageCallback implements AppendMessageCallback {

        @Override
        public AppendMessageResult doAppend(long fileFromOffset, ByteBuffer byteBuffer, int maxBlank, Message msg, PutMessageContext putMessageContext) {
            return null;
        }
    }
}
