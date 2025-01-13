package com.dpp.minimq.store;

import com.dpp.minimq.common.message.Message;
import com.dpp.minimq.store.logfile.MappedFile;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.UnpooledByteBufAllocator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;

/**
 * CommitLog 类是消息存储的核心组件之一，它负责持久化存储所有生产者发送的消息。
 * CommitLog 是一个按顺序写入的日志文件，消息会被连续地追加到该日志文件中。
 * 当消费者消费消息时，它会从 CommitLog 中读取数据。
 * <p>
 * CommitLog 是 RocketMQ 用来存储消息的“物理日志文件”，并且支持高效的消息写入和读取。
 * 它使用 MappedByteBuffer 进行文件映射，以实现快速随机读写。
 *
 * @author dpp
 * @date 2025/1/7
 * @Description
 */
public class CommitLog implements Swappable {
    private static final Logger log = LoggerFactory.getLogger(CommitLog.class);

    protected final TopicQueueLock topicQueueLock;

    protected final PutMessageLock putMessageLock;

    private final AppendMessageCallback appendMessageCallback;

    protected final DefaultMessageStore defaultMessageStore;

    protected int commitLogSize;

    private final MappedFileQueue mappedFileQueue;

    private final ThreadLocal<PutMessageThreadLocal> putMessageThreadLocal;

    public CommitLog(final DefaultMessageStore messageStore) {
        this.defaultMessageStore = messageStore;
        this.topicQueueLock = new TopicQueueLock();
        this.putMessageLock = new PutMessageReentrantLock();
        this.appendMessageCallback = new DefaultAppendMessageCallback();
        this.commitLogSize = messageStore.getMessageStoreConfig().getMappedFileSizeCommitLog();
        this.mappedFileQueue = new MappedFileQueue(messageStore.getMessageStoreConfig().getStorePathCommitLog(),
                commitLogSize, messageStore.getAllocateMappedFileService());
        putMessageThreadLocal = new ThreadLocal<PutMessageThreadLocal>() {
            @Override
            protected PutMessageThreadLocal initialValue() {
                return new PutMessageThreadLocal(defaultMessageStore.getMessageStoreConfig().getMaxMessageSize());
            }
        };
    }

    /**
     * 异步写入消息到CommitLog
     *
     * @param msg
     * @return
     */
    public CompletableFuture<PutMessageResult> asyncPutMessage(final Message msg) {
        msg.setStoreTimestamp(System.currentTimeMillis());
        AppendMessageResult result = null;
        String topic = msg.getTopic();
        String topicQueueKey = generateKey(msg);
        MappedFile unlockMappedFile = null;
        MappedFile mappedFile = this.mappedFileQueue.getLastMappedFile();
        //获取当前的偏移量
        long currOffset;
        if (mappedFile == null) {
            currOffset = 0;
        } else {
            currOffset = mappedFile.getFileFromOffset() + mappedFile.getWrotePosition();
        }
        //根据topicQueueKey加锁
        topicQueueLock.lock(topicQueueKey);
        PutMessageThreadLocal putMessageThreadLocal = this.putMessageThreadLocal.get();
        try {
            //分配消息的偏移量
            this.defaultMessageStore.assignOffset(msg, (short) 1);

            PutMessageResult putMessageResult = putMessageThreadLocal.getEncoder().encode(msg);
            if (putMessageResult != null){
                return CompletableFuture.completedFuture(putMessageResult);
            }

            putMessageLock.lock();
            //TODO

        } finally {
            //释放锁
            topicQueueLock.unlock(topicQueueKey);
        }
        return null;
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

    /**
     * 消息编码类
     */
    public static class MessageEncoder {
        private ByteBuf byteBuf;
        private int maxMessageBodySize;
        private int maxMessageSize;

        MessageEncoder(final int maxMessageBodySize) {
            ByteBufAllocator alloc = UnpooledByteBufAllocator.DEFAULT;
            // 消息最大长度
            // 预留64kb用于体外编码缓冲区(Reserve 64kb for encoding buffer outside body)
            int maxMessageSize = Integer.MAX_VALUE - maxMessageBodySize >= 64 * 1024 ?
                    maxMessageBodySize + 64 * 1024 : Integer.MAX_VALUE;
            byteBuf = alloc.directBuffer(maxMessageSize);
            this.maxMessageBodySize = maxMessageBodySize;
            this.maxMessageSize = maxMessageSize;
        }

        protected PutMessageResult encode(final Message msg) {
            this.byteBuf.clear();

            final byte[] propertiesData =
                    msg.getPropertiesString() == null ? null : msg.getPropertiesString().getBytes(StandardCharsets.UTF_8);

            final int propertiesLength = propertiesData == null ? 0 : propertiesData.length;

            final byte[] topicData = msg.getTopic().getBytes(StandardCharsets.UTF_8);
            final int topicLength = topicData.length;

            final int bodyLength = msg.getBody() == null ? 0 : msg.getBody().length;
            // 消息总长度
            final int msgLen = calMsgLength(bodyLength, topicLength, propertiesLength);

            if (bodyLength > this.maxMessageBodySize) {
                log.warn("message body size exceeded, msg total size: " + msgLen + ", msg body size: " + bodyLength
                        + ", maxMessageSize: " + this.maxMessageBodySize);
                return new PutMessageResult(PutMessageStatus.MESSAGE_ILLEGAL, null);
            }
            if (msgLen > this.maxMessageSize) {
                log.warn("message size exceeded, msg total size: " + msgLen + ", msg body size: " + bodyLength
                        + ", maxMessageSize: " + this.maxMessageSize);
                return new PutMessageResult(PutMessageStatus.MESSAGE_ILLEGAL, null);
            }
            //消息写入缓冲区
            this.byteBuf.writeInt(msgLen);//消息总长度
            this.byteBuf.writeInt(msg.getQueueId());//队列ID
            this.byteBuf.writeLong(0);//PHYSICALOFFSET
            this.byteBuf.writeLong(msg.getBornTimestamp());//BORNTIMESTAMP
            this.byteBuf.writeLong(msg.getStoreTimestamp());//STORETIMESTAMP
            this.byteBuf.writeInt(bodyLength);//BODYLENGTH
            if (bodyLength > 0) {
                this.byteBuf.writeBytes(msg.getBody());
            }
            //topicLength不能大于byte的范围[-127,127]
            this.byteBuf.writeByte((byte) topicLength);
            this.byteBuf.writeBytes(topicData);
            this.byteBuf.writeShort((short)propertiesLength);
            if (propertiesLength > 0){
                this.byteBuf.writeBytes(propertiesData);
            }
            return null;
        }
    }

    protected static int calMsgLength(int bodyLength, int topicLength, int propertiesLength) {
        final int msgLen = 4 //TOTALSIZE
                + 4 //QUEUEID
                + 8 //PHYSICALOFFSET
                + 8 //BORNTIMESTAMP
                + 8 //STORETIMESTAMP
                + 4 + (bodyLength > 0 ? bodyLength : 0) //BODY
                + 1 + topicLength //TOPIC
                + 2 + (propertiesLength > 0 ? propertiesLength : 0) //propertiesLength
                + 0;
        return msgLen;
    }

    static class PutMessageThreadLocal {
        private MessageEncoder encoder;
        private StringBuilder keyBuilder;

        PutMessageThreadLocal(int maxMessageBodySize) {
            encoder = new MessageEncoder(maxMessageBodySize);
            keyBuilder = new StringBuilder();
        }

        public MessageEncoder getEncoder() {
            return encoder;
        }

        public StringBuilder getKeyBuilder() {
            return keyBuilder;
        }
    }
}
