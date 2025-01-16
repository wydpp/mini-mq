package com.dpp.minimq.store;

import com.dpp.minimq.common.UtilAll;
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
import java.util.function.Supplier;

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

    private volatile long beginTimeInLock = 0;

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
            if (putMessageResult != null) {
                return CompletableFuture.completedFuture(putMessageResult);
            }
            //设置encode之后的消息
            msg.setEncodeBuff(putMessageThreadLocal.getEncoder().getEncoderBuffer());
            PutMessageContext putMessageContext = new PutMessageContext(topicQueueKey);

            putMessageLock.lock();
            try {
                long beginLockTimestamp = this.defaultMessageStore.getSystemClock().now();
                this.beginTimeInLock = beginLockTimestamp;
                msg.setStoreTimestamp(beginLockTimestamp);
                if (mappedFile == null || mappedFile.isFull()) {
                    //获取一个新的文件映射
                    mappedFile = this.mappedFileQueue.getLastMappedFile(0);
                }
                if (mappedFile == null) {
                    //不能生成文件了
                    log.error("Create mapped file failed, server shutdown, topic: {} queueId: {}", msg.getTopic(), msg.getQueueId());
                    beginTimeInLock = 0;
                    return CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.CREATE_MAPPED_FILE_FAILED, null));
                }
                result = mappedFile.appendMessage(msg, this.appendMessageCallback, putMessageContext);
            } finally {
                putMessageLock.unlock();
            }

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
            this.byteBuf.writeShort((short) propertiesLength);
            if (propertiesLength > 0) {
                this.byteBuf.writeBytes(propertiesData);
            }
            return null;
        }

        public ByteBuffer getEncoderBuffer() {
            return this.byteBuf.nioBuffer();
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

    class DefaultAppendMessageCallback implements AppendMessageCallback {
        // File at the end of the minimum fixed length empty
        private static final int END_FILE_MIN_BLANK_LENGTH = 4 + 4;
        // Store the message content
        private final ByteBuffer msgStoreItemMemory;

        DefaultAppendMessageCallback() {
            this.msgStoreItemMemory = ByteBuffer.allocate(END_FILE_MIN_BLANK_LENGTH);
        }

        /**
         * 消息保存实现
         * @param fileFromOffset
         * @param byteBuffer
         * @param maxBlank
         * @param msgInner
         * @param putMessageContext
         * @return
         */
        public AppendMessageResult doAppend(final long fileFromOffset, final ByteBuffer byteBuffer, final int maxBlank,
                                            final Message msgInner, PutMessageContext putMessageContext) {
            // STORETIMESTAMP + STOREHOSTADDRESS + OFFSET <br>

            // PHY OFFSET
            long wroteOffset = fileFromOffset + byteBuffer.position();

            Supplier<String> msgIdSupplier = () -> {
                int sysflag = msgInner.getSysFlag();
                int msgIdLen = (sysflag & MessageSysFlag.STOREHOSTADDRESS_V6_FLAG) == 0 ? 4 + 4 + 8 : 16 + 4 + 8;
                ByteBuffer msgIdBuffer = ByteBuffer.allocate(msgIdLen);
                MessageExt.socketAddress2ByteBuffer(msgInner.getStoreHost(), msgIdBuffer);
                msgIdBuffer.clear();//because socketAddress2ByteBuffer flip the buffer
                msgIdBuffer.putLong(msgIdLen - 8, wroteOffset);
                return UtilAll.bytes2string(msgIdBuffer.array());
            };

            // Record ConsumeQueue information
            Long queueOffset = msgInner.getQueueOffset();

            // this msg maybe a inner-batch msg.
            short messageNum = getMessageNum(msgInner);

            // Transaction messages that require special handling
            final int tranType = MessageSysFlag.getTransactionValue(msgInner.getSysFlag());
            switch (tranType) {
                // Prepared and Rollback message is not consumed, will not enter the consume queue
                case MessageSysFlag.TRANSACTION_PREPARED_TYPE:
                case MessageSysFlag.TRANSACTION_ROLLBACK_TYPE:
                    queueOffset = 0L;
                    break;
                case MessageSysFlag.TRANSACTION_NOT_TYPE:
                case MessageSysFlag.TRANSACTION_COMMIT_TYPE:
                default:
                    break;
            }

            ByteBuffer preEncodeBuffer = msgInner.getEncodedBuff();
            final int msgLen = preEncodeBuffer.getInt(0);

            // Determines whether there is sufficient free space
            if ((msgLen + END_FILE_MIN_BLANK_LENGTH) > maxBlank) {
                this.msgStoreItemMemory.clear();
                // 1 TOTALSIZE
                this.msgStoreItemMemory.putInt(maxBlank);
                // 2 MAGICCODE
                this.msgStoreItemMemory.putInt(CommitLog.BLANK_MAGIC_CODE);
                // 3 The remaining space may be any value
                // Here the length of the specially set maxBlank
                final long beginTimeMills = CommitLog.this.defaultMessageStore.now();
                byteBuffer.put(this.msgStoreItemMemory.array(), 0, 8);
                return new AppendMessageResult(AppendMessageStatus.END_OF_FILE, wroteOffset,
                        maxBlank, /* only wrote 8 bytes, but declare wrote maxBlank for compute write position */
                        msgIdSupplier, msgInner.getStoreTimestamp(),
                        queueOffset, CommitLog.this.defaultMessageStore.now() - beginTimeMills);
            }

            int pos = 4 + 4 + 4 + 4 + 4;
            // 6 QUEUEOFFSET
            preEncodeBuffer.putLong(pos, queueOffset);
            pos += 8;
            // 7 PHYSICALOFFSET
            preEncodeBuffer.putLong(pos, fileFromOffset + byteBuffer.position());
            int ipLen = (msgInner.getSysFlag() & MessageSysFlag.BORNHOST_V6_FLAG) == 0 ? 4 + 4 : 16 + 4;
            // 8 SYSFLAG, 9 BORNTIMESTAMP, 10 BORNHOST, 11 STORETIMESTAMP
            pos += 8 + 4 + 8 + ipLen;
            // refresh store time stamp in lock
            preEncodeBuffer.putLong(pos, msgInner.getStoreTimestamp());

            final long beginTimeMills = CommitLog.this.defaultMessageStore.now();
            CommitLog.this.getMessageStore().getPerfCounter().startTick("WRITE_MEMORY_TIME_MS");
            // Write messages to the queue buffer
            byteBuffer.put(preEncodeBuffer);
            CommitLog.this.getMessageStore().getPerfCounter().endTick("WRITE_MEMORY_TIME_MS");
            msgInner.setEncodedBuff(null);
            return new AppendMessageResult(AppendMessageStatus.PUT_OK, wroteOffset, msgLen, msgIdSupplier,
                    msgInner.getStoreTimestamp(), queueOffset, CommitLog.this.defaultMessageStore.now() - beginTimeMills, messageNum);
        }

        public AppendMessageResult doAppend(final long fileFromOffset, final ByteBuffer byteBuffer, final int maxBlank,
                                            final Message messageExtBatch, PutMessageContext putMessageContext) {
            byteBuffer.mark();
            //physical offset
            long wroteOffset = fileFromOffset + byteBuffer.position();
            // Record ConsumeQueue information
            Long queueOffset = messageExtBatch.getQueueOffset();
            long beginQueueOffset = queueOffset;
            int totalMsgLen = 0;
            int msgNum = 0;

            final long beginTimeMills = CommitLog.this.defaultMessageStore.now();
            ByteBuffer messagesByteBuff = messageExtBatch.getEncodedBuff();

            int sysFlag = messageExtBatch.getSysFlag();
            int bornHostLength = (sysFlag & MessageSysFlag.BORNHOST_V6_FLAG) == 0 ? 4 + 4 : 16 + 4;
            int storeHostLength = (sysFlag & MessageSysFlag.STOREHOSTADDRESS_V6_FLAG) == 0 ? 4 + 4 : 16 + 4;
            Supplier<String> msgIdSupplier = () -> {
                int msgIdLen = storeHostLength + 8;
                int batchCount = putMessageContext.getBatchSize();
                long[] phyPosArray = putMessageContext.getPhyPos();
                ByteBuffer msgIdBuffer = ByteBuffer.allocate(msgIdLen);
                MessageExt.socketAddress2ByteBuffer(messageExtBatch.getStoreHost(), msgIdBuffer);
                msgIdBuffer.clear();//because socketAddress2ByteBuffer flip the buffer

                StringBuilder buffer = new StringBuilder(batchCount * msgIdLen * 2 + batchCount - 1);
                for (int i = 0; i < phyPosArray.length; i++) {
                    msgIdBuffer.putLong(msgIdLen - 8, phyPosArray[i]);
                    String msgId = UtilAll.bytes2string(msgIdBuffer.array());
                    if (i != 0) {
                        buffer.append(',');
                    }
                    buffer.append(msgId);
                }
                return buffer.toString();
            };

            messagesByteBuff.mark();
            int index = 0;
            while (messagesByteBuff.hasRemaining()) {
                // 1 TOTALSIZE
                final int msgPos = messagesByteBuff.position();
                final int msgLen = messagesByteBuff.getInt();

                totalMsgLen += msgLen;
                // Determines whether there is sufficient free space
                if ((totalMsgLen + END_FILE_MIN_BLANK_LENGTH) > maxBlank) {
                    this.msgStoreItemMemory.clear();
                    // 1 TOTALSIZE
                    this.msgStoreItemMemory.putInt(maxBlank);
                    // 2 MAGICCODE
                    this.msgStoreItemMemory.putInt(CommitLog.BLANK_MAGIC_CODE);
                    // 3 The remaining space may be any value
                    //ignore previous read
                    messagesByteBuff.reset();
                    // Here the length of the specially set maxBlank
                    byteBuffer.reset(); //ignore the previous appended messages
                    byteBuffer.put(this.msgStoreItemMemory.array(), 0, 8);
                    return new AppendMessageResult(AppendMessageStatus.END_OF_FILE, wroteOffset, maxBlank, msgIdSupplier, messageExtBatch.getStoreTimestamp(),
                            beginQueueOffset, CommitLog.this.defaultMessageStore.now() - beginTimeMills);
                }
                //move to add queue offset and commitlog offset
                int pos = msgPos + 20;
                messagesByteBuff.putLong(pos, queueOffset);
                pos += 8;
                messagesByteBuff.putLong(pos, wroteOffset + totalMsgLen - msgLen);
                // 8 SYSFLAG, 9 BORNTIMESTAMP, 10 BORNHOST, 11 STORETIMESTAMP
                pos += 8 + 4 + 8 + bornHostLength;
                // refresh store time stamp in lock
                messagesByteBuff.putLong(pos, messageExtBatch.getStoreTimestamp());

                putMessageContext.getPhyPos()[index++] = wroteOffset + totalMsgLen - msgLen;
                queueOffset++;
                msgNum++;
                messagesByteBuff.position(msgPos + msgLen);
            }

            messagesByteBuff.position(0);
            messagesByteBuff.limit(totalMsgLen);
            byteBuffer.put(messagesByteBuff);
            messageExtBatch.setEncodedBuff(null);
            AppendMessageResult result = new AppendMessageResult(AppendMessageStatus.PUT_OK, wroteOffset, totalMsgLen, msgIdSupplier,
                    messageExtBatch.getStoreTimestamp(), beginQueueOffset, CommitLog.this.defaultMessageStore.now() - beginTimeMills);
            result.setMsgNum(msgNum);

            return result;
        }

    }
}
