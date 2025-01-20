package com.dpp.minimq.store;

import com.dpp.minimq.common.ServiceThread;
import com.dpp.minimq.common.UtilAll;
import com.dpp.minimq.common.message.Message;
import com.dpp.minimq.common.sysflag.MessageSysFlag;
import com.dpp.minimq.store.config.FlushDiskType;
import com.dpp.minimq.store.logfile.MappedFile;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.UnpooledByteBufAllocator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.LinkedList;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
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

    private final FlushManager flushManager;

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
        long elapsedTimeInLock = 0;
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
                switch (result.getStatus()){
                    case PUT_OK:
                        //写入文件成功
                        onCommitLogAppend(msg, result, mappedFile);
                        break;
                    case END_OF_FILE:
                        onCommitLogAppend(msg, result, mappedFile);
                        unlockMappedFile = mappedFile;
                        //新建文件，尝试重写
                        mappedFile = this.mappedFileQueue.getLastMappedFile(0);
                        if (null == mappedFile){
                            log.error("create mapped file2 error, topic: " + msg.getTopic());
                            beginTimeInLock = 0;
                            return CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.CREATE_MAPPED_FILE_FAILED, result));
                        }
                        result = mappedFile.appendMessage(msg, this.appendMessageCallback, putMessageContext);
                        if (AppendMessageStatus.PUT_OK.equals(result.getStatus())){
                            //写入文件成功
                            onCommitLogAppend(msg, result, mappedFile);
                        }else {
                            //再次失败就不处理了
                            log.error("appendMessage2 error, do nothing, topic: " + msg.getTopic());
                        }
                        break;
                        case MESSAGE_SIZE_EXCEEDED:
                        case PROPERTIES_SIZE_EXCEEDED:
                            beginTimeInLock = 0;
                            return CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.MESSAGE_ILLEGAL, result));
                        case UNKNOWN_ERROR:
                            beginTimeInLock = 0;
                            return CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.UNKNOWN_ERROR, result));
                        default:
                            beginTimeInLock = 0;
                            return CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.UNKNOWN_ERROR, result));
                }
                elapsedTimeInLock = this.defaultMessageStore.getSystemClock().now() - beginLockTimestamp;
                beginTimeInLock = 0;
            } finally {
                putMessageLock.unlock();
            }
        } finally {
            //释放锁
            topicQueueLock.unlock(topicQueueKey);
        }
        if (elapsedTimeInLock > 500) {
            log.warn("[NOTIFYME]putMessage in lock cost time(ms)={}, bodyLength={} AppendMessageResult={}", elapsedTimeInLock, msg.getBody().length, result);
        }

        PutMessageResult putMessageResult = new PutMessageResult(PutMessageStatus.PUT_OK, result);
        return null;
    }

    protected void onCommitLogAppend(Message msg, AppendMessageResult result, MappedFile commitLogFile) {
        this.getMessageStore().onCommitLogAppend(msg, result, commitLogFile);
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

    public MessageStore getMessageStore() {
        return defaultMessageStore;
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
            //1 消息总长度
            this.byteBuf.writeInt(msgLen);
            //2 队列ID
            this.byteBuf.writeInt(msg.getQueueId());
            //3 PHYSICALOFFSET
            this.byteBuf.writeLong(0);
            //4 sysFlag
            this.byteBuf.writeInt(msg.getSysFlag());
            //5 BORNTIMESTAMP
            this.byteBuf.writeLong(msg.getBornTimestamp());
            //6 STORETIMESTAMP
            this.byteBuf.writeLong(msg.getStoreTimestamp());
            //7 BODY
            this.byteBuf.writeInt(bodyLength);
            if (bodyLength > 0) {
                this.byteBuf.writeBytes(msg.getBody());
            }
            //8 topic topicLength不能大于byte的范围[-127,127]
            this.byteBuf.writeByte((byte) topicLength);
            this.byteBuf.writeBytes(topicData);
            //9 properties
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

    private CompletableFuture<PutMessageResult> handleDiskFlushAndHA(PutMessageResult putMessageResult,
                                                                     Message messageExt, int needAckNums, boolean needHandleHA) {
        CompletableFuture<PutMessageStatus> flushResultFuture = handleDiskFlush(putMessageResult.getAppendMessageResult(), messageExt);
        CompletableFuture<PutMessageStatus> replicaResultFuture;
        if (!needHandleHA) {
            replicaResultFuture = CompletableFuture.completedFuture(PutMessageStatus.PUT_OK);
        } else {
            replicaResultFuture = handleHA(putMessageResult.getAppendMessageResult(), putMessageResult, needAckNums);
        }

        return flushResultFuture.thenCombine(replicaResultFuture, (flushStatus, replicaStatus) -> {
            if (flushStatus != PutMessageStatus.PUT_OK) {
                putMessageResult.setPutMessageStatus(flushStatus);
            }
            if (replicaStatus != PutMessageStatus.PUT_OK) {
                putMessageResult.setPutMessageStatus(replicaStatus);
            }
            return putMessageResult;
        });
    }

    private CompletableFuture<PutMessageStatus> handleDiskFlush(AppendMessageResult result, Message messageExt) {
        return this.flushManager.handleDiskFlush(result, messageExt);
    }

    /**
     * 负责管理数据刷盘操作的重要组件.
     * 在消息存储系统中，数据的刷盘操作是将内存中的数据持久化到磁盘的过程，以确保数据的可靠性和持久性。
     * FlushManager 的主要任务是协调和管理消息存储引擎（如 CommitLog 和 ConsumeQueue）的刷盘操作，确保数据在适当的时候从内存安全地存储到磁盘.
     */
    interface FlushManager {
        void start();

        void shutdown();

        void wakeUpFlush();

        void handleDiskFlush(AppendMessageResult result, PutMessageResult putMessageResult, Message messageExt);

        CompletableFuture<PutMessageStatus> handleDiskFlush(AppendMessageResult result, Message messageExt);
    }

    abstract class FlushCommitLogService extends ServiceThread {
        protected static final int RETRY_TIMES_OVER = 10;
    }

    /**
     * 负责将内存中的数据实时提交到磁盘，以确保数据的持久化和消息存储的可靠性。
     */
    class FlushRealTimeService extends FlushCommitLogService {

        private long lastFlushTimestamp = 0;
        private long printTimes = 0;

        @Override
        public String getServiceName() {
            return FlushRealTimeService.class.getSimpleName();
        }

        @Override
        public void run() {
            CommitLog.log.info(this.getServiceName() + " service started");
            while (!this.isStopped()) {
                boolean flushCommitLogTimed = CommitLog.this.defaultMessageStore.getMessageStoreConfig().isFlushCommitLogTimed();

                int interval = CommitLog.this.defaultMessageStore.getMessageStoreConfig().getFlushIntervalCommitLog();

                int flushPhysicQueueLeastPages = CommitLog.this.defaultMessageStore.getMessageStoreConfig().getFlushCommitLogLeastPages();

                int flushPhysicQueueThoroughInterval =
                        CommitLog.this.defaultMessageStore.getMessageStoreConfig().getFlushCommitLogThoroughInterval();

                try {
                    if (flushCommitLogTimed) {
                        Thread.sleep(interval);
                    } else {
                        this.waitForRunning(interval);
                    }
                    long begin = System.currentTimeMillis();
                    //同步磁盘
                    CommitLog.this.mappedFileQueue.flush(flushPhysicQueueLeastPages);
                    long storeTimestamp = CommitLog.this.mappedFileQueue.getStoreTimestamp();
                    if (storeTimestamp > 0) {
                        //CommitLog.this.defaultMessageStore.getStoreCheckpoint().setPhysicMsgTimestamp(storeTimestamp);
                    }
                    long past = System.currentTimeMillis();
                    if (past > 500) {
                        log.info("Flush data to disk costs {} ms", past);
                    }
                } catch (Throwable e) {
                    CommitLog.log.error(this.getServiceName() + " service has exception. ", e);
                }
            }

            boolean result = false;
            for (int i = 0; i < RETRY_TIMES_OVER && !result; i++) {
                result = CommitLog.this.mappedFileQueue.commit(0);
                CommitLog.log.info(this.getServiceName() + " service shutdown, retry " + (i + 1) + " times " + (result ? "OK" : "Not OK"));
            }
            CommitLog.log.info(this.getServiceName() + " service end");
        }
    }

    class GroupCommitService extends FlushCommitLogService {
        private volatile LinkedList<GroupCommitRequest> requestsWrite = new LinkedList<>();
        private volatile LinkedList<GroupCommitRequest> requestsRead = new LinkedList<>();
        private final PutMessageSpinLock lock = new PutMessageSpinLock();

        public void putRequest(final GroupCommitRequest request) {
            lock.lock();
            try {
                this.requestsWrite.add(request);
            } finally {
                lock.unlock();
            }
            this.wakeup();
        }

        private void swapRequests() {
            lock.lock();
            try {
                LinkedList<GroupCommitRequest> tmp = this.requestsWrite;
                this.requestsWrite = this.requestsRead;
                this.requestsRead = tmp;
            } finally {
                lock.unlock();
            }
        }

        private void doCommit() {
            if (!this.requestsRead.isEmpty()) {
                for (GroupCommitRequest req : this.requestsRead) {
                    // There may be a message in the next file, so a maximum of
                    // two times the flush
                    boolean flushOK = CommitLog.this.mappedFileQueue.getFlushedWhere() >= req.getNextOffset();
                    for (int i = 0; i < 2 && !flushOK; i++) {
                        CommitLog.this.mappedFileQueue.flush(0);
                        flushOK = CommitLog.this.mappedFileQueue.getFlushedWhere() >= req.getNextOffset();
                    }

                    req.wakeupCustomer(flushOK ? PutMessageStatus.PUT_OK : PutMessageStatus.FLUSH_DISK_TIMEOUT);
                }

                long storeTimestamp = CommitLog.this.mappedFileQueue.getStoreTimestamp();
                if (storeTimestamp > 0) {
                    //CommitLog.this.defaultMessageStore.getStoreCheckpoint().setPhysicMsgTimestamp(storeTimestamp);
                }
                this.requestsRead = new LinkedList<>();
            } else {
                // Because of individual messages is set to not sync flush, it
                // will come to this process
                CommitLog.this.mappedFileQueue.flush(0);
            }
        }

        @Override
        public void run() {
            CommitLog.log.info(this.getServiceName() + " service started");

            while (!this.isStopped()) {
                try {
                    this.waitForRunning(10);
                    this.doCommit();
                } catch (Exception e) {
                    CommitLog.log.warn(this.getServiceName() + " service has exception. ", e);
                }
            }

            // Under normal circumstances shutdown, wait for the arrival of the
            // request, and then flush
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                CommitLog.log.warn("GroupCommitService Exception, ", e);
            }

            this.swapRequests();
            this.doCommit();

            CommitLog.log.info(this.getServiceName() + " service end");
        }

        @Override
        protected void onWaitEnd() {
            this.swapRequests();
        }

        @Override
        public String getServiceName() {
            return GroupCommitService.class.getSimpleName();
        }

        @Override
        public long getJoinTime() {
            return 1000 * 60 * 5;
        }
    }

    class DefaultFlushManager implements FlushManager {

        private final FlushCommitLogService flushCommitLogService;

        public DefaultFlushManager() {
            if (FlushDiskType.SYNC_FLUSH == CommitLog.this.defaultMessageStore.getMessageStoreConfig().getFlushDiskType()) {
                //异步刷盘
                this.flushCommitLogService = new CommitLog.GroupCommitService();
            } else {
                //同步刷盘
                this.flushCommitLogService = new CommitLog.FlushRealTimeService();
            }
        }

        @Override
        public void start() {
            this.flushCommitLogService.start();

            if (defaultMessageStore.getMessageStoreConfig().isTransientStorePoolEnable()) {
                this.commitLogService.start();
            }
        }

        public void handleDiskFlush(AppendMessageResult result, PutMessageResult putMessageResult,
                                    Message messageExt) {
            // Synchronization flush
            if (FlushDiskType.SYNC_FLUSH == CommitLog.this.defaultMessageStore.getMessageStoreConfig().getFlushDiskType()) {
                final GroupCommitService service = (GroupCommitService) this.flushCommitLogService;
                if (messageExt.isWaitStoreMsgOK()) {
                    GroupCommitRequest request = new GroupCommitRequest(result.getWroteOffset() + result.getWroteBytes(), CommitLog.this.defaultMessageStore.getMessageStoreConfig().getSyncFlushTimeout());
                    service.putRequest(request);
                    CompletableFuture<PutMessageStatus> flushOkFuture = request.future();
                    PutMessageStatus flushStatus = null;
                    try {
                        flushStatus = flushOkFuture.get(CommitLog.this.defaultMessageStore.getMessageStoreConfig().getSyncFlushTimeout(),
                                TimeUnit.MILLISECONDS);
                    } catch (InterruptedException | ExecutionException | TimeoutException e) {
                        //flushOK=false;
                    }
                    if (flushStatus != PutMessageStatus.PUT_OK) {
                        log.error("do groupcommit, wait for flush failed, topic: " + messageExt.getTopic() + " tags: " + messageExt.getTags()
                                + " client address: " + messageExt.getBornHostString());
                        putMessageResult.setPutMessageStatus(PutMessageStatus.FLUSH_DISK_TIMEOUT);
                    }
                } else {
                    service.wakeup();
                }
            }
            // Asynchronous flush
            else {
                if (!CommitLog.this.defaultMessageStore.getMessageStoreConfig().isTransientStorePoolEnable()) {
                    flushCommitLogService.wakeup();
                } else {
                    commitLogService.wakeup();
                }
            }
        }

        @Override
        public CompletableFuture<PutMessageStatus> handleDiskFlush(AppendMessageResult result, Message messageExt) {
            // Synchronization flush
            if (FlushDiskType.SYNC_FLUSH == CommitLog.this.defaultMessageStore.getMessageStoreConfig().getFlushDiskType()) {
                final GroupCommitService service = (GroupCommitService) this.flushCommitLogService;
                if (messageExt.isWaitStoreMsgOK()) {
                    GroupCommitRequest request = new GroupCommitRequest(result.getWroteOffset() + result.getWroteBytes(), CommitLog.this.defaultMessageStore.getMessageStoreConfig().getSyncFlushTimeout());
                    flushDiskWatcher.add(request);
                    service.putRequest(request);
                    return request.future();
                } else {
                    service.wakeup();
                    return CompletableFuture.completedFuture(PutMessageStatus.PUT_OK);
                }
            }
            // Asynchronous flush
            else {
                if (!CommitLog.this.defaultMessageStore.getMessageStoreConfig().isTransientStorePoolEnable()) {
                    flushCommitLogService.wakeup();
                } else {
                    commitLogService.wakeup();
                }
                return CompletableFuture.completedFuture(PutMessageStatus.PUT_OK);
            }
        }

        @Override
        public void wakeUpFlush() {
            // now wake up flush thread.
            flushCommitLogService.wakeup();
        }

        @Override
        public void shutdown() {
            if (defaultMessageStore.getMessageStoreConfig().isTransientStorePoolEnable()) {
                this.commitLogService.shutdown();
            }

            this.flushCommitLogService.shutdown();
        }

    }

    public static class GroupCommitRequest {
        private final long nextOffset;
        // Indicate the GroupCommitRequest result: true or false
        private final CompletableFuture<PutMessageStatus> flushOKFuture = new CompletableFuture<>();
        private volatile int ackNums = 1;
        private final long deadLine;

        public GroupCommitRequest(long nextOffset, long timeoutMillis) {
            this.nextOffset = nextOffset;
            this.deadLine = System.nanoTime() + (timeoutMillis * 1_000_000);
        }

        public GroupCommitRequest(long nextOffset, long timeoutMillis, int ackNums) {
            this(nextOffset, timeoutMillis);
            this.ackNums = ackNums;
        }

        public long getNextOffset() {
            return nextOffset;
        }

        public int getAckNums() {
            return ackNums;
        }

        public long getDeadLine() {
            return deadLine;
        }

        public void wakeupCustomer(final PutMessageStatus status) {
            this.flushOKFuture.complete(status);
        }

        public CompletableFuture<PutMessageStatus> future() {
            return flushOKFuture;
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
