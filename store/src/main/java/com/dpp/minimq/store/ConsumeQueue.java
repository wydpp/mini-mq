package com.dpp.minimq.store;

import com.dpp.minimq.common.message.Message;
import com.dpp.minimq.common.message.MessageConst;
import com.dpp.minimq.store.logfile.MappedFile;
import com.dpp.minimq.store.queue.ConsumeQueueInterface;
import com.dpp.minimq.store.queue.QueueOffsetAssigner;
import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.nio.ByteBuffer;

/**
 * @author dpp
 * @date 2025/1/7
 * @Description
 */
public class ConsumeQueue implements ConsumeQueueInterface {

    public static final int CQ_STORE_UNIT_SIZE = 20;

    private final MessageStore messageStore;

    private final MappedFileQueue mappedFileQueue;
    private final String topic;
    private final int queueId;
    private final ByteBuffer byteBufferIndex;

    private final String storePath;
    private final int mappedFileSize;
    private long maxPhysicOffset = -1;

    public ConsumeQueue(String topic, int queueId, String storePath, int mappedFileSize, MessageStore messageStore) {
        this.storePath = storePath;
        this.mappedFileSize = mappedFileSize;
        this.messageStore = messageStore;
        this.topic = topic;
        this.queueId = queueId;
        String queueDir = this.storePath
                + File.separator + topic
                + File.separator + queueId;
        this.mappedFileQueue = new MappedFileQueue(queueDir, mappedFileSize, null);
        this.byteBufferIndex = ByteBuffer.allocate(CQ_STORE_UNIT_SIZE);
    }

    @Override
    public String getTopic() {
        return topic;
    }

    @Override
    public int getQueueId() {
        return queueId;
    }

    @Override
    public long getLastOffset() {
        long lastOffset = -1;

        int logicFileSize = this.mappedFileSize;

        MappedFile mappedFile = this.mappedFileQueue.getLastMappedFile();
        if (mappedFile != null) {

            int position = mappedFile.getWrotePosition() - CQ_STORE_UNIT_SIZE;
            if (position < 0)
                position = 0;

            ByteBuffer byteBuffer = mappedFile.sliceByteBuffer();
            byteBuffer.position(position);
            for (int i = 0; i < logicFileSize; i += CQ_STORE_UNIT_SIZE) {
                long offset = byteBuffer.getLong();
                int size = byteBuffer.getInt();
                byteBuffer.getLong();

                if (offset >= 0 && size > 0) {
                    lastOffset = offset + size;
                } else {
                    break;
                }
            }
        }

        return lastOffset;
    }

    @Override
    public long getMinOffsetInQueue() {
        return 0;
    }

    @Override
    public long getMaxOffsetInQueue() {
        return 0;
    }

    @Override
    public long getMessageTotalInQueue() {
        return 0;
    }

    @Override
    public long getOffsetInQueueByTime(long timestamp) {
        return 0;
    }

    @Override
    public long getMaxPhysicOffset() {
        return 0;
    }

    @Override
    public long getMinLogicOffset() {
        return 0;
    }

    @Override
    public long getTotalSize() {
        return 0;
    }

    @Override
    public int getUnitSize() {
        return 0;
    }

    @Override
    public void correctMinOffset(long minCommitLogOffset) {

    }

    @Override
    public void assignQueueOffset(QueueOffsetAssigner queueOffsetAssigner, Message msg, short messageNum) {
        String topicQueueKey = getTopic() + "-" + getQueueId();
        long queueOffset = queueOffsetAssigner.assignQueueOffset(topicQueueKey, messageNum);
        // 设置消息的队列偏移量
        msg.setQueueOffset(queueOffset);
        String multiDispatchQueue = msg.getProperty(MessageConst.PROPERTY_INNER_MULTI_DISPATCH);
        if (StringUtils.isBlank(multiDispatchQueue)) {
            return;
        }
        //TODO 暂不处理多分发逻辑
    }
}
