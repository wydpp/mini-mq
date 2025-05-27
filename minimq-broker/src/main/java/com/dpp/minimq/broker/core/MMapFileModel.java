package com.dpp.minimq.broker.core;

import com.dpp.minimq.broker.cache.CommonCache;
import com.dpp.minimq.broker.constants.BrokerConstants;
import com.dpp.minimq.broker.model.CommitLogMessageModel;
import com.dpp.minimq.broker.model.CommitLogModel;
import com.dpp.minimq.broker.model.TopicInfoModel;
import com.dpp.minimq.broker.utils.ByteConvertUtil;
import com.dpp.minimq.broker.utils.CommitLogFileNameUtil;
import io.netty.util.internal.PlatformDependent;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

/**
 * @author dpp
 * @date 2025/5/16
 * @Description
 */
public class MMapFileModel {
    private File file;
    private MappedByteBuffer mappedByteBuffer;
    private FileChannel fileChannel;
    private String topic;

    public MMapFileModel() {
    }

    /**
     * 从指定的offset开始映射文件
     *
     * @param topicName
     * @param startOffset
     * @param size
     */
    public void loadFileInMMap(String topicName, long startOffset, long size) throws IOException {
        String filePath = getLatestCommitLogFilePath(topicName);
        this.topic = topicName;
        doMMap(filePath, startOffset, size);
    }

    private void doMMap(String filePath, long startOffset, long size) throws IOException {
        this.file = new File(filePath);
        if (!file.exists()) {
            throw new FileNotFoundException("filePath " + filePath + " inValid");
        }
        this.fileChannel = new RandomAccessFile(file, "rw").getChannel();
        this.mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, startOffset, size);
    }

    private String getLatestCommitLogFilePath(String topicName) {
        TopicInfoModel topicInfoModel = CommonCache.getTopicInfoModelMap().get(topicName);
        if (topicInfoModel == null) {
            throw new RuntimeException("topic " + topicName + " inValid");
        }
        CommitLogModel latestCommitLog = topicInfoModel.getLatestCommitLog();
        long diff = latestCommitLog.getOffsetLimit() - latestCommitLog.getOffset();
        String filePath = null;
        if (diff == 0) {
            //已经写满了，创建新的文件
            filePath = this.createNewCommitLogFile(topicName, latestCommitLog.getFileName());
        } else if (diff > 0) {
            //还有机会写入
            filePath = CommonCache.getGlobalProperties().getMiniMqHome()
                    + BrokerConstants.BASE_STORE_PATH
                    + "/" + topicName
                    + "/" + latestCommitLog.getFileName();
        }
        return filePath;
    }

    private String createNewCommitLogFile(String topicName, String fileName) {
        String newCommitLogFileName = CommitLogFileNameUtil.incrementCommitLogFileName(fileName);
        String newFilePath = CommonCache.getGlobalProperties().getMiniMqHome()
                + BrokerConstants.BASE_STORE_PATH
                + "/" + topicName
                + "/" + newCommitLogFileName;
        File newFile = new File(newFilePath);
        if (newFile.exists()) {
            throw new RuntimeException("newFile " + newFilePath + " inValid");
        }
        try {
            newFile.createNewFile();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return newFilePath;
    }

    /**
     * 从文件的指定offset开始读数据
     *
     * @param startOffset
     * @param size
     * @return
     */
    public byte[] readContent(int startOffset, int size) {
        mappedByteBuffer.position(startOffset);
        byte[] result = new byte[size];
        mappedByteBuffer.get(result);
        return result;
    }

    public void writeContent(CommitLogMessageModel commitLogMessageModel) throws IOException {
        writeContent(commitLogMessageModel, false);
    }

    /**
     * 文件写数据
     */
    public void writeContent(CommitLogMessageModel commitLogMessageModel, boolean force) throws IOException {
        //定位到最新的commitLog文件中，记录下当前文件是否已经写满，如果写满，则创建新的文件，并且做新的映射
        //如果当前文件没有写满，对content内容做一层封装，在判断写入是否会导致CommitLog文件写满，如果写满，则创建新的文件，并且做新的映射
        //如果当前文件没有写满，直接写入content内容
        //定义一个对象，专门管理各个topic最新写入的offset值，并且定时刷新到磁盘中
        //写入数据，offset变更，如果高并发场景，offset会不会被多个线程访问
        //加锁机制

        //判断当前文件是否已经写满
        this.checkCommitLogHasEnableSpace(commitLogMessageModel);
        //默认刷到page cache,如果需要强制刷盘,可以使用mappedByteBuffer.force()
        mappedByteBuffer.put(commitLogMessageModel.convertToBytes());
        if (force) {
            mappedByteBuffer.force();
        }
    }

    private void checkCommitLogHasEnableSpace(CommitLogMessageModel commitLogMessageModel) throws IOException {
        TopicInfoModel topicInfoModel = CommonCache.getTopicInfoModelMap().get(topic);
        CommitLogModel commitLogModel = topicInfoModel.getLatestCommitLog();
        long diff = commitLogModel.getOffsetLimit() - commitLogModel.getOffset();
        if (diff >= commitLogMessageModel.getSize()) {
            //还有空间
            return;
        }else {
            //没有空间，创建新的文件
            String newCommitLogFile = this.createNewCommitLogFile(topic, commitLogModel.getFileName());
            this.doMMap(newCommitLogFile, 0, BrokerConstants.COMMITLONG_DEFAULT_MMAP_SIZE);
        }
    }

    /**
     * 释放内存映射
     */
    public void clean() {
        //使用netty的工具类来释放内存映射
        if (mappedByteBuffer != null) {
            PlatformDependent.freeDirectBuffer(mappedByteBuffer);
        }
    }

}
