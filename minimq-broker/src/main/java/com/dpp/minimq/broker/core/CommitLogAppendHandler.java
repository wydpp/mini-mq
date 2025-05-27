package com.dpp.minimq.broker.core;

import com.dpp.minimq.broker.constants.BrokerConstants;
import com.dpp.minimq.broker.model.CommitLogMessageModel;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * @author dpp
 * @date 2025/5/16
 * @Description
 */
public class CommitLogAppendHandler {

    private MModelFileModelManager mModelFileModelManager = new MModelFileModelManager();

    public CommitLogAppendHandler(){
    }

    public void prepareMMapLoading(String topicName) throws IOException {
        MMapFileModel mMapFileModel = new MMapFileModel();
        mMapFileModel.loadFileInMMap(topicName, 0, BrokerConstants.COMMITLONG_DEFAULT_MMAP_SIZE);
        mModelFileModelManager.put(topicName, mMapFileModel);
    }

    /**
     * 追加消息
     *
     * @param topic
     * @param content
     */
    public void appendMessage(String topic, byte[] content) throws IOException {
        MMapFileModel mMapFileModel = mModelFileModelManager.get(topic);
        if (mMapFileModel != null) {
            CommitLogMessageModel commitLogMessageModel = new CommitLogMessageModel();
            commitLogMessageModel.setContent(content);
            commitLogMessageModel.setSize(content.length);
            mMapFileModel.writeContent(commitLogMessageModel);
        } else {
            throw new RuntimeException("topic " + topic + " inValid");
        }
    }

    public String readMessage(String topic, int startOffset, int size) {
        MMapFileModel mMapFileModel = mModelFileModelManager.get(topic);
        if (mMapFileModel != null) {
            return new String(mMapFileModel.readContent(startOffset, size));
        } else {
            throw new RuntimeException("topic " + topic + " inValid");
        }
    }

    public static void main(String[] args) throws IOException {
        String topic = "order_cancel_topic";
        CommitLogAppendHandler commitLogAppendHandler = new CommitLogAppendHandler();
        commitLogAppendHandler.appendMessage(topic,"MessageAppendHandler".getBytes(StandardCharsets.UTF_8));
        commitLogAppendHandler.appendMessage(topic,"MessageAppendHandler2".getBytes(StandardCharsets.UTF_8));
        System.out.println(commitLogAppendHandler.readMessage(topic, 0, 10));
    }
}
