package com.dpp.minimq.broker.core;

import java.io.File;
import java.io.IOException;

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
        mMapFileModel.loadFileInMMap(topicName, 0, 1 * 1024);
        mModelFileModelManager.put(topicName, mMapFileModel);
    }

    /**
     * 追加消息
     *
     * @param topic
     * @param content
     */
    public void appendMessage(String topic, String content) {
        MMapFileModel mMapFileModel = mModelFileModelManager.get(topic);
        if (mMapFileModel != null) {
            mMapFileModel.writeContent(content.getBytes(),false);
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
        //messageAppendHandler.appendMessage(topic,"MessageAppendHandler");
        //messageAppendHandler.appendMessage(topic,"MessageAppendHandler2");
        System.out.println(commitLogAppendHandler.readMessage(topic, 0, 10));
    }
}
