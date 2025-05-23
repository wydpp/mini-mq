package com.dpp.minimq.broker.model;

import com.dpp.minimq.broker.utils.ByteConvertUtil;

/**
 * @author dpp
 * @date 2025/5/20
 * @Description commitLog文件的写入数据封装
 */
public class CommitLogMessageModel {
    /**
     * 消息的大小，单位是字节
     */
    private int size;
    /**
     * 消息的内容
     */
    private byte[] content;

    public int getSize() {
        return size;
    }

    public void setSize(int size) {
        this.size = size;
    }

    public byte[] getContent() {
        return content;
    }

    public void setContent(byte[] content) {
        this.content = content;
    }

    public byte[] convertToBytes(){
        byte[] sizeByte = ByteConvertUtil.intToBytes(getSize());
        byte[] content = getContent();
        byte[] mergeResultByte = new byte[sizeByte.length + content.length];
        int j = 0;
        for (int i = 0; i < sizeByte.length; i++,j++) {
            mergeResultByte[j] = sizeByte[i];
        }
        for (int i = 0; i < content.length; i++,j++) {
            mergeResultByte[j] = content[i];
        }
        return mergeResultByte;
    }
}
