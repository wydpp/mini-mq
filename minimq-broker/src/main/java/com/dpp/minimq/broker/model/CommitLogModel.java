package com.dpp.minimq.broker.model;

import com.dpp.minimq.broker.constants.BrokerConstants;

import java.util.concurrent.atomic.AtomicLong;

/**
 * @author dpp
 * @date 2025/5/20
 * @Description commitLog文件的写入offset封装
 */
public class CommitLogModel {
    /**
     * 文件名
     */
    private String fileName;
    /**
     * 写入的地址
     */
    private AtomicLong offset;
    /**
     * 文件写入的上限
     */
    private Long offsetLimit = BrokerConstants.COMMITLONG_DEFAULT_MMAP_SIZE;

    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public AtomicLong getOffset() {
        return offset;
    }

    public void addOffset(int size){
        offset.addAndGet(size);
    }

    public void setOffset(AtomicLong offset) {
        this.offset = offset;
    }

    public Long diff(){
        return offsetLimit - offset.get();
    }

    public Long getOffsetLimit() {
        return offsetLimit;
    }

    public void setOffsetLimit(Long offsetLimit) {
        this.offsetLimit = offsetLimit;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("CommitLogModel{");
        sb.append("fileName='").append(fileName).append('\'');
        sb.append(", offset=").append(offset);
        sb.append(", offsetLimit=").append(offsetLimit);
        sb.append('}');
        return sb.toString();
    }

}
