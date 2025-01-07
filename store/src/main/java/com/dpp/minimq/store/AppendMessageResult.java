package com.dpp.minimq.store;

/**
 * @author dpp
 * @date 2025/1/7
 * @Description
 */
public class AppendMessageResult {
    private AppendMessageStatus status;
    //开始写入的位置
    private long wroteOffset;
    //写入的字节数
    private int wroteBytes;
    //消息id
    private String msgId;
    //存储时间
    private long storeTimestamp;
    //逻辑偏移量
    private long logicsOffset;
    //消息数量
    private int msgNum = 1;

    public AppendMessageResult(AppendMessageStatus status) {
        this(status, 0, 0, 0, 0);
    }


    public AppendMessageResult(AppendMessageStatus status, long wroteOffset, int wroteBytes, String msgId,
                               long storeTimestamp, long logicsOffset) {
        this.status = status;
        this.wroteOffset = wroteOffset;
        this.wroteBytes = wroteBytes;
        this.msgId = msgId;
        this.storeTimestamp = storeTimestamp;
        this.logicsOffset = logicsOffset;
    }

    public AppendMessageResult(AppendMessageStatus status, long wroteOffset, int wroteBytes,
                               long storeTimestamp, long logicsOffset) {
        this.status = status;
        this.wroteOffset = wroteOffset;
        this.wroteBytes = wroteBytes;
        this.storeTimestamp = storeTimestamp;
        this.logicsOffset = logicsOffset;
    }

    public AppendMessageResult(AppendMessageStatus status, long wroteOffset, int wroteBytes,
                               long storeTimestamp, long logicsOffset, int msgNum) {
        this.status = status;
        this.wroteOffset = wroteOffset;
        this.wroteBytes = wroteBytes;
        this.storeTimestamp = storeTimestamp;
        this.logicsOffset = logicsOffset;
        this.msgNum = msgNum;
    }

    public boolean isOk(){
        return status == AppendMessageStatus.PUT_OK;
    }

    public AppendMessageStatus getStatus() {
        return status;
    }

    public void setStatus(AppendMessageStatus status) {
        this.status = status;
    }

    public long getWroteOffset() {
        return wroteOffset;
    }

    public void setWroteOffset(long wroteOffset) {
        this.wroteOffset = wroteOffset;
    }

    public int getWroteBytes() {
        return wroteBytes;
    }

    public void setWroteBytes(int wroteBytes) {
        this.wroteBytes = wroteBytes;
    }

    public String getMsgId() {
        return msgId;
    }

    public void setMsgId(String msgId) {
        this.msgId = msgId;
    }

    public long getStoreTimestamp() {
        return storeTimestamp;
    }

    public void setStoreTimestamp(long storeTimestamp) {
        this.storeTimestamp = storeTimestamp;
    }

    public long getLogicsOffset() {
        return logicsOffset;
    }

    public void setLogicsOffset(long logicsOffset) {
        this.logicsOffset = logicsOffset;
    }

    public int getMsgNum() {
        return msgNum;
    }

    public void setMsgNum(int msgNum) {
        this.msgNum = msgNum;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("AppendMessageResult{");
        sb.append("status=").append(status);
        sb.append(", wroteOffset=").append(wroteOffset);
        sb.append(", wroteBytes=").append(wroteBytes);
        sb.append(", msgId='").append(msgId).append('\'');
        sb.append(", storeTimestamp=").append(storeTimestamp);
        sb.append(", logicsOffset=").append(logicsOffset);
        sb.append(", msgNum=").append(msgNum);
        sb.append('}');
        return sb.toString();
    }
}
