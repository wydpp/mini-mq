package com.dpp.minimq.store;

/**
 * @author dpp
 * @date 2025/1/7
 * @Description
 */
public class PutMessageResult {
    /**
     * 消息存储结果状态
     */
    private PutMessageStatus putMessageStatus;
    /**
     * 追加消息结果
     */
    private AppendMessageResult appendMessageResult;
    /**
     * 是否远程存储
     */
    private boolean remotePut = false;

    public PutMessageResult(PutMessageStatus putMessageStatus, AppendMessageResult appendMessageResult) {
        this.putMessageStatus = putMessageStatus;
        this.appendMessageResult = appendMessageResult;
    }

    /**
     * 判断是否成功
     * @return
     */
    public boolean isOk(){
        if (remotePut){
            return putMessageStatus == PutMessageStatus.PUT_OK;
        }else {
            return this.appendMessageResult != null && this.appendMessageResult.isOk();
        }
    }


    public PutMessageStatus getPutMessageStatus() {
        return putMessageStatus;
    }

    public void setPutMessageStatus(PutMessageStatus putMessageStatus) {
        this.putMessageStatus = putMessageStatus;
    }

    public AppendMessageResult getAppendMessageResult() {
        return appendMessageResult;
    }

    public void setAppendMessageResult(AppendMessageResult appendMessageResult) {
        this.appendMessageResult = appendMessageResult;
    }

    public boolean isRemotePut() {
        return remotePut;
    }

    public void setRemotePut(boolean remotePut) {
        this.remotePut = remotePut;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("PutMessageResult{");
        sb.append("putMessageStatus=").append(putMessageStatus);
        sb.append(", appendMessageResult=").append(appendMessageResult);
        sb.append(", remotePut=").append(remotePut);
        sb.append('}');
        return sb.toString();
    }
}
