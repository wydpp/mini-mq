package com.dpp.minimq.remoting.protocol;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * @author dpp
 * @date 2024/11/12
 * @Description
 */
public class ResponseFuture {

    private volatile RemotingCommand responseCommand;

    private final CountDownLatch countDownLatch = new CountDownLatch(1);

    private volatile boolean sendRequestOK = true;

    private volatile Throwable cause;

    public ResponseFuture() {
    }

    /**
     * 等待返回结果
     *
     * @param timeoutMillis
     * @return
     * @throws InterruptedException
     */
    public RemotingCommand waitResponse(final long timeoutMillis) throws InterruptedException {
        this.countDownLatch.await(timeoutMillis, TimeUnit.MILLISECONDS);
        return responseCommand;
    }

    public void putResponse(final RemotingCommand remotingCommand) {
        this.responseCommand = remotingCommand;
        this.countDownLatch.countDown();
    }

    public RemotingCommand getResponseCommand() {
        return responseCommand;
    }

    public void setResponseCommand(RemotingCommand responseCommand) {
        this.responseCommand = responseCommand;
    }

    public CountDownLatch getCountDownLatch() {
        return countDownLatch;
    }

    public boolean isSendRequestOK() {
        return sendRequestOK;
    }

    public void setSendRequestOK(boolean sendRequestOK) {
        this.sendRequestOK = sendRequestOK;
    }

    public Throwable getCause() {
        return cause;
    }

    public void setCause(Throwable cause) {
        this.cause = cause;
    }
}
