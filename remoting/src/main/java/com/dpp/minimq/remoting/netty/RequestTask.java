package com.dpp.minimq.remoting.netty;

import com.dpp.minimq.remoting.protocol.RemotingCommand;
import io.netty.channel.Channel;

import java.util.Objects;


/**
 * runnable包装类
 * @author dpp
 * @date 2025/1/7
 * @Description
 */
public class RequestTask implements Runnable{

    private final Runnable runnable;
    private final long createTimestamp = System.currentTimeMillis();
    private final Channel channel;
    private final RemotingCommand request;
    private volatile boolean stopRun = false;

    public RequestTask(Runnable runnable, Channel channel, RemotingCommand request) {
        this.runnable = runnable;
        this.channel = channel;
        this.request = request;
    }

    @Override
    public void run() {
        if (!stopRun){
            this.runnable.run();
        }
    }

    public void returnResponse(int code, String mark){
        final RemotingCommand response = RemotingCommand.createResponseCommand(code, mark);
        response.setId(response.getId());
        this.channel.writeAndFlush(response);
    }

    public long getCreateTimestamp() {
        return createTimestamp;
    }

    public boolean isStopRun() {
        return stopRun;
    }

    public void setStopRun(boolean stopRun) {
        this.stopRun = stopRun;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RequestTask that = (RequestTask) o;
        return createTimestamp == that.createTimestamp && stopRun == that.stopRun && Objects.equals(runnable, that.runnable) && Objects.equals(channel, that.channel) && Objects.equals(request, that.request);
    }

    @Override
    public int hashCode() {
        return Objects.hash(runnable, createTimestamp, channel, request, stopRun);
    }
}
