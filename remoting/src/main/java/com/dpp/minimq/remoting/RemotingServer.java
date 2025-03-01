package com.dpp.minimq.remoting;

import com.dpp.minimq.remoting.netty.NettyRequestProcessor;
import com.dpp.minimq.remoting.protocol.RemotingCommand;
import io.netty.channel.Channel;

import java.util.concurrent.ExecutorService;

public interface RemotingServer extends RemotingService {

    /**
     * 同步执行命令
     *
     * @param channel
     * @param request
     * @param timeoutMillis
     * @return
     */
    RemotingCommand invokeSync(final Channel channel,
                               final RemotingCommand request,
                               final long timeoutMillis);

    /**
     * 添加处理器
     *
     * @param nettyRequestProcessor
     */
    void registerProcessor(int requestCode, NettyRequestProcessor nettyRequestProcessor, ExecutorService executor);
}
