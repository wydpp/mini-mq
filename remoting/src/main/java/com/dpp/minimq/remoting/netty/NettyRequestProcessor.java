package com.dpp.minimq.remoting.netty;

import com.dpp.minimq.remoting.protocol.RemotingCommand;
import io.netty.channel.ChannelHandlerContext;

public interface NettyRequestProcessor {
    /**
     * 请求处理器
     * @param ctx
     * @param request
     * @return
     * @throws Exception
     */
    RemotingCommand processRequest(ChannelHandlerContext ctx, RemotingCommand request)
            throws Exception;
}
