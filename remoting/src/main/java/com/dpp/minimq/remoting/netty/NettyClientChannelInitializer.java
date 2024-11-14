package com.dpp.minimq.remoting.netty;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;

/**
 * @author dpp
 * @date 2024/11/12
 * @Description
 */
public class NettyClientChannelInitializer extends ChannelInitializer<SocketChannel> {
    @Override
    protected void initChannel(SocketChannel ch) throws Exception {
        ChannelPipeline pipeline = ch.pipeline();
        pipeline.addLast(new NettyEncoder());
//                        pipeline.addLast(
//                                defaultEventExecutorGroup,
//                                new NettyEncoder(),
//                                new NettyDecoder(),
//                                new NettyConnectManageHandler(),
//                                new NettyClientHandler());
    }
}
