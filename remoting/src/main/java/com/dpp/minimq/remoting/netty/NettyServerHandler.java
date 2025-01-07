package com.dpp.minimq.remoting.netty;

import com.dpp.minimq.remoting.protocol.RemotingCommand;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;

public class NettyServerHandler extends SimpleChannelInboundHandler<RemotingCommand> {

    private static final Logger log = LoggerFactory.getLogger(NettyServerHandler.class);

    private NettyRemotingServer nettyRemotingServer;

    public NettyServerHandler(NettyRemotingServer nettyRemotingServer){
        this.nettyRemotingServer = nettyRemotingServer;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, RemotingCommand msg) {
        //处理接收到的消息
        log.info("receive msg = {}", msg);
        this.nettyRemotingServer.processMessageReceived(ctx, msg);
    }

}
