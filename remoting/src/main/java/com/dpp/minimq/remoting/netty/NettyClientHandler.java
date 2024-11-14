package com.dpp.minimq.remoting.netty;

import com.dpp.minimq.remoting.protocol.RemotingCommand;
import com.dpp.minimq.remoting.protocol.ResponseFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

import java.util.concurrent.ConcurrentMap;

public class NettyClientHandler extends SimpleChannelInboundHandler<RemotingCommand> {

    protected ConcurrentMap<Integer /* requestCode */, ResponseFuture> responseTable;

    public NettyClientHandler(ConcurrentMap<Integer /* requestCode */, ResponseFuture> responseTable) {
        this.responseTable = responseTable;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, RemotingCommand msg) throws Exception {
        if (msg != null) {
            System.out.println("NettyClientHandler: Hi " + ctx.channel().remoteAddress());
            ResponseFuture responseFuture = responseTable.get(msg.getId());
            if (responseFuture != null) {
                responseFuture.putResponse(msg);
            }
        }
    }
}
