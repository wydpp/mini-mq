package com.dpp.minimq.remoting.netty;

import com.dpp.minimq.remoting.protocol.RemotingCommand;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;

public class NettyServerHandler extends SimpleChannelInboundHandler<RemotingCommand> {

    private static final Logger log = LoggerFactory.getLogger(NettyServerHandler.class);

    private HashMap<Integer/* request code */, NettyRequestProcessor> processorTable;

    public NettyServerHandler(HashMap<Integer, NettyRequestProcessor> processorTable){
        this.processorTable = processorTable;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, RemotingCommand msg) {
        //处理接收到的消息
        log.info("receive msg = {}", msg);
        NettyRequestProcessor requestProcessor = this.processorTable.get(msg.getCode());
        RemotingCommand response = null;
        if (requestProcessor != null){
            try {
                response = requestProcessor.processRequest(ctx,msg);
                response.setId(msg.getId());
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }else {
            msg.setMessage("this is server default response");
            response = msg;
        }
        ctx.channel().writeAndFlush(response);
        ctx.close();
    }

}
