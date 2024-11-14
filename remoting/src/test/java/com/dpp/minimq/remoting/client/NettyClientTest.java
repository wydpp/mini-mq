package com.dpp.minimq.remoting.client;

import com.dpp.minimq.remoting.netty.NettyEncoder;
import com.dpp.minimq.remoting.protocol.RemotingCommand;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;

/**
 * @author dpp
 * @date 2024/11/12
 * @Description
 */
public class NettyClientTest {

    public static void main(String[] args) throws InterruptedException {
        EventLoopGroup workerGroup = new NioEventLoopGroup();

        Bootstrap bootstrap = new Bootstrap();
        bootstrap.group(workerGroup)
                .channel(NioSocketChannel.class)
                .option(ChannelOption.TCP_NODELAY, true)
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) throws Exception {
                        ch.pipeline().addLast(new NettyEncoder());
                    }
                });
        ChannelFuture sync = bootstrap.connect("127.0.0.1", 8080).sync();
        Channel channel = sync.channel();
        channel.writeAndFlush(RemotingCommand.createRequestCommand(100));
        channel.closeFuture().sync();
    }
}
