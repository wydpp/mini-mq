package com.dpp.minimq.remoting.netty;

import com.alibaba.fastjson.JSON;
import com.dpp.minimq.remoting.RemotingClient;
import com.dpp.minimq.remoting.common.RemotingHelper;
import com.dpp.minimq.remoting.protocol.RemotingCommand;
import com.dpp.minimq.remoting.protocol.ResponseFuture;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.util.concurrent.DefaultEventExecutorGroup;
import io.netty.util.concurrent.EventExecutorGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

import static com.dpp.minimq.remoting.common.RemotingUtil.closeChannel;

public class NettyRemotingClient implements RemotingClient {

    private static final Logger log = LoggerFactory.getLogger(NettyRemotingClient.class);
    private NettyClientConfig nettyClientConfig;

    private final Bootstrap bootstrap = new Bootstrap();
    private final EventLoopGroup eventLoopGroupWorker;

    private EventExecutorGroup defaultEventExecutorGroup;

    //一个客户端需要和多个namesrv保持链接
    private final List<String> namesrvAddrList = new ArrayList<>();
    private final ConcurrentHashMap<String /* cidr */, Bootstrap> bootstrapMap = new ConcurrentHashMap<>();
    private final ConcurrentMap<String /* addr */, ChannelFuture> channelTables = new ConcurrentHashMap<>();

    public NettyRemotingClient(final NettyClientConfig nettyClientConfig) {
        this(nettyClientConfig, null, null);
    }

    public NettyRemotingClient(final NettyClientConfig nettyClientConfig,
                               final EventLoopGroup eventLoopGroup,
                               final EventExecutorGroup eventExecutorGroup) {
        this.nettyClientConfig = nettyClientConfig;
        if (eventLoopGroup != null) {
            this.eventLoopGroupWorker = eventLoopGroup;
        } else {
            this.eventLoopGroupWorker = new NioEventLoopGroup(1, new ThreadFactory() {
                private final AtomicInteger threadIndex = new AtomicInteger(0);

                @Override
                public Thread newThread(Runnable r) {
                    return new Thread(r, String.format("NettyClientSelector_%d", this.threadIndex.incrementAndGet()));
                }
            });
        }
        this.defaultEventExecutorGroup = eventExecutorGroup;
    }


    @Override
    public RemotingCommand invokeSync(String addr, RemotingCommand request, long timeoutMillis) {
        if (Objects.isNull(addr)) {
            throw new RuntimeException("addr can not be null");
        }
        Channel channel = getChannel(addr);
        if (channel != null && channel.isActive()) {
            return this.invokeSync(channel, request, timeoutMillis);
        } else {
            log.warn("createChannel: connect remote host[" + addr + "] failed, " + channel);
        }
        return RemotingCommand.createRequestCommand(500);
    }

    public RemotingCommand invokeSync(Channel channel, RemotingCommand request, long timeoutMillis) {
        if (Objects.isNull(channel)) {
            throw new RuntimeException("channel can not be null");
        }
        ResponseFuture responseFuture = new ResponseFuture();
        final SocketAddress addr = channel.remoteAddress();
        channel.writeAndFlush(request).addListener((ChannelFutureListener) f -> {
            RemotingCommand remotingCommand = RemotingCommand.createRequestCommand(500);
            if (f.isSuccess()) {
                remotingCommand.setCode(200);
                log.info("Success to write a request command to {}", addr);
                responseFuture.putResponse(remotingCommand);
                return;
            }
            remotingCommand.setMessage(f.cause().getMessage());
            responseFuture.putResponse(remotingCommand);
            log.info("Failed to write a request command to {}, error {}", addr, f.cause());
        });
        try {
            RemotingCommand remotingCommand = responseFuture.waitResponse(1000L);
            log.info("invokeSync return remotingCommand : {}", JSON.toJSONString(remotingCommand));
            return remotingCommand;
        } catch (Exception e) {
            log.error("", e);
        }
        return RemotingCommand.createRequestCommand(500);
    }

    private Channel getChannel(String addr) {
        ChannelFuture channelFuture = this.channelTables.get(addr);
        if (channelFuture == null) {
            try {
                channelFuture = createChannelFuture(addr);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        if (channelFuture != null) {
            //这段代码必须要添加，确保channel初始化完成
            Channel channel = channelFuture.channel();
            if (channel != null && channel.isActive()) {
                return channel;
            } else {
                log.warn("createChannel: connect remote host[" + addr + "] failed, " + channelFuture.toString());
            }
        }
        return null;
    }

    private ChannelFuture createChannelFuture(String addr) throws InterruptedException {
        if (!namesrvAddrList.contains(addr)) {
            namesrvAddrList.add(addr);
        }
        String[] hostAndPort = addr.split(":");
        Bootstrap bootstrap = this.bootstrapMap.get(addr);
        if (bootstrap == null) {
            bootstrap = createBootstrap();
            this.bootstrapMap.put(addr, bootstrap);
        }
        ChannelFuture channelFuture = bootstrap.connect(hostAndPort[0], Integer.parseInt(hostAndPort[1]));
        log.info("createChannel: begin to connect remote host[{}]", addr);
        //awaitUninterruptibly方法确保channel是初始化完成的
        if (channelFuture.awaitUninterruptibly(this.nettyClientConfig.getConnectTimeoutMillis())) {
            log.info("createChannel: connect remote host[{}] success, {}", addr, channelFuture.toString());
            this.channelTables.put(addr, channelFuture);
            return channelFuture;
        } else {
            log.warn("createChannel: connect remote host[{}] timeout {}ms, {}", addr, this.nettyClientConfig.getConnectTimeoutMillis(),
                    channelFuture.toString());
        }
        return null;
    }

    private Bootstrap createBootstrap() {
        Bootstrap bootstrap = new Bootstrap();
        bootstrap.group(this.eventLoopGroupWorker).channel(NioSocketChannel.class)
                .option(ChannelOption.TCP_NODELAY, true)
                .option(ChannelOption.SO_KEEPALIVE, false)
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, nettyClientConfig.getConnectTimeoutMillis())
                .handler(new ChannelInitializer<SocketChannel>() {
                             @Override
                             protected void initChannel(SocketChannel ch) throws Exception {
                                 ch.pipeline().addLast(
                                         new NettyEncoder(),
                                         new NettyDecoder(),
                                         new NettyClientHandler());
                             }
                         }
                );
        return bootstrap;
    }

    @Override
    public void registerProcessor(NettyRequestProcessor nettyRequestProcessor) {

    }

    @Override
    public void start() {
        if (this.defaultEventExecutorGroup == null) {
            this.defaultEventExecutorGroup = new DefaultEventExecutorGroup(
                    nettyClientConfig.getClientWorkerThreads(),
                    new ThreadFactory() {

                        private AtomicInteger threadIndex = new AtomicInteger(0);

                        @Override
                        public Thread newThread(Runnable r) {
                            return new Thread(r, "NettyClientWorkerThread_" + this.threadIndex.incrementAndGet());
                        }
                    });
        }
    }

    @Override
    public void shutDown() {
        defaultEventExecutorGroup.shutdownGracefully();
    }

}
