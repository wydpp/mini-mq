package com.dpp.minimq.remoting.netty;

import com.dpp.minimq.remoting.RemotingClient;
import com.dpp.minimq.remoting.common.RemotingHelper;
import com.dpp.minimq.remoting.protocol.RemotingCommand;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.Unpooled;
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
        //TODO
        if (Objects.isNull(addr)) {
            throw new RuntimeException("addr can not be null");
        }
        ChannelFuture channelFuture = this.channelTables.get(addr);
        if (channelFuture == null) {
            channelFuture = createChannelFuture(addr);
        }
        if (channelFuture != null) {
            Channel channel = channelFuture.channel();
            return this.invokeSync(channel, request, timeoutMillis);
        }
        return RemotingCommand.createRequestCommand(500);
    }

    public RemotingCommand invokeSync(Channel channel, RemotingCommand request, long timeoutMillis) {
        if (Objects.isNull(channel)) {
            throw new RuntimeException("channel can not be null");
        }
        RemotingCommand remotingCommand = RemotingCommand.createRequestCommand(500);
        final SocketAddress addr = channel.remoteAddress();
        channel.writeAndFlush(request).addListener((ChannelFutureListener) f -> {
            if (f.isSuccess()) {
                remotingCommand.setCode(200);
                log.info("Success to write a request command to {}", addr);
                return;
            }
            remotingCommand.setMessage(f.cause().getMessage());
            log.info("Failed to write a request command to {}, error {}", addr, f.cause());
        });
        return remotingCommand;
    }

    private ChannelFuture createChannelFuture(String addr) {
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
        this.channelTables.put(addr, channelFuture);
        return channelFuture;
    }

    private Bootstrap createBootstrap() {
        Bootstrap bootstrap = new Bootstrap();
        bootstrap.group(this.eventLoopGroupWorker).channel(NioSocketChannel.class)
                .option(ChannelOption.TCP_NODELAY, true)
                .option(ChannelOption.SO_KEEPALIVE, false)
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, nettyClientConfig.getConnectTimeoutMillis())
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    public void initChannel(SocketChannel ch) {
                        log.info("client initChannel 方法执行!!");
                        ChannelPipeline pipeline = ch.pipeline();
                        pipeline.addLast(
                                defaultEventExecutorGroup,
                                new NettyDecoder(),
                                new NettyEncoder(),
                                new NettyClientHandler());
                        log.info("client initChannel 方法执行结束!!");
                    }
                });
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
        Bootstrap bootstrap = this.bootstrap.group(this.eventLoopGroupWorker).channel(NioSocketChannel.class)
                .option(ChannelOption.TCP_NODELAY, true)
                .option(ChannelOption.SO_KEEPALIVE, false)
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, nettyClientConfig.getConnectTimeoutMillis())
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    public void initChannel(SocketChannel ch) throws Exception {
                        ChannelPipeline pipeline = ch.pipeline();
                        ch.pipeline().addLast(
                                defaultEventExecutorGroup,
                                new NettyEncoder(),
                                new NettyDecoder(),
                                new NettyConnectManageHandler(),
                                new NettyClientHandler());
                    }
                });
    }

    @Override
    public void shutDown() {

    }

    class NettyConnectManageHandler extends ChannelDuplexHandler {
        @Override
        public void connect(ChannelHandlerContext ctx, SocketAddress remoteAddress, SocketAddress localAddress,
                            ChannelPromise promise) throws Exception {
            //final String local = localAddress == null ? "UNKNOWN" : RemotingHelper.parseSocketAddressAddr(localAddress);
            //final String remote = remoteAddress == null ? "UNKNOWN" : RemotingHelper.parseSocketAddressAddr(remoteAddress);
            log.info("NETTY CLIENT PIPELINE: CONNECT  {} => {}", remoteAddress,localAddress);
            closeChannel(ctx.channel());
            //super.connect(ctx, remoteAddress, localAddress, promise);
        }

        @Override
        public void disconnect(ChannelHandlerContext ctx, ChannelPromise promise) throws Exception {
            final String remoteAddress = RemotingHelper.parseChannelRemoteAddr(ctx.channel());
            log.info("NETTY CLIENT PIPELINE: DISCONNECT {}", remoteAddress);
            super.disconnect(ctx, promise);
        }

        @Override
        public void close(ChannelHandlerContext ctx, ChannelPromise promise) throws Exception {
            final String remoteAddress = RemotingHelper.parseChannelRemoteAddr(ctx.channel());
            log.info("NETTY CLIENT PIPELINE: CLOSE {}", remoteAddress);
            closeChannel(ctx.channel());
            super.close(ctx, promise);
        }

        @Override
        public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
            if (evt instanceof IdleStateEvent) {
                IdleStateEvent event = (IdleStateEvent) evt;
                if (event.state().equals(IdleState.ALL_IDLE)) {
                    final String remoteAddress = RemotingHelper.parseChannelRemoteAddr(ctx.channel());
                    log.warn("NETTY CLIENT PIPELINE: IDLE exception [{}]", remoteAddress);
                    closeChannel(ctx.channel());
                }
            }

            ctx.fireUserEventTriggered(evt);
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
            final String remoteAddress = RemotingHelper.parseChannelRemoteAddr(ctx.channel());
            log.warn("NETTY CLIENT PIPELINE: exceptionCaught {}", remoteAddress);
            log.warn("NETTY CLIENT PIPELINE: exceptionCaught exception.", cause);
            closeChannel(ctx.channel());
        }
    }
}
