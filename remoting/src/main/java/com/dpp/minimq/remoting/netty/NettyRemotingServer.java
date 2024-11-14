package com.dpp.minimq.remoting.netty;

import com.dpp.minimq.remoting.RemotingServer;
import com.dpp.minimq.remoting.common.RemotingUtil;
import com.dpp.minimq.remoting.protocol.RemotingCommand;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.string.StringDecoder;
import io.netty.handler.codec.string.StringEncoder;
import io.netty.util.concurrent.DefaultEventExecutorGroup;
import javafx.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

public class NettyRemotingServer implements RemotingServer {

    private static final Logger log = LoggerFactory.getLogger(NettyRemotingServer.class);

    private final ServerBootstrap serverBootstrap;
    private final EventLoopGroup eventLoopGroupSelector;
    private final EventLoopGroup eventLoopGroupBoss;
    private final NettyServerConfig nettyServerConfig;
    //线程池
    private final ExecutorService publicExecutor;

    private DefaultEventExecutorGroup defaultEventExecutorGroup;

    private HandshakeHandler handshakeHandler;
    private NettyDecoder nettyDecoder;
    private NettyEncoder nettyEncoder;

    private NettyServerHandler serverHandler;

    /**
     * 对应code的请求处理器
     */
    protected final HashMap<Integer/* request code */, NettyRequestProcessor> processorTable =
            new HashMap<>(64);

    /**
     * 构建一个服务端
     *
     * @param nettyServerConfig
     */
    public NettyRemotingServer(NettyServerConfig nettyServerConfig) {
        this.serverBootstrap = new ServerBootstrap();
        this.nettyServerConfig = nettyServerConfig;
        this.eventLoopGroupBoss = buildBossEventLoopGroup();
        this.eventLoopGroupSelector = buildEventLoopGroupSelector();
        this.publicExecutor = buildPublicExecutor(nettyServerConfig);
    }

    /**
     * 启动一个netty服务
     */
    @Override
    public void start() {
        this.defaultEventExecutorGroup = new DefaultEventExecutorGroup(
                nettyServerConfig.getServerWorkerThreads(),
                new ThreadFactory() {

                    private final AtomicInteger threadIndex = new AtomicInteger(0);

                    @Override
                    public Thread newThread(Runnable r) {
                        return new Thread(r, "NettyServerCodecThread_" + this.threadIndex.incrementAndGet());
                    }
                });
        handshakeHandler = new HandshakeHandler();
        nettyDecoder = new NettyDecoder();
        nettyEncoder = new NettyEncoder();
        serverHandler = new NettyServerHandler(this.processorTable);
        //netty server 初始化
        serverBootstrap.group(this.eventLoopGroupBoss, this.eventLoopGroupSelector)
                .channel(useEpoll() ? EpollServerSocketChannel.class : NioServerSocketChannel.class)
                .option(ChannelOption.SO_BACKLOG, 1024)
                .option(ChannelOption.SO_REUSEADDR, true)
                .childOption(ChannelOption.SO_KEEPALIVE, false)
                .childOption(ChannelOption.TCP_NODELAY, true)
                .localAddress(new InetSocketAddress(this.nettyServerConfig.getBindAddress(),
                        this.nettyServerConfig.getListenPort()))
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) throws Exception {
                        ch.pipeline()
                                .addLast(defaultEventExecutorGroup, "handshakeHandler", handshakeHandler)
                                .addLast(defaultEventExecutorGroup,
                                        nettyEncoder,
                                        nettyDecoder,
                                        serverHandler
                                )
                        ;
                    }
                });

        try {
            ChannelFuture sync = serverBootstrap.bind().sync();
            InetSocketAddress addr = (InetSocketAddress) sync.channel().localAddress();
            if (0 == nettyServerConfig.getListenPort()) {
                this.nettyServerConfig.setListenPort(addr.getPort());
            }
            log.info("RemotingServer started, listening {}:{}", this.nettyServerConfig.getBindAddress(),
                    this.nettyServerConfig.getListenPort());
        } catch (Exception e) {
            throw new IllegalStateException(String.format("Failed to bind to %s:%d", nettyServerConfig.getBindAddress(),
                    nettyServerConfig.getListenPort()), e);
        }
    }

    @Override
    public void shutDown() {

    }

    @Override
    public RemotingCommand invokeSync(Channel channel, RemotingCommand request, long timeoutMillis) {
        return null;
    }

    private EventLoopGroup buildBossEventLoopGroup() {
        if (useEpoll()) {
            return new EpollEventLoopGroup(1, new ThreadFactory() {
                private final AtomicInteger threadIndex = new AtomicInteger(0);

                @Override
                public Thread newThread(Runnable r) {
                    return new Thread(r, String.format("NettyEPOLLBoss_%d", this.threadIndex.incrementAndGet()));
                }
            });
        } else {
            return new NioEventLoopGroup(1, new ThreadFactory() {
                private final AtomicInteger threadIndex = new AtomicInteger(0);

                @Override
                public Thread newThread(Runnable r) {
                    return new Thread(r, String.format("NettyNIOBoss_%d", this.threadIndex.incrementAndGet()));
                }
            });
        }
    }

    private EventLoopGroup buildEventLoopGroupSelector() {
        if (useEpoll()) {
            return new EpollEventLoopGroup(nettyServerConfig.getServerSelectorThreads(), new ThreadFactory() {
                private final AtomicInteger threadIndex = new AtomicInteger(0);
                private final int threadTotal = nettyServerConfig.getServerSelectorThreads();

                @Override
                public Thread newThread(Runnable r) {
                    return new Thread(r, String.format("NettyServerEPOLLSelector_%d_%d", threadTotal, this.threadIndex.incrementAndGet()));
                }
            });
        } else {
            return new NioEventLoopGroup(nettyServerConfig.getServerSelectorThreads(), new ThreadFactory() {
                private final AtomicInteger threadIndex = new AtomicInteger(0);
                private final int threadTotal = nettyServerConfig.getServerSelectorThreads();

                @Override
                public Thread newThread(Runnable r) {
                    return new Thread(r, String.format("NettyServerNIOSelector_%d_%d", threadTotal, this.threadIndex.incrementAndGet()));
                }
            });
        }
    }

    private ExecutorService buildPublicExecutor(NettyServerConfig nettyServerConfig) {
        int publicThreadNums = nettyServerConfig.getServerCallbackExecutorThreads();
        if (publicThreadNums <= 0) {
            publicThreadNums = 4;
        }

        return Executors.newFixedThreadPool(publicThreadNums, new ThreadFactory() {
            private final AtomicInteger threadIndex = new AtomicInteger(0);

            @Override
            public Thread newThread(Runnable r) {
                return new Thread(r, "NettyServerPublicExecutor_" + this.threadIndex.incrementAndGet());
            }
        });
    }

    /**
     * 是否使用epoll
     *
     * @return
     */
    private boolean useEpoll() {
        return RemotingUtil.isLinuxPlatform()
                && nettyServerConfig.isUseEpollNativeSelector()
                && Epoll.isAvailable();
    }

    public void registerProcessor(int requestCode, NettyRequestProcessor nettyRequestProcessor){
        this.processorTable.put(requestCode,nettyRequestProcessor);
    }
}
