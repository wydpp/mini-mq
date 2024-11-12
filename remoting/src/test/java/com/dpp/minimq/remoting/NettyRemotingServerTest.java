package com.dpp.minimq.remoting;

import com.alibaba.fastjson.JSON;
import com.dpp.minimq.remoting.netty.*;
import com.dpp.minimq.remoting.protocol.RemotingCommand;
import io.netty.channel.ChannelHandlerContext;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

public class NettyRemotingServerTest {

    private static RemotingServer remotingServer;
    private static RemotingClient remotingClient;

    @BeforeClass
    public static void setup() throws InterruptedException {
        //remotingServer = createRemotingServer();
        remotingClient = createRemotingClient();
    }


    public static RemotingServer createRemotingServer() throws InterruptedException {
        NettyServerConfig config = new NettyServerConfig();
        config.setBindAddress("127.0.0.1");
        config.setListenPort(8080);
        RemotingServer remotingServer = new NettyRemotingServer(config);
        remotingServer.registerProcessor(new NettyRequestProcessor() {
            @Override
            public RemotingCommand processRequest(ChannelHandlerContext ctx, RemotingCommand request) throws Exception {
                System.out.println("请求信息= " + JSON.toJSONString(request));
                return request;
            }
        });
        remotingServer.start();
        return remotingServer;
    }

    public static RemotingClient createRemotingClient() {
        NettyClientConfig nettyClientConfig = new NettyClientConfig();
        RemotingClient client = new NettyRemotingClient(nettyClientConfig);
        client.start();
        return client;
    }

    @Test
    public void testInvokeSync() throws Exception {
        TimeUnit.SECONDS.sleep(1);
        RemotingCommand request = RemotingCommand.createRequestCommand(100);
        RemotingCommand response = remotingClient.invokeSync("localhost:8080", request, 1000 * 3);
        System.out.println(JSON.toJSONString(response));
        TimeUnit.MINUTES.sleep(10);
    }
}
