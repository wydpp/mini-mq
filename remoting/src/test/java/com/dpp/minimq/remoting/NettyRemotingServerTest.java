package com.dpp.minimq.remoting;

import com.alibaba.fastjson.JSON;
import com.dpp.minimq.remoting.netty.*;
import com.dpp.minimq.remoting.protocol.RemotingCommand;
import io.netty.channel.ChannelHandlerContext;
import org.junit.Test;

public class NettyRemotingServerTest {

    public static RemotingServer createRemotingServer() throws InterruptedException {
        NettyServerConfig config = new NettyServerConfig();
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

    public static RemotingClient createRemotingClient(NettyClientConfig nettyClientConfig) {
        RemotingClient client = new NettyRemotingClient(nettyClientConfig);
        client.start();
        return client;
    }

    @Test
    public void testInvokeSync() throws Exception {
        RemotingCommand request = RemotingCommand.createRequestCommand(0);
        RemotingServer remotingServer = createRemotingServer();
//        RemotingCommand response = remotingClient.invokeSync("localhost:" + remotingServer.localListenPort(), request, 1000 * 3);
//        assertNotNull(response);
//        assertThat(response.getLanguage()).isEqualTo(LanguageCode.JAVA);
//        assertThat(response.getExtFields()).hasSize(2);

    }
}
