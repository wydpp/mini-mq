package com.dpp.minimq.remoting;

import com.dpp.minimq.remoting.netty.NettyRemotingServer;
import com.dpp.minimq.remoting.netty.NettyServerConfig;
import com.dpp.minimq.remoting.protocol.RemotingCommand;
import io.netty.channel.ChannelHandlerContext;
import org.junit.Test;

import java.util.concurrent.Executors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertNotNull;

public class NettyRemotingServerTest {

    public static RemotingServer createRemotingServer() throws InterruptedException {
        NettyServerConfig config = new NettyServerConfig();
        RemotingServer remotingServer = new NettyRemotingServer(config);
        remotingServer.start();
        return remotingServer;
    }

    @Test
    public void testInvokeSync(){
        RemotingCommand request = RemotingCommand.createRequestCommand(0);
//        RemotingCommand response = remotingClient.invokeSync("localhost:" + remotingServer.localListenPort(), request, 1000 * 3);
//        assertNotNull(response);
//        assertThat(response.getLanguage()).isEqualTo(LanguageCode.JAVA);
//        assertThat(response.getExtFields()).hasSize(2);

    }
}
