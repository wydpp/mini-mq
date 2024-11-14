package com.dpp.minimq.broker;

import com.dpp.minimq.common.BrokerConfig;
import com.dpp.minimq.remoting.RemotingServer;
import com.dpp.minimq.remoting.netty.NettyRemotingServer;
import com.dpp.minimq.remoting.netty.NettyServerConfig;

/**
 * @author dpp
 * @date 2024/11/6
 * @Description
 */
public class BrokerController {

    private BrokerConfig brokerConfig;

    private NettyServerConfig nettyServerConfig;

    protected RemotingServer remotingServer;

    public BrokerController(NettyServerConfig nettyServerConfig) {
        this.nettyServerConfig = nettyServerConfig;
    }

    /**
     * 初始化工作
     * @return
     */
    public boolean initialize(){
        //message store 初始化
        //netty server 初始化
        initializeRemotingServer();
        return true;
    }

    protected void initializeRemotingServer() {
        this.remotingServer = new NettyRemotingServer(this.nettyServerConfig);
    }

    public void start(){
        if (remotingServer != null){
            remotingServer.start();
        }
    }

    public void shutdown(){
        if (remotingServer != null){
            remotingServer.shutDown();
        }
    }

    public BrokerConfig getBrokerConfig() {
        return brokerConfig;
    }

    public NettyServerConfig getNettyServerConfig() {
        return nettyServerConfig;
    }

    public String getBrokerAddr() {
        return this.brokerConfig.getBrokerIP1() + ":" + this.nettyServerConfig.getListenPort();
    }
}
