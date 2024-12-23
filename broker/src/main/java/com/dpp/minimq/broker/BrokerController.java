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
    /**
     * broker配置
     */
    private final BrokerConfig brokerConfig;
    /**
     * nettyServerClient配置
     */
    private final NettyServerConfig nettyServerConfig;
    /**
     * broker netty 服务端，用来接收和处理客户端发送的消息
     */
    private RemotingServer remotingServer;

    public BrokerController(BrokerConfig brokerConfig, NettyServerConfig nettyServerConfig) {
        this.brokerConfig = brokerConfig;
        this.nettyServerConfig = nettyServerConfig;
    }

    public boolean initialize() throws CloneNotSupportedException {
        initializeRemotingServer();
        return true;
    }

    protected void initializeRemotingServer() throws CloneNotSupportedException {
        this.remotingServer = new NettyRemotingServer(this.nettyServerConfig);
    }

    public void start() throws Exception{
        if (remotingServer != null){
            //启动netty服务端
            remotingServer.start();
        }
    }

    public void shutdown() {
        if (remotingServer != null){
            this.remotingServer.shutDown();
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
