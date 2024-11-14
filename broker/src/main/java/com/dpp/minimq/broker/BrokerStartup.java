package com.dpp.minimq.broker;

import com.dpp.minimq.remoting.netty.NettyServerConfig;

public class BrokerStartup {

    public static void main(String[] args) {
        start(createBrokerController(args));
    }

    public static void start(BrokerController controller) {
        controller.start();
        String tip = "The broker[" + controller.getBrokerConfig().getBrokerName() + ", "
                + controller.getBrokerAddr() + "] boot success.";
        System.out.printf("%s%n", tip);
    }

    public static BrokerController createBrokerController(String[] args) {
        final NettyServerConfig nettyServerConfig = new NettyServerConfig();
        BrokerController brokerController = new BrokerController(nettyServerConfig);
        boolean initResult = brokerController.initialize();
        if (!initResult) {
            brokerController.shutdown();
            System.exit(-3);
        }
        return brokerController;
    }
}
