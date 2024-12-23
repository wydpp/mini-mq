package com.dpp.minimq.broker;

import com.dpp.minimq.common.BrokerConfig;
import com.dpp.minimq.remoting.netty.NettyServerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author dpp
 * @date 2024/12/17
 * @Description
 */
public class BrokerStartup {

    private static Logger log = LoggerFactory.getLogger(BrokerStartup.class);

    public static void main(String[] args) {
        start();
    }

    /**
     * 启动broker
     * @return
     */
    public static BrokerController start() {
        BrokerController controller = createBrokerController();
        try {
            //调用start方法启动
            controller.start();
            String tip = "The broker[" + controller.getBrokerConfig().getBrokerName() + "," + controller.getBrokerAddr() + "] boot success.";
            log.info(tip);
            System.out.printf("%s%n", tip);
            return controller;
        } catch (Throwable e) {
            e.printStackTrace();
            System.exit(-1);
        }
        return null;
    }

    /**
     * 创建并初始化 brokerController
     * @return
     */
    public static BrokerController createBrokerController() {
        final BrokerConfig brokerConfig = new BrokerConfig();
        final NettyServerConfig nettyServerConfig = new NettyServerConfig();
        nettyServerConfig.setListenPort(10911);
        final BrokerController controller = new BrokerController(brokerConfig, nettyServerConfig);
        try {
            boolean initResult = controller.initialize();
            if (!initResult) {
                controller.shutdown();
                System.exit(-3);
            }
            return controller;
        } catch (Throwable e) {
            e.printStackTrace();
            System.exit(-1);
        }
        return null;
    }
}
