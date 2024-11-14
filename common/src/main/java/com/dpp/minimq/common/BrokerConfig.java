package com.dpp.minimq.common;

import com.dpp.minimq.remoting.common.RemotingUtil;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;

public class BrokerConfig {

    protected static final Logger LOGGER = LoggerFactory.getLogger(BrokerConfig.class);

    private static String localHostName;

    static {
        try {
            localHostName = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            LOGGER.error("Failed to obtain the host name", e);
        }
    }

    private String brokerName = defaultBrokerName();

    private String brokerIP1 = RemotingUtil.getLocalAddress();


    private String defaultBrokerName() {
        return StringUtils.isEmpty(localHostName) ? "DEFAULT_BROKER" : localHostName;
    }

    public static String getLocalHostName() {
        return localHostName;
    }

    public static void setLocalHostName(String localHostName) {
        BrokerConfig.localHostName = localHostName;
    }

    public String getBrokerName() {
        return brokerName;
    }

    public void setBrokerName(String brokerName) {
        this.brokerName = brokerName;
    }

    public String getBrokerIP1() {
        return brokerIP1;
    }
}
