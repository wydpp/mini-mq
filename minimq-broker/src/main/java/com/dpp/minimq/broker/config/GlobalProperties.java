package com.dpp.minimq.broker.config;

/**
 * @author dpp
 * @date 2025/5/16
 * @Description
 */
public class GlobalProperties {
    /**
     * 消息存储的根目录
     */
    private String miniMqHome;

    public String getMiniMqHome() {
        return miniMqHome;
    }

    public void setMiniMqHome(String miniMqHome) {
        this.miniMqHome = miniMqHome;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("GlobalProperties{");
        sb.append("miniMqHome='").append(miniMqHome).append('\'');
        sb.append('}');
        return sb.toString();
    }
}
