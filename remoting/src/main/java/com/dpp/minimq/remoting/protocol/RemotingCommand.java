package com.dpp.minimq.remoting.protocol;

import java.io.Serializable;

/**
 * 执行命令
 */
public class RemotingCommand implements Serializable {

    private int code;

    private String message;

    protected RemotingCommand() {
    }

    public static RemotingCommand createRequestCommand(int code) {
        RemotingCommand cmd = new RemotingCommand();
        cmd.setCode(code);
        return cmd;
    }

    public int getCode() {
        return code;
    }

    public void setCode(int code) {
        this.code = code;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }
}
