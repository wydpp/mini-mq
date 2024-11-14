package com.dpp.minimq.remoting.protocol;

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 执行命令
 */
public class RemotingCommand implements Serializable {

    private static AtomicInteger requestId = new AtomicInteger(0);

    private int code;

    private String message;

    private int id = requestId.getAndIncrement();

    protected RemotingCommand() {
    }

    public int getId(){
        return id;
    }

    public void setId(int id) {
        this.id = id;
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

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("RemotingCommand{");
        sb.append("code=").append(code);
        sb.append(", message='").append(message).append('\'');
        sb.append('}');
        return sb.toString();
    }
}
