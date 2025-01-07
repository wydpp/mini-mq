package com.dpp.minimq.remoting.protocol;

import java.io.Serializable;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 执行命令
 */
public class RemotingCommand implements Serializable {

    private static AtomicInteger requestId = new AtomicInteger(0);

    private int code;

    private String message;

    private String remark;

    private int id = requestId.getAndIncrement();
    /**
     * 包含topic信息等
     */
    private HashMap<String, String> extFields;

    private RemotingCommandType remotingCommandType;

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
        cmd.setRemotingCommandType(RemotingCommandType.REQUEST_COMMAND);
        return cmd;
    }

    public static RemotingCommand createResponseCommand(int code, String remark) {
        RemotingCommand cmd = new RemotingCommand();
        cmd.setCode(code);
        cmd.setRemark(remark);
        cmd.setRemotingCommandType(RemotingCommandType.RESPONSE_COMMAND);
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

    public void addExtField(String key, String value) {
        if (null == extFields) {
            extFields = new HashMap<>();
        }
        extFields.put(key, value);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("RemotingCommand{");
        sb.append("code=").append(code);
        sb.append(", message='").append(message).append('\'');
        sb.append(", remark='").append(remark).append('\'');
        sb.append(", id=").append(id);
        sb.append(", extFields=").append(extFields);
        sb.append('}');
        return sb.toString();
    }

    public String getExtField(String key) {
        if (null != extFields){
            return extFields.get(key);
        }
        return null;
    }

    public String getRemark() {
        return remark;
    }

    public void setRemark(String remark) {
        this.remark = remark;
    }

    public RemotingCommandType getRemotingCommandType() {
        return remotingCommandType;
    }

    public void setRemotingCommandType(RemotingCommandType remotingCommandType) {
        this.remotingCommandType = remotingCommandType;
    }
}
