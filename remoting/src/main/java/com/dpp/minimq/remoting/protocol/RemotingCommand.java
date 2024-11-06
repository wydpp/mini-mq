package com.dpp.minimq.remoting.protocol;

import com.dpp.minimq.remoting.exception.RemotingCommandException;
import io.netty.buffer.ByteBuf;

/**
 * 执行命令
 */
public class RemotingCommand {

    private int code;

    private byte[] body;

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

    public static RemotingCommand decode(final ByteBuf byteBuffer) throws RemotingCommandException {
        //todo byteBuffer 转换成 command
        return new RemotingCommand();
    }

    public byte[] getBody() {
        return body;
    }
}
