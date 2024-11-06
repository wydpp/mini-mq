package com.dpp.minimq.common.message;

import java.nio.ByteBuffer;

/**
 * @author dpp
 * @date 2024/11/6
 * @Description 消息编码和解码
 */
public class MessageDecoder {
    /**
     * 消息编码
     * @param message
     * @return
     */
    public static byte[] encode(Message message){
        ByteBuffer byteBuffer = ByteBuffer.allocate(1024);
        //TODO
        return byteBuffer.array();
    }

    /**
     * 消息解码
     * @param byteBuffer
     * @return
     */
    public static Message decode(ByteBuffer byteBuffer){
        Message message = new Message();
        //todo
        return message;
    }
}
