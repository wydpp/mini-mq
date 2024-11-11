/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.dpp.minimq.remoting.netty;

import com.alibaba.fastjson.JSON;
import com.dpp.minimq.remoting.protocol.RemotingCommand;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.List;

public class NettyDecoder extends ByteToMessageDecoder {
    private static final Logger log = LoggerFactory.getLogger(NettyDecoder.class);

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
        if (in.readableBytes() < 1) {
            log.info("没有需要读取的字节");
            return;
        }
        //标记当前的读位置
        in.markReaderIndex();
        int length = in.readInt();
        if (in.readableBytes() < length) { // 确保缓冲区中有足够的数据
            in.resetReaderIndex();// 数据不足，重置到标记位置
            return;
        }
        byte[] contentBytes = new byte[length];
        in.readBytes(contentBytes);
        String content = new String(contentBytes, StandardCharsets.UTF_8);
        RemotingCommand command = JSON.parseObject(content, RemotingCommand.class);
        out.add(command);
        log.info("byte==>command 解码结束");
    }

}
