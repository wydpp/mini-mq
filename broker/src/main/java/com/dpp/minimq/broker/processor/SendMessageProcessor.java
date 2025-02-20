package com.dpp.minimq.broker.processor;

import com.alibaba.fastjson.JSON;
import com.dpp.minimq.broker.BrokerController;
import com.dpp.minimq.broker.mqtrace.SendMessageContext;
import com.dpp.minimq.common.TopicConfig;
import com.dpp.minimq.common.message.Message;
import com.dpp.minimq.common.message.MessageType;
import com.dpp.minimq.common.protocol.ResponseCode;
import com.dpp.minimq.common.protocol.SendMessageRequestHeader;
import com.dpp.minimq.common.statictopic.TopicQueueMappingContext;
import com.dpp.minimq.remoting.common.RemotingHelper;
import com.dpp.minimq.remoting.protocol.RemotingCommand;
import com.dpp.minimq.store.PutMessageResult;
import io.netty.channel.ChannelHandlerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadLocalRandom;

/**
 * @author dpp
 * @date 2024/12/30
 * @Description
 */
public class SendMessageProcessor extends AbstractSendMessageProcessor {

    private static final Logger logger = LoggerFactory.getLogger(SendMessageProcessor.class);

    protected final BrokerController brokerController;

    public SendMessageProcessor(final BrokerController brokerController) {
        this.brokerController = brokerController;
    }

    /**
     * 处理生产者发送过来的消息
     *
     * @param ctx
     * @param request
     * @return
     * @throws Exception
     */
    @Override
    public RemotingCommand processRequest(ChannelHandlerContext ctx, RemotingCommand request) throws Exception {
        System.out.println("SendMessageProcessor: Hi " + ctx.channel().remoteAddress());
        SendMessageRequestHeader requestHeader = SendMessageRequestHeader.parseRequestHeader(request);
        SendMessageContext traceContent = null;
        if (requestHeader == null) {
            logger.info("requestHeader is null!");
            return null;
        }
        //找到topic和对应的队列和所在的broker
        TopicQueueMappingContext mappingContext = this.brokerController.getTopicQueueMappingManager().buildTopicQueueMappingContext(requestHeader, true);
        traceContent = buildMsgContext(ctx, requestHeader);
        //存储消息和回复消息（异常会返回）
        RemotingCommand response = this.sendMessage(ctx, request, traceContent, requestHeader, mappingContext);
        return response;
    }

    /**
     * 消息存储和回复
     *
     * @param ctx
     * @param request        消息载体
     * @param traceContent
     * @param requestHeader
     * @param mappingContext
     * @return
     */
    private RemotingCommand sendMessage(final ChannelHandlerContext ctx,
                                        final RemotingCommand request,
                                        final SendMessageContext traceContent,
                                        final SendMessageRequestHeader requestHeader,
                                        final TopicQueueMappingContext mappingContext) {
        final RemotingCommand response = RemotingCommand.createRequestCommand(-1);
        //发送的消息体
        final byte[] body = request.getMessage().getBytes(StandardCharsets.UTF_8);
        //队列id
        Integer queueIdInt = requestHeader.getQueueId();
        //获取topic配置
        TopicConfig topicConfig = this.brokerController.getTopicConfigManager().selectTopicConfig(requestHeader.getTopic());
        if (queueIdInt == null || queueIdInt < 0) {
            queueIdInt = randomQueueId(topicConfig.getWriteQueueNums());
        }
        Message message = new Message();
        message.setTopic(requestHeader.getTopic());
        message.setQueueId(queueIdInt);
        message.setBody(body);
        message.setPropertiesString(JSON.toJSONString(message.getProperties()));
        if (brokerController.getBrokerConfig().isAsyncSendEnable()) {
            //异步存储消息
            CompletableFuture<PutMessageResult> asyncPutMessageFuture = this.brokerController.getMessageStore().asyncPutMessage(message);
            //消息存储结果处理
            final int finalQueueIdInt = queueIdInt;
            final Message finalMessage = message;
            asyncPutMessageFuture.thenAcceptAsync(putMessageResult -> {
                RemotingCommand responseFuture =
                        handlePutMessageResult(putMessageResult, response, request, finalMessage, ctx, finalQueueIdInt, traceContent, mappingContext);
                if (responseFuture != null) {
                    doResponse(ctx, request, responseFuture);
                }
            });
            // Returns null to release the send message thread
            return null;
        } else {
            //同步存储消息
            PutMessageResult putMessageResult = this.brokerController.getMessageStore().putMessage(message);
            handlePutMessageResult(putMessageResult, response, request, message, ctx, queueIdInt, traceContent, mappingContext);
            return response;
        }
    }

    /**
     * 处理发送结果
     *
     * @return
     */
    private RemotingCommand handlePutMessageResult(PutMessageResult putMessageResult, RemotingCommand response, RemotingCommand request,
                                                   Message finalMessage, ChannelHandlerContext ctx, int finalQueueIdInt,
                                                   SendMessageContext traceContent, TopicQueueMappingContext mappingContext) {
        if (putMessageResult == null) {
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark("store putMessage return null");
            return response;
        }
        boolean sendOK = false;
        switch (putMessageResult.getPutMessageStatus()) {
            case PUT_OK:
                sendOK = true;
                response.setCode(ResponseCode.SUCCESS);
                break;
            case UNKNOWN_ERROR:
                response.setCode(ResponseCode.SYSTEM_ERROR);
                response.setRemark("UNKNOWN_ERROR");
                break;
            default:
                response.setCode(ResponseCode.SYSTEM_ERROR);
                response.setRemark("UNKNOWN_ERROR DEFAULT");
                break;
        }
        if (sendOK) {
            //发送成功后处理逻辑
            response.setRemark(null);
            doResponse(ctx, request, response);
            //返回null
            return null;
        } else {
            //发送失败处理逻辑--无
        }
        return response;
    }

    /**
     * 执行发送响应
     *
     * @param ctx
     * @param request
     * @param response
     */
    private void doResponse(ChannelHandlerContext ctx, RemotingCommand request, RemotingCommand response) {
        try {
            ctx.writeAndFlush(response);
        } catch (Throwable e) {
            logger.error("SendMessageProcessor finished processing the request, but failed to send response, client" +
                            "address={}, request={}, response={}",
                    RemotingHelper.parseChannelRemoteAddr(ctx.channel()),
                    request.toString(), response.toString(), e);
        }
    }

    protected int randomQueueId(int writeQueueNums) {
        return ThreadLocalRandom.current().nextInt(99999999) % writeQueueNums;
    }

    protected SendMessageContext buildMsgContext(ChannelHandlerContext ctx,
                                                 SendMessageRequestHeader requestHeader) {
        //String namespace = NamespaceUtil.getNamespaceFromResource(requestHeader.getTopic());
        String namespace = requestHeader.getTopic();
        SendMessageContext traceContext = new SendMessageContext();
        traceContext.setNamespace(namespace);
        traceContext.setProducerGroup(requestHeader.getProducerGroup());
        traceContext.setTopic(requestHeader.getTopic());
        traceContext.setBornHost(RemotingHelper.parseChannelRemoteAddr(ctx.channel()));
        traceContext.setBrokerAddr(this.brokerController.getBrokerAddr());
        traceContext.setRequestTimeStamp(System.currentTimeMillis());
        //暂时统一为普通消息
        traceContext.setMsgType(MessageType.Normal_Msg);
        return traceContext;
    }
}
