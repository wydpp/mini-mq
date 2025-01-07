package com.dpp.minimq.remoting.netty;

import com.dpp.minimq.remoting.common.RemotingHelper;
import com.dpp.minimq.remoting.protocol.RemotingCommand;
import com.dpp.minimq.remoting.protocol.RemotingCommandType;
import com.dpp.minimq.remoting.protocol.RemotingSysResponseCode;
import com.dpp.minimq.remoting.protocol.ResponseFuture;
import io.netty.channel.ChannelHandlerContext;
import javafx.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;

/**
 * @author dpp
 * @date 2025/1/7
 * @Description
 */
public abstract class NettyRemotingAbstract {

    private static final Logger log = LoggerFactory.getLogger(NettyRemotingAbstract.class);

    /**
     * This map caches all on-going requests.
     */
    protected final ConcurrentMap<Integer /* opaque */, ResponseFuture> responseTable =
            new ConcurrentHashMap<>(256);

    /**
     * 对应code的请求处理器
     */
    protected final HashMap<Integer/* request code */, Pair<NettyRequestProcessor, ExecutorService>> processorTable =
            new HashMap<>(64);

    /**
     * 处理请求
     *
     * @param ctx Channel handler context.
     * @param msg incoming remoting command.
     */
    public void processMessageReceived(ChannelHandlerContext ctx, RemotingCommand msg) {
        if (msg != null) {
            switch (msg.getRemotingCommandType()) {
                case REQUEST_COMMAND:
                    processRequestCommand(ctx, msg);
                    break;
                case RESPONSE_COMMAND:
                    processResponseCommand(ctx, msg);
                    break;
                default:
                    break;
            }
        }
    }

    private void processRequestCommand(ChannelHandlerContext ctx, RemotingCommand cmd) {
        final Pair<NettyRequestProcessor, ExecutorService> pair = this.processorTable.get(cmd.getCode());
        if (pair == null) {
            String error = " request type " + cmd.getCode() + " not supported";
            final RemotingCommand response = RemotingCommand.createResponseCommand(RemotingSysResponseCode.REQUEST_CODE_NOT_SUPPORTED, error);
            response.setId(cmd.getId());
            //响应消息
            ctx.writeAndFlush(response);
            log.error(RemotingHelper.parseChannelRemoteAddr(ctx.channel()) + error);
            return;
        }
        Runnable run = buildProcessRequestHandler(ctx, cmd, pair);
        try {
            final RequestTask requestTask = new RequestTask(run, ctx.channel(), cmd);
            //使用线程池执行任务
            pair.getValue().submit(requestTask);
        } catch (RejectedExecutionException e) {
            //线程池拒绝处理任务了
            log.warn(RemotingHelper.parseChannelRemoteAddr(ctx.channel())
                    + ", too many requests and system thread pool busy, RejectedExecutionException "
                    + pair.getValue().toString()
                    + " request code: " + cmd.getCode());
            final RemotingCommand response = RemotingCommand.createResponseCommand(RemotingSysResponseCode.SYSTEM_BUSY, "[OVERLOAD]system busy, start flow control for a while");
            response.setId(cmd.getId());
            ctx.writeAndFlush(response);
        }

    }

    private void processResponseCommand(ChannelHandlerContext ctx, RemotingCommand cmd) {

    }

    /**
     * 构建处理请求的Runnable
     *
     * @param ctx
     * @param pair
     * @return
     */
    private Runnable buildProcessRequestHandler(ChannelHandlerContext ctx, RemotingCommand cmd, Pair<NettyRequestProcessor, ExecutorService> pair) {
        return () -> {
            RemotingCommand response;
            try {
                //处理请求
                response = pair.getKey().processRequest(ctx, cmd);
                //不是null则进行响应处理
                if (response != null) {
                    response.setId(cmd.getId());
                    response.setRemotingCommandType(RemotingCommandType.RESPONSE_COMMAND);
                    try {
                        //响应处理结果
                        ctx.writeAndFlush(response);
                    } catch (Throwable e) {
                        log.error("process request over, bug response failed", e);
                        log.error(cmd.toString());
                        log.error(response.toString());
                    }
                }
            } catch (Throwable e) {
                log.error("process request exception", e);
                log.error(cmd.toString());
                response = RemotingCommand.createResponseCommand(RemotingSysResponseCode.SYSTEM_ERROR,
                        e.getMessage());
                response.setId(cmd.getId());
                //响应异常结果
                ctx.writeAndFlush(response);
            }
        };
    }


}
