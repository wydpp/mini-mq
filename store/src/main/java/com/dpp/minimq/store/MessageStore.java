package com.dpp.minimq.store;

import com.dpp.minimq.common.message.Message;
import com.dpp.minimq.store.logfile.MappedFile;

import java.util.concurrent.CompletableFuture;

/**
 * @author dpp
 * @date 2025/1/7
 * @Description 消息存储接口
 */
public interface MessageStore {

    /**
     * Load previously stored messages.
     *
     * @return true if success; false otherwise.
     */
    boolean load();

    /**
     * 启动消息存储服务
     *
     * @throws Exception if there is any error.
     */
    void start() throws Exception;

    /**
     * 终止消息存储服务
     * @throws Exception
     */
    void shutdown() throws Exception;
    /**
     * Destroy this message store. Generally, all persistent files should be removed after invocation.
     * 销毁存储服务，一般情况下，销毁后，所有持久化文件应该被删除
     */
    void destroy();

    /**
     * 异步存储消息
     * @param msg
     * @return
     */
    default CompletableFuture<PutMessageResult> asyncPutMessage(final Message msg) {
        return CompletableFuture.completedFuture(putMessage(msg));
    }

    /**
     * Store a message into store.
     * 存储消息
     * @param msg Message instance to store
     * @return result of store operation.
     */
    PutMessageResult putMessage(final Message msg);

    void onCommitLogAppend(Message msg, AppendMessageResult result, MappedFile commitLogFile);
}
