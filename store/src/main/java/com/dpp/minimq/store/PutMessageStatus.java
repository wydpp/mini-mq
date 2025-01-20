package com.dpp.minimq.store;

/**
 * @author dpp
 * @date 2025/1/7
 * @Description
 */
public enum PutMessageStatus {
    PUT_OK,
    UNKNOWN_ERROR,
    MESSAGE_ILLEGAL,
    CREATE_MAPPED_FILE_FAILED,
    FLUSH_DISK_TIMEOUT
}
