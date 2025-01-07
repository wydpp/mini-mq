package com.dpp.minimq.store;

/**
 * @author dpp
 * @date 2025/1/7
 * @Description
 */
public enum AppendMessageStatus {
    PUT_OK,
    END_OF_FILE,
    MESSAGE_SIZE_EXCEEDED,
    PROPERTIES_SIZE_EXCEEDED,
    UNKNOWN_ERROR,
}
