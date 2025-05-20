package com.dpp.minimq.broker.utils;

/**
 * @author dpp
 * @date 2025/5/20
 * @Description
 */
public class CommitLogFileNameUtil {

    public static String buildFirstCommitLogFileName() {
        return "00000000";
    }

    /**
     * 根据老的commitLog文件名，生成新的commitLog文件名
     * 00000000 -> 00000001
     * 00000001 -> 00000002
     * 00000002 -> 00000003
     * @param oldCommitLogFileName
     * @return
     */
    public static String incrementCommitLogFileName(String oldCommitLogFileName) {
        if (oldCommitLogFileName.length() != 8) {
            throw new RuntimeException("oldCommitLogFileName " + oldCommitLogFileName + " inValid");
        }
        long commitLogFileNum = Long.parseLong(oldCommitLogFileName);
        commitLogFileNum++;
        return String.format("%08d", commitLogFileNum);
    }

}
