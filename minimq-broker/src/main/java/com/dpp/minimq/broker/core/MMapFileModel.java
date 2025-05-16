package com.dpp.minimq.broker.core;

import com.dpp.minimq.broker.utils.MMapUtil;
import io.netty.util.internal.PlatformDependent;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Scanner;

/**
 * @author dpp
 * @date 2025/5/16
 * @Description
 */
public class MMapFileModel {
    private File file;
    private MappedByteBuffer mappedByteBuffer;
    private FileChannel fileChannel;

    /**
     * 从指定的offset开始映射文件
     *
     * @param filePath
     * @param startOffset
     * @param size
     */
    public void loadFileInMMap(String filePath, int startOffset, int size) throws IOException {
        this.file = new File(filePath);
        if (!file.exists()) {
            throw new FileNotFoundException("filePath "+filePath + " inValid");
        }
        this.fileChannel = new RandomAccessFile(file, "rw").getChannel();
        this.mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, startOffset, size);
    }

    /**
     *  从文件的指定offset开始读数据
     * @param startOffset
     * @param size
     * @return
     */
    public byte[] readContent(int startOffset, int size) {
        byte[] result = new byte[size];
        mappedByteBuffer.position(startOffset);
        mappedByteBuffer.get(result);
        return result;
    }

    public void writeContent(byte[] content) {
        writeContent(content, false);
    }

    /**
     * 文件写数据
     * @param content
     */
    public void writeContent(byte[] content, boolean force) {
        //默认刷到page cache,如果需要强制刷盘,可以使用mappedByteBuffer.force()
        mappedByteBuffer.put(content);
        if (force) {
            mappedByteBuffer.force();
        }
    }

    /**
     * 释放内存映射
     */
    public void clean(){
        //使用netty的工具类来释放内存映射
        if (mappedByteBuffer != null) {
            PlatformDependent.freeDirectBuffer(mappedByteBuffer);
        }
    }

}
