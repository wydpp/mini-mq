package com.dpp.minimq.broker.utils;

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
 * @version 1.0
 * @date 2025/5/15
 * @Description 支持基于Java的 MMap api 访问文件能力<br/>
 * 1. 支持指定的offset的文件映射<br/>
 * 2. 支持文件从指定的offset开始写数据<br/>
 * 3. 支持文件从指定的offset开始读数据<br/>
 * 4. 支持文件映射后的内存释放
 */
public class MMapUtil {
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
    public void clear(){
        //使用netty的工具类来释放内存映射
        if (mappedByteBuffer != null) {
            PlatformDependent.freeDirectBuffer(mappedByteBuffer);
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        MMapUtil mMapUtil = new MMapUtil();
        mMapUtil.loadFileInMMap("C:\\Users\\wydpp\\Documents\\学习资料\\洞见-领域驱动设计文集.pdf", 0, 1024*5);
        System.out.println("映射了5M的空间");
        Scanner scanner = new Scanner(System.in);
        while (true) {
            String s = scanner.nextLine();
            if (s.equals("exit")){
                mMapUtil.clear();
                break;
            }
        }
        Thread.sleep(10000);
        //可以通过 arthas memory命令来查看内存占用情况
    }

}
