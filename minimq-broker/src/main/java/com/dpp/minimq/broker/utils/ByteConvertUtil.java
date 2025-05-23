package com.dpp.minimq.broker.utils;

/**
 * @author dpp
 * @date 2025/5/20
 * @Description
 */
public class ByteConvertUtil {

    /**
     * 将 int 类型的值转换为 4 字节的 byte 数组
     * @param value 要转换的 int 值
     * @return 转换后的 4 字节 byte 数组
     */

    public static byte[] intToBytes(int value) {
        byte[] bytes = new byte[4];
        bytes[0] = (byte) (value >> 24);
        bytes[1] = (byte) (value >> 16);
        bytes[2] = (byte) (value >> 8);
        bytes[3] = (byte) value;
        return bytes;
    }

    /**
     * 将 4 字节的 byte 数组转换为 int 类型的值
     * @param bytes 要转换的 4 字节 byte 数组
     * @return 转换后的 int 值
     */
    public static int bytesToInt(byte[] bytes) {
        int value = 0;
        for (int i = 0; i < 4; i++) {
            value |= (bytes[i] & 0xFF) << (24 - i * 8);
        }
        return value;
    }

    public static void main(String[] args) {
        int i = 1000000;
        byte[] bytes = intToBytes(i);
        int i1 = bytesToInt(bytes);
        System.out.println(i1);
    }
}
