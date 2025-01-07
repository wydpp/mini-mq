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
package com.dpp.minimq.store.logfile;


import com.dpp.minimq.common.message.Message;
import com.dpp.minimq.store.*;
import com.dpp.minimq.store.config.FlushDiskType;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

/**
 * 主要用于将磁盘文件映射到内存中。
 * 这种内存映射机制（Memory - Mapped I/O）允许程序像访问内存一样高效地访问磁盘文件。
 * 它是 RocketMQ 实现高性能文件存储和读取的关键技术之一。
 *
 * 通过内存映射，减少了传统文件 I/O 操作中频繁的用户态和内核态切换的开销。
 * 当对MappedFile对应的文件区域进行读写操作时，操作系统会自动将文件内容加载到内存映射区域，使得消息存储和读取更加高效。
 *
 * MappedFile将文件看作是一系列固定大小的存储单元，用于存储消息。
 * 它提供了一种抽象，使得CommitLog等高层组件可以方便地在这些存储单元上进行消息的写入、读取和管理操作。
 */
public interface MappedFile {
    /**
     * Returns the file name of the {@code MappedFile}.
     *
     * @return the file name
     */
    String getFileName();

    /**
     * Returns the file size of the {@code MappedFile}.
     *
     * @return the file size
     */
    int getFileSize();

    /**
     * Returns the {@code FileChannel} behind the {@code MappedFile}.
     *
     * @return the file channel
     */
    FileChannel getFileChannel();

    /**
     * Returns true if this {@code MappedFile} is full and no new messages can be added.
     *
     * @return true if the file is full
     */
    boolean isFull();

    /**
     * Returns true if this {@code MappedFile} is available.
     * <p>
     * The mapped file will be not available if it's shutdown or destroyed.
     *
     * @return true if the file is available
     */
    boolean isAvailable();

    /**
     * Appends a message object to the current {@code MappedFile} with a specific call back.
     *
     * @param message a message to append
     * @param messageCallback the specific call back to execute the real append action
     * @param putMessageContext
     * @return the append result
     */
    AppendMessageResult appendMessage(Message message, AppendMessageCallback messageCallback, PutMessageContext putMessageContext);

    /**
     * Appends a raw message data represents by a byte array to the current {@code MappedFile}.
     *
     * @param data the byte array to append
     * @return true if success; false otherwise.
     */
    boolean appendMessage(byte[] data);

    /**
     * Appends a raw message data represents by a byte array to the current {@code MappedFile}.
     *
     * @param data the byte buffer to append
     * @return true if success; false otherwise.
     */
    boolean appendMessage(ByteBuffer data);

    /**
     * Appends a raw message data represents by a byte array to the current {@code MappedFile},
     * starting at the given offset in the array.
     *
     * @param data the byte array to append
     * @param offset the offset within the array of the first byte to be read
     * @param length the number of bytes to be read from the given array
     * @return true if success; false otherwise.
     */
    boolean appendMessage(byte[] data, int offset, int length);

    /**
     * Returns the global offset of the current {code MappedFile}, it's a long value of the file name.
     *
     * @return the offset of this file
     */
    long getFileFromOffset();

    /**
     * Flushes the data in cache to disk immediately.
     *
     * @param flushLeastPages the least pages to flush
     * @return the flushed position after the method call
     */
    int flush(int flushLeastPages);

    /**
     * Flushes the data in the secondary cache to page cache or disk immediately.
     *
     * @param commitLeastPages the least pages to commit
     * @return the committed position after the method call
     */
    int commit(int commitLeastPages);

    /**
     * Selects a slice of the mapped byte buffer's sub-region behind the mapped file,
     * starting at the given position.
     *
     * @param pos the given position
     * @param size the size of the returned sub-region
     * @return a {@code SelectMappedBufferResult} instance contains the selected slice
     */
    SelectMappedBufferResult selectMappedBuffer(int pos, int size);

    /**
     * Selects a slice of the mapped byte buffer's sub-region behind the mapped file,
     * starting at the given position.
     *
     * @param pos the given position
     * @return a {@code SelectMappedBufferResult} instance contains the selected slice
     */
    SelectMappedBufferResult selectMappedBuffer(int pos);

    /**
     * Returns the mapped byte buffer behind the mapped file.
     *
     * @return the mapped byte buffer
     */
    MappedByteBuffer getMappedByteBuffer();

    /**
     * Returns a slice of the mapped byte buffer behind the mapped file.
     *
     * @return the slice of the mapped byte buffer
     */
    ByteBuffer sliceByteBuffer();

    /**
     * Returns the store timestamp of the last message.
     *
     * @return the store timestamp
     */
    long getStoreTimestamp();

    /**
     * Returns the last modified timestamp of the file.
     *
     * @return the last modified timestamp
     */
    long getLastModifiedTimestamp();

    /**
     * Get data from a certain pos offset with size byte
     *
     * @param pos a certain pos offset to get data
     * @param size the size of data
     * @param byteBuffer the data
     * @return true if with data; false if no data;
     */
    boolean getData(int pos, int size, ByteBuffer byteBuffer);

    /**
     * Destroys the file and delete it from the file system.
     *
     * @param intervalForcibly If {@code true} then this method will destroy the file forcibly and ignore the reference
     * @return true if success; false otherwise.
     */
    boolean destroy(long intervalForcibly);

    /**
     * Shutdowns the file and mark it unavailable.
     *
     * @param intervalForcibly If {@code true} then this method will shutdown the file forcibly and ignore the reference
     */
    void shutdown(long intervalForcibly);

    /**
     * Decreases the reference count by {@code 1} and clean up the mapped file if the reference count reaches at
     * {@code 0}.
     */
    void release();

    /**
     * Increases the reference count by {@code 1}.
     *
     * @return true if success; false otherwise.
     */
    boolean hold();

    /**
     * Returns true if the current file is first mapped file of some consume queue.
     *
     * @return true or false
     */
    boolean isFirstCreateInQueue();

    /**
     * Sets the flag whether the current file is first mapped file of some consume queue.
     *
     * @param firstCreateInQueue true or false
     */
    void setFirstCreateInQueue(boolean firstCreateInQueue);

    /**
     * Returns the flushed position of this mapped file.
     *
     * @return the flushed posotion
     */
    int getFlushedPosition();

    /**
     * Sets the flushed position of this mapped file.
     *
     * @param flushedPosition the specific flushed position
     */
    void setFlushedPosition(int flushedPosition);

    /**
     * Returns the wrote position of this mapped file.
     *
     * @return the wrote position
     */
    int getWrotePosition();

    /**
     * Sets the wrote position of this mapped file.
     *
     * @param wrotePosition the specific wrote position
     */
    void setWrotePosition(int wrotePosition);

    /**
     * Returns the current max readable position of this mapped file.
     *
     * @return the max readable position
     */
    int getReadPosition();

    /**
     * Sets the committed position of this mapped file.
     *
     * @param committedPosition the specific committed position
     */
    void setCommittedPosition(int committedPosition);

    /**
     * Lock the mapped bytebuffer
     */
    void mlock();

    /**
     * Unlock the mapped bytebuffer
     */
    void munlock();

    /**
     * Warm up the mapped bytebuffer
     * @param type
     * @param pages
     */
    void warmMappedFile(FlushDiskType type, int pages);

    /**
     * Swap map
     */
    boolean swapMap();

    /**
     * Clean pageTable
     */
    void cleanSwapedMap(boolean force);

    /**
     * Get recent swap map time
     */
    long getRecentSwapMapTime();

    /**
     * Get recent MappedByteBuffer access count since last swap
     */
    long getMappedByteBufferAccessCountSinceLastSwap();

    /**
     * Get the underlying file
     * @return
     */
    File getFile();

    /**
     * Get the last flush time
     * @return
     */
    long getLastFlushTime();

    /**
     * Init mapped file
     * @param fileName file name
     * @param fileSize file size
     * @param transientStorePool transient store pool
     * @throws IOException
     */
    void init(String fileName, int fileSize, TransientStorePool transientStorePool) throws IOException;
}
