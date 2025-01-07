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
package com.dpp.minimq.store;


import com.dpp.minimq.store.config.MessageStoreConfig;
import com.dpp.minimq.store.util.LibC;
import com.sun.jna.NativeLong;
import com.sun.jna.Pointer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.nio.ch.DirectBuffer;

import java.nio.ByteBuffer;
import java.util.Deque;
import java.util.concurrent.ConcurrentLinkedDeque;

/**
 * 暂存数据池：TransientStorePool主要用于创建一个暂存数据的缓冲池。
 * 这个缓冲池是为了在消息写入磁盘（更具体地说是写入CommitLog对应的文件）之前，提供一个临时的内存存储区域，用于暂存消息数据，以提高消息写入的性能。
 *
 * 性能优化关键组件：通过使用暂存池，可以利用内存的高速读写特性，批量处理消息写入磁盘的操作。
 * 例如，先将一批消息快速写入暂存池的内存缓冲区，然后再以更合适的时机（如缓冲区满或者达到一定时间间隔等）将数据刷写到磁盘，减少了频繁的磁盘 I/O 操作，从而优化了消息存储的整体性能。
 *
 * 数据缓冲与一致性保障：作为数据缓冲区域，它也在一定程度上保障了数据的一致性。在系统出现异常情况（如突然断电等）时，暂存池中的数据可能会丢失，
 * 但由于有一定的机制（如与磁盘存储的协同工作）来处理这种情况，使得系统在正常运行时能够高效存储消息，同时在异常情况下尽量减少数据丢失的影响。
 *
 */
public class TransientStorePool {
    private static final Logger log = LoggerFactory.getLogger(TransientStorePool.class);

    private final int poolSize;
    private final int fileSize;
    private final Deque<ByteBuffer> availableBuffers;
    private final MessageStoreConfig storeConfig;

    public TransientStorePool(final MessageStoreConfig storeConfig) {
        this.storeConfig = storeConfig;
        this.poolSize = storeConfig.getTransientStorePoolSize();
        this.fileSize = storeConfig.getMappedFileSizeCommitLog();
        this.availableBuffers = new ConcurrentLinkedDeque<>();
    }

    /**
     * It's a heavy init method.
     */
    public void init() {
        for (int i = 0; i < poolSize; i++) {
            ByteBuffer byteBuffer = ByteBuffer.allocateDirect(fileSize);

            final long address = ((DirectBuffer) byteBuffer).address();
            Pointer pointer = new Pointer(address);
            LibC.INSTANCE.mlock(pointer, new NativeLong(fileSize));

            availableBuffers.offer(byteBuffer);
        }
    }

    public void destroy() {
        for (ByteBuffer byteBuffer : availableBuffers) {
            final long address = ((DirectBuffer) byteBuffer).address();
            Pointer pointer = new Pointer(address);
            LibC.INSTANCE.munlock(pointer, new NativeLong(fileSize));
        }
    }

    public void returnBuffer(ByteBuffer byteBuffer) {
        byteBuffer.position(0);
        byteBuffer.limit(fileSize);
        this.availableBuffers.offerFirst(byteBuffer);
    }

    public ByteBuffer borrowBuffer() {
        ByteBuffer buffer = availableBuffers.pollFirst();
        if (availableBuffers.size() < poolSize * 0.4) {
            log.warn("TransientStorePool only remain {} sheets.", availableBuffers.size());
        }
        return buffer;
    }

    public int availableBufferNums() {
        if (storeConfig.isTransientStorePoolEnable()) {
            return availableBuffers.size();
        }
        return Integer.MAX_VALUE;
    }
}
