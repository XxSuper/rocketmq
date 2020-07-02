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
package org.apache.rocketmq.store;

import com.sun.jna.NativeLong;
import com.sun.jna.Pointer;
import java.nio.ByteBuffer;
import java.util.Deque;
import java.util.concurrent.ConcurrentLinkedDeque;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.apache.rocketmq.store.util.LibC;
import sun.nio.ch.DirectBuffer;

/**
 * 采用双端队列 Deque 维护了一些列的预分配的 ByteBuffer，这些 ByteBuffer 都是在堆外分配的直接内存，DefaultMessageStore 会持有 TransientStorePool 对象实例，
 * 如果启动时配置了启用 transientStorePoolEnable，那么在 DefaultMessageStore 构造函数中会调用 TransientStorePool.init() 方法，预分配 ByteBuffer 并放入队列中，
 * 如果启动时没有启用 TransientStorePool 功能，则不会调用 TransientStorePool.init 方法，那么从队列中获取 ByteBuffer 会返回 null
 */
public class TransientStorePool {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    // 池中预分配的 ByteBuffer 数量
    private final int poolSize;
    // 每个 ByteBuffer 大小
    private final int fileSize;
    // 采用双端队列维护预分配的 ByteBuffer
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
     * 因为这里需要申请多个堆外 ByteBuffer ，所以是个十分重的初始化方法
     */
    public void init() {
        // 申请 poolSize 个 ByteBuffer，容量为 fileSize
        for (int i = 0; i < poolSize; i++) {
           // 申请直接内存空间
            ByteBuffer byteBuffer = ByteBuffer.allocateDirect(fileSize);
            final long address = ((DirectBuffer) byteBuffer).address();
            Pointer pointer = new Pointer(address);
            // 锁住内存，避免操作系统虚拟内存的换入换出
            LibC.INSTANCE.mlock(pointer, new NativeLong(fileSize));
            // 将预分配的 ByteBuffer 放入队列中
            availableBuffers.offer(byteBuffer);
        }
    }

    // 释放内存池
    public void destroy() {
        for (ByteBuffer byteBuffer : availableBuffers) {
            // 取消对内存的锁定
            final long address = ((DirectBuffer) byteBuffer).address();
            Pointer pointer = new Pointer(address);
            LibC.INSTANCE.munlock(pointer, new NativeLong(fileSize));
        }
    }

    // 使用完毕之后归还 ByteBuffer
    public void returnBuffer(ByteBuffer byteBuffer) {
        // ByteBuffer 各下标复位
        byteBuffer.position(0);
        byteBuffer.limit(fileSize);
        // 放入队头，等待下次重新被分配
        this.availableBuffers.offerFirst(byteBuffer);
    }

    // 从池中获取 ByteBuffer
    public ByteBuffer borrowBuffer() {
        // 非阻塞弹出队头元素，如果没有启用暂存池，则不会调用 init() 方法，队列中就没有元素，这里返回 null
        ByteBuffer buffer = availableBuffers.pollFirst();
        if (availableBuffers.size() < poolSize * 0.4) {
            log.warn("TransientStorePool only remain {} sheets.", availableBuffers.size());
        }
        return buffer;
    }

    // 剩下可借出的 ByteBuffer 数量
    public int availableBufferNums() {
        // /如果启用了暂存池，则返回队列中元素个数
        if (storeConfig.isTransientStorePoolEnable()) {
            return availableBuffers.size();
        }
        // 否则返会Integer.MAX_VALUE
        return Integer.MAX_VALUE;
    }
}
