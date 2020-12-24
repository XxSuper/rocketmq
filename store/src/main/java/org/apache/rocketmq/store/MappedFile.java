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
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageExtBatch;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.store.config.FlushDiskType;
import org.apache.rocketmq.store.util.LibC;
import sun.nio.ch.DirectBuffer;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * MappedFile 内存映射文件，MappedFile 是 RocketMQ 存映射文件的具体实现
 * RocketMQ 通过使用内存映射文件来提高 IO 访问性能，无论是 CommitLog 、ConsumeQueue、IndexFile ，单个文件都被设计为固定长度，如果一个文件写满以后再创建一个新文件，文件名就为该文件第一条消息对应的全局物理偏移量
 */
public class MappedFile extends ReferenceResource {
    // 操作系统每页大小，默认 4k
    public static final int OS_PAGE_SIZE = 1024 * 4;
    protected static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    // 当前 JVM 实例中 MappedFile 虚拟内存
    private static final AtomicLong TOTAL_MAPPED_VIRTUAL_MEMORY = new AtomicLong(0);

    // 当前 JVM 实例中 MappedFile 对象个数
    private static final AtomicInteger TOTAL_MAPPED_FILES = new AtomicInteger(0);

    // 当前该文件的写指针，从 0 开始（内存映射文件中的写指针）
    protected final AtomicInteger wrotePosition = new AtomicInteger(0);

    // 当前文件的提交指针，如果开启 transientStorePoolEnable，则数据会存储在 TransientStorePool 中，然后提交到内存映射 ByteBuffer 中，在刷写到磁盘
    protected final AtomicInteger committedPosition = new AtomicInteger(0);

    // 刷写到磁盘指针，该指针之前的数据持久化到磁盘中
    private final AtomicInteger flushedPosition = new AtomicInteger(0);
    // 文件大小
    protected int fileSize;
    // 文件通道，该 MappedFile 文件对应的 channel
    protected FileChannel fileChannel;
    /**
     * Message will put to here first, and then reput to FileChannel if writeBuffer is not null.
     * 如果启用了 TransientStorePool，则 writeBuffer 为从暂时存储池中借用的 buffer，此时存储对象（比如消息等）会先写入该 writeBuffer，然后commit 到 fileChannel，最后对 fileChannel 进行 flush 刷盘
     */
    // 堆内存 ByteBuffer 如果不为空，数据先将存储在该 Buffer 中，然后提交到 MappedFile 对应的内存映射文件 Buffer。transientStorePoolEnable 为 true 时不为空
    protected ByteBuffer writeBuffer = null;

    // 堆内存池，一个内存 ByteBuffer 池实现，如果启用了 TransientStorePool 则不为空，transientStorePoolEnable 为 true 时启用
    protected TransientStorePool transientStorePool = null;
    // 文件名称
    private String fileName;

    // 该文件的初始偏移量，文件第一个消息的偏移，该文件中内容相对于整个文件的偏移，其实和文件名相同
    private long fileFromOffset;

    // 物理文件，该 MappedFile 对应的实际文件
    private File file;
    // 物理文件对应的内存映射 Buffer，通过 fileChannel.map 得到的可读写的内存映射 buffer，如果没有启用 TransientStorePool 则写数据时会写到该缓冲区中，刷盘时直接调用该映射 buffer 的 force 函数，而不需要进行commit操作
    private MappedByteBuffer mappedByteBuffer;
    // 文件最后一次内容写入时间
    private volatile long storeTimestamp = 0;
    // 是否是 MappedFileQueue 队列中第一个文件
    private boolean firstCreateInQueue = false;

    public MappedFile() {
    }

    /**
     * 根据是否开启 transientStorePoolEnable 存在两种初始化情况。transientStorePoolEnable 为 true 则表示内容先存储在堆外内存，然后通过 Commit 线程将数据提交到内存映射 buffer 中，
     * 再通过 Flush 线程将内存映射 Buffer 中的数据持久化到磁盘中
     * @param fileName
     * @param fileSize
     * @throws IOException
     */
    public MappedFile(final String fileName, final int fileSize) throws IOException {
        // 初始化文件
        init(fileName, fileSize);
    }

    // 启用了暂存池
    public MappedFile(final String fileName, final int fileSize,
        final TransientStorePool transientStorePool) throws IOException {
        init(fileName, fileSize, transientStorePool);
    }

    public static void ensureDirOK(final String dirName) {
        if (dirName != null) {
            File f = new File(dirName);
            if (!f.exists()) {
                boolean result = f.mkdirs();
                log.info(dirName + " mkdir " + (result ? "OK" : "Failed"));
            }
        }
    }

    public static void clean(final ByteBuffer buffer) {
        // 如果是堆外内存，调用堆外内存的 cleanup 方法清除
        if (buffer == null || !buffer.isDirect() || buffer.capacity() == 0)
            return;
        invoke(invoke(viewed(buffer), "cleaner"), "clean");
    }

    private static Object invoke(final Object target, final String methodName, final Class<?>... args) {
        return AccessController.doPrivileged(new PrivilegedAction<Object>() {
            public Object run() {
                try {
                    Method method = method(target, methodName, args);
                    method.setAccessible(true);
                    return method.invoke(target);
                } catch (Exception e) {
                    throw new IllegalStateException(e);
                }
            }
        });
    }

    private static Method method(Object target, String methodName, Class<?>[] args)
        throws NoSuchMethodException {
        try {
            return target.getClass().getMethod(methodName, args);
        } catch (NoSuchMethodException e) {
            return target.getClass().getDeclaredMethod(methodName, args);
        }
    }

    private static ByteBuffer viewed(ByteBuffer buffer) {
        String methodName = "viewedBuffer";
        Method[] methods = buffer.getClass().getMethods();
        for (int i = 0; i < methods.length; i++) {
            if (methods[i].getName().equals("attachment")) {
                methodName = "attachment";
                break;
            }
        }

        ByteBuffer viewedBuffer = (ByteBuffer) invoke(buffer, methodName);
        if (viewedBuffer == null)
            return buffer;
        else
            return viewed(viewedBuffer);
    }

    public static int getTotalMappedFiles() {
        return TOTAL_MAPPED_FILES.get();
    }

    public static long getTotalMappedVirtualMemory() {
        return TOTAL_MAPPED_VIRTUAL_MEMORY.get();
    }

    // 启用了暂存池
    public void init(final String fileName, final int fileSize,
        final TransientStorePool transientStorePool) throws IOException {
        init(fileName, fileSize);
        // 如果 transientStorePoolEnable 为 true ，则初始化 MappedFile 的 writeBuffer
        // writeBuffer 会被赋值，后续写入操作会优先
        this.writeBuffer = transientStorePool.borrowBuffer();
        this.transientStorePool = transientStorePool;
    }

    private void init(final String fileName, final int fileSize) throws IOException {
        // 文件名
        this.fileName = fileName;
        // 文件大小
        this.fileSize = fileSize;
        this.file = new File(fileName);
        // 文件第一个消息的偏移，初始化 fileFromOffset 为文件名，也就是文件名代表该文件的起始偏移量
        this.fileFromOffset = Long.parseLong(this.file.getName());
        boolean ok = false;

        // 创建父文件目录
        ensureDirOK(this.file.getParent());

        try {
            // 通过 RandomAccessFile 创建读写文件通道，内存文件映射
            this.fileChannel = new RandomAccessFile(this.file, "rw").getChannel();
            // 文件映射到虚拟内存，MapMode.READ_WRITE：读/写，对得到的缓冲区的更改最终将写入文件；但该更改对映射到同一文件的其他程序不一定是可见的
            // 将文件内容使用 NIO 的内存映射 Buffer 将文件映射到内存中
            this.mappedByteBuffer = this.fileChannel.map(MapMode.READ_WRITE, 0, fileSize);
            // 统计计数器更新
            TOTAL_MAPPED_VIRTUAL_MEMORY.addAndGet(fileSize);
            TOTAL_MAPPED_FILES.incrementAndGet();
            ok = true;
        } catch (FileNotFoundException e) {
            log.error("Failed to create file " + this.fileName, e);
            throw e;
        } catch (IOException e) {
            log.error("Failed to map file " + this.fileName, e);
            throw e;
        } finally {
            if (!ok && this.fileChannel != null) {
                this.fileChannel.close();
            }
        }
    }

    public long getLastModifiedTimestamp() {
        return this.file.lastModified();
    }

    public int getFileSize() {
        return fileSize;
    }

    public FileChannel getFileChannel() {
        return fileChannel;
    }

    // 写入消息
    public AppendMessageResult appendMessage(final MessageExtBrokerInner msg, final AppendMessageCallback cb) {
        return appendMessagesInner(msg, cb);
    }

    public AppendMessageResult appendMessages(final MessageExtBatch messageExtBatch, final AppendMessageCallback cb) {
        return appendMessagesInner(messageExtBatch, cb);
    }

    public AppendMessageResult appendMessagesInner(final MessageExt messageExt, final AppendMessageCallback cb) {
        assert messageExt != null;
        assert cb != null;

        // 获取 MappedFile 当前写指针
        int currentPos = this.wrotePosition.get();

        // 如果 currentPos 大于或等于文件大小则表明文件已写满，抛出 AppendMessageStatus.UNKNOWN_ERROR，如果 currentPos 小于文件大小，通过 slice() 方法创建一个与 MappedFile 的共
        // 存区，并设置 position 为当前写指针
        if (currentPos < this.fileSize) {
            // 如果 writeBuffer 不为空，则优先写入 writeBuffer（暂存池中获取的），否则写入 mappedByteBuffer，如果启用了暂存池 TransientStorePool 则 writeBuffer 会被初始化
            // 否则 writeBuffer 为空，slice() 方法会返回一个新的 buffer，但是新的 buffer 和源对象 buffer 引用的是同一个
            ByteBuffer byteBuffer = writeBuffer != null ? writeBuffer.slice() : this.mappedByteBuffer.slice();
            byteBuffer.position(currentPos);
            AppendMessageResult result;
            // 对消息进行编码，然后将编码后的数据写入得到的 byteBuffer 等待刷盘
            if (messageExt instanceof MessageExtBrokerInner) {
                result = cb.doAppend(this.getFileFromOffset(), byteBuffer, this.fileSize - currentPos, (MessageExtBrokerInner) messageExt);
            } else if (messageExt instanceof MessageExtBatch) {
                result = cb.doAppend(this.getFileFromOffset(), byteBuffer, this.fileSize - currentPos, (MessageExtBatch) messageExt);
            } else {
                return new AppendMessageResult(AppendMessageStatus.UNKNOWN_ERROR);
            }
            this.wrotePosition.addAndGet(result.getWroteBytes());
            this.storeTimestamp = result.getStoreTimestamp();
            return result;
        }
        log.error("MappedFile.appendMessage return null, wrotePosition: {} fileSize: {}", currentPos, this.fileSize);
        return new AppendMessageResult(AppendMessageStatus.UNKNOWN_ERROR);
    }

    public long getFileFromOffset() {
        return this.fileFromOffset;
    }

    public boolean appendMessage(final byte[] data) {
        int currentPos = this.wrotePosition.get();

        if ((currentPos + data.length) <= this.fileSize) {
            try {
                this.fileChannel.position(currentPos);
                this.fileChannel.write(ByteBuffer.wrap(data));
            } catch (Throwable e) {
                log.error("Error occurred when append message to mappedFile.", e);
            }
            this.wrotePosition.addAndGet(data.length);
            return true;
        }

        return false;
    }

    /**
     * Content of data from offset to offset + length will be wrote to file.
     *
     * @param offset The offset of the subarray to be used.
     * @param length The length of the subarray to be used.
     */
    public boolean appendMessage(final byte[] data, final int offset, final int length) {
        int currentPos = this.wrotePosition.get();

        if ((currentPos + length) <= this.fileSize) {
            try {
                this.fileChannel.position(currentPos);
                this.fileChannel.write(ByteBuffer.wrap(data, offset, length));
            } catch (Throwable e) {
                log.error("Error occurred when append message to mappedFile.", e);
            }
            this.wrotePosition.addAndGet(length);
            return true;
        }

        return false;
    }

    /**
     * MappedFile 刷盘（flush)，刷盘指的是将内存中的数据刷写到磁盘，永久存储在磁盘中
     * @return The current flushed position
     */
    public int flush(final int flushLeastPages) {
        // 刷写磁盘，直接调用 mappedByteBuffer 或 fileChannel 的 force 方法将内存中的数据持久化到磁盘，那么 flushedPosition 应该等于 MappedByteBuffer 中的写指针
        // 如果 writeBuffer 不为空， flushedPosition 应等于上一次 commit 指针；因为上一次提交的数据就是，进入到 MappedByteBuffer 中的数据；如果 writeBuffer 为空，数据是直接进入到 MappedByteBuffer
        // wrotePosition 代表的是 MappedByteBuffer 中的指针，故设置 flushedPosition 为 wrotePosition。
        if (this.isAbleToFlush(flushLeastPages)) {
            if (this.hold()) {
                // 获取最大可读指针
                int value = getReadPosition();

                try {
                    //We only append data to fileChannel or mappedByteBuffer, never both.
                    if (writeBuffer != null || this.fileChannel.position() != 0) {
                        this.fileChannel.force(false);
                    } else {
                        this.mappedByteBuffer.force();
                    }
                } catch (Throwable e) {
                    log.error("Error occurred when force data to disk.", e);
                }

                this.flushedPosition.set(value);
                this.release();
            } else {
                log.warn("in flush, hold failed, flush offset = " + this.flushedPosition.get());
                this.flushedPosition.set(getReadPosition());
            }
        }
        return this.getFlushedPosition();
    }

    /**
     * 内存映射文件的提交动作由 MappedFile 的 commit() 方法实现
     * 执行提交操作，commitLeastPages 为本次提交最小的页数
     * @param commitLeastPages
     * @return
     */
    public int commit(final int commitLeastPages) {
        // writeBuffer 如果为空，直接返回 wrotePosition 指针 ，无须执行 commit 操作，表明 commit 操作主体是 writeBuffer
        if (writeBuffer == null) {
            //no need to commit data to file channel, so just regard wrotePosition as committedPosition.
            return this.wrotePosition.get();
        }
        // 如果待提交数据不满 commitLeastPages，则不执行本次提交操作，待下次提交
        if (this.isAbleToCommit(commitLeastPages)) {
            if (this.hold()) {
                // 执行提交
                commit0(commitLeastPages);
                this.release();
            } else {
                log.warn("in commit, hold failed, commit offset = " + this.committedPosition.get());
            }
        }

        // All dirty data has been committed to FileChannel.
        if (writeBuffer != null && this.transientStorePool != null && this.fileSize == this.committedPosition.get()) {
            this.transientStorePool.returnBuffer(writeBuffer);
            this.writeBuffer = null;
        }

        return this.committedPosition.get();
    }

    /**
     *
     * ByteBuffer 使用技巧： slice() 方法创建一个共享缓存区，与原先的 ByteBuffer 共享内存但维护一套独立的指针（position、mark、limit）
     *
     * 具体的提交实现，commit 的作用就是 MappedFile#writeBuffer 中的数据提交到 FileChannel 中
     * @param commitLeastPages
     */
    protected void commit0(final int commitLeastPages) {
        int writePos = this.wrotePosition.get();
        int lastCommittedPosition = this.committedPosition.get();

        if (writePos - this.committedPosition.get() > 0) {
            try {
                // 首先创建 writeBuffer 的共享缓存区
                ByteBuffer byteBuffer = writeBuffer.slice();
                // 然后将新创建的 position 回退到上一次提交的位置（committedPosition）
                byteBuffer.position(lastCommittedPosition);
                // 设置 limit 为 wrotePosition（当前最大有效数据指针）
                byteBuffer.limit(writePos);
                // 然后把 commitedPosition 到 wrotePosition 的数据复制（写入）到 FileChannel 中
                this.fileChannel.position(lastCommittedPosition);
                this.fileChannel.write(byteBuffer);
                // 然后更新 committedPosition 指针 为 wrotePosition
                this.committedPosition.set(writePos);
            } catch (Throwable e) {
                log.error("Error occurred when commit data to FileChannel.", e);
            }
        }
    }

    private boolean isAbleToFlush(final int flushLeastPages) {
        int flush = this.flushedPosition.get();
        // 获取 MappedFile 最大读指针
        int write = getReadPosition();

        if (this.isFull()) {
            return true;
        }

        if (flushLeastPages > 0) {
            return ((write / OS_PAGE_SIZE) - (flush / OS_PAGE_SIZE)) >= flushLeastPages;
        }

        return write > flush;
    }

    /**
     * 判断是否执行 commit 操作
     * @param commitLeastPages
     * @return
     */
    protected boolean isAbleToCommit(final int commitLeastPages) {
        int flush = this.committedPosition.get();
        int write = this.wrotePosition.get();

        // 如果文件己满返回 true
        if (this.isFull()) {
            return true;
        }

        // 如果 commitLeastPages 大于 0，则比较 wrotePosition（ 当前 writeBuffer 的写指针）与上一次提交的指针（committedPosition) 差值，除 OS_PAGE_SIZE 得到当前脏页的数量，如果大于 commitLeastPages 则返回 true
        // 如果 commitLeastPages 小于 0 表示只要存在脏页就提交
        if (commitLeastPages > 0) {
            return ((write / OS_PAGE_SIZE) - (flush / OS_PAGE_SIZE)) >= commitLeastPages;
        }

        return write > flush;
    }

    public int getFlushedPosition() {
        return flushedPosition.get();
    }

    public void setFlushedPosition(int pos) {
        this.flushedPosition.set(pos);
    }

    public boolean isFull() {
        return this.fileSize == this.wrotePosition.get();
    }

    /**
     * 操作 ByteBuffer 时如果使用了 slice() 方法，对其 ByteBuffer 进行读取时一般手动指定 position 与 limit 指针，而不是调用 flip 方法来切换读写状态
     * @param pos
     * @param size
     * @return
     */
    public SelectMappedBufferResult selectMappedBuffer(int pos, int size) {
        // 查找 pos 到当前最大可读之间的数据，由于在整个写入期间都未曾改变 MappedByteBuffer 的指针，所以 mappedByteBuffer.slice() 方法返回的共享缓存区空间为整个 mappedFile
        // 然后通过设置 byteBuffer 的 position 为待查找的值，读取字节为当前可读字节长度，最终返回的 ByteBuffer 的 limit （可读最大长度）为 size。整个共享缓存区的容量
        // 为（MappedFile#fileSize-pos），故在操作 SelectMappedBufferResult 不能对包含在里面的 ByteBuffer 调用 flip 方法
        int readPosition = getReadPosition();
        if ((pos + size) <= readPosition) {
            if (this.hold()) {
                ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
                byteBuffer.position(pos);
                ByteBuffer byteBufferNew = byteBuffer.slice();
                byteBufferNew.limit(size);
                return new SelectMappedBufferResult(this.fileFromOffset + pos, byteBufferNew, size, this);
            } else {
                log.warn("matched, but hold failed, request pos: " + pos + ", fileFromOffset: "
                    + this.fileFromOffset);
            }
        } else {
            log.warn("selectMappedBuffer request pos invalid, request pos: " + pos + ", size: " + size
                + ", fileFromOffset: " + this.fileFromOffset);
        }

        return null;
    }

    /**
     * 查找 pos 到当前最大可读之间的数据
     * @param pos
     * @return
     */
    public SelectMappedBufferResult selectMappedBuffer(int pos) {
        int readPosition = getReadPosition();
        if (pos < readPosition && pos >= 0) {
            if (this.hold()) {
                // slice() 方法用于创建一个新的字节缓冲区，其内容是给定缓冲区内容的共享子序列。
                // 新缓冲区的内容将从该缓冲区的当前位置开始。对该缓冲区内容的更改将在新缓冲区中可见，反之亦然。这两个缓冲区的位置，限制和标记值将是独立的。
                // 新缓冲区的位置将为零，其容量和限制将为该缓冲区中剩余的浮点数，并且其标记将不确定。当且仅当该缓冲区是直接缓冲区时，新缓冲区才是直接缓冲区；当且仅当该缓冲区是只读缓冲区时，新缓冲区才是只读缓冲区。
                ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
                byteBuffer.position(pos);
                int size = readPosition - pos;
                ByteBuffer byteBufferNew = byteBuffer.slice();
                byteBufferNew.limit(size);
                return new SelectMappedBufferResult(this.fileFromOffset + pos, byteBufferNew, size, this);
            }
        }

        return null;
    }

    @Override
    public boolean cleanup(final long currentRef) {
        // 判断是否可用，如果 available 为 true，表示 MappedFile 当前可用，无须清理，返回 false
        if (this.isAvailable()) {
            log.error("this file[REF:" + currentRef + "] " + this.fileName
                + " have not shutdown, stop unmapping.");
            return false;
        }

        // 判断是否清理完成，如果资源已经被清除，返回 true
        if (this.isCleanupOver()) {
            log.error("this file[REF:" + currentRef + "] " + this.fileName
                + " have cleanup, do not do it again.");
            return true;
        }


        clean(this.mappedByteBuffer);
        // 维护 MappedFile 类变量 TOTAL_MAPPED_VIRTUAL_MEMORY、TOTAL_MAPPED_FILES 并返回 true ，表示 cleanupOver 为 true
        TOTAL_MAPPED_VIRTUAL_MEMORY.addAndGet(this.fileSize * (-1));
        TOTAL_MAPPED_FILES.decrementAndGet();
        log.info("unmap file[REF:" + currentRef + "] " + this.fileName + " OK");
        return true;
    }

    /**
     * MappedFile 文件销毁，在整个 MappedFile 销毁过程中，首先需要释放资源，释放资源的前提条件是该 MappedFile 的引用小于等于 0
     * @param intervalForcibly 表示拒绝被销毁的最大存活时间
     * @return
     */
    public boolean destroy(final long intervalForcibly) {
        this.shutdown(intervalForcibly);

        // 是否清理完成
        if (this.isCleanupOver()) {
            try {
                // 关闭文件通道，删除物理文件
                this.fileChannel.close();
                log.info("close file channel " + this.fileName + " OK");

                long beginTime = System.currentTimeMillis();
                boolean result = this.file.delete();
                log.info("delete file[REF:" + this.getRefCount() + "] " + this.fileName
                    + (result ? " OK, " : " Failed, ") + "W:" + this.getWrotePosition() + " M:"
                    + this.getFlushedPosition() + ", "
                    + UtilAll.computeElapsedTimeMilliseconds(beginTime));
            } catch (Exception e) {
                log.warn("close file channel " + this.fileName + " Failed. ", e);
            }

            return true;
        } else {
            log.warn("destroy mapped file[REF:" + this.getRefCount() + "] " + this.fileName
                + " Failed. cleanupOver: " + this.cleanupOver);
        }

        return false;
    }

    public int getWrotePosition() {
        return wrotePosition.get();
    }

    public void setWrotePosition(int pos) {
        this.wrotePosition.set(pos);
    }

    /**
     * 获取 MappedFile 文件最大的可读指针
     * RocketMQ 文件的一个组织方式是内存映射文件，预先申请一块连续的固定大小的内存，需要一套指针标识当前最大有效数据的位置，获取最大有效数据偏移量的方法
     * @return The max position which have valid data
     */
    public int getReadPosition() {
        // 获取当前文件最大的可读指针。如果 writeBuffer 为空，直接返回当前的写指针，如果 writeBuffer 不为空，则返回上一次提交的指针，在 MappedFile 设计中，只有提交了的数据（写入到 MappedByteBuffer FileChannel 中的数据 ）才是安全的数据
        return this.writeBuffer == null ? this.wrotePosition.get() : this.committedPosition.get();
    }

    public void setCommittedPosition(int pos) {
        this.committedPosition.set(pos);
    }

    /**
     * 文件预热：文件预热的时候需要了解的知识点。操作系统的 Page Cache 和内存映射技术 mmap。
     *
     * Page Cache：Page Cache 叫做页缓存，而每一页的大小通常是4K，在 Linux 系统中写入数据的时候并不会直接写到硬盘上，而是会先写到 Page Cache 中，并打上 dirty 标识，由内核线程 flusher 定期将被打上 dirty 的页发送给 IO 调度层，最后由 IO 调度决定何时落地到磁盘中，
     * 而 Linux 一般会把还没有使用的内存全拿来给 Page Cache 使用。而读的过程也是类似，会先到 Page Cache 中寻找是否有数据，有的话直接返回，如果没有才会到磁盘中去读取并写入 Page Cache 然后再次读取 Page Cache 并返回。而且读的这个过程中操作系统也会有一个预读的操作，
     * 你的每一次读取操作系统都会帮你预读出后面一部分数据，而且当你一直在使用预读数据的时候，系统会帮你预读出更多的数据(最大到 128K )。
     *
     * mmap：mmap 是一种将文件映射到虚拟内存的技术，可以将文件在磁盘位置的地址和在虚拟内存中的虚拟地址通过映射对应起来，之后就可以在内存这块区域进行读写数据，而不必调用系统级别的 read、wirte 这些函数，从而提升 IO 操作性能，
     * 另外一点就是 mmap 后的虚拟内存大小必须是内存页大小 (通常是 4K) 的倍数，之所以这么做是为了匹配内存操作。
     *
     * 这里 MappedFile 已经创建，对应的 Buffer 为 mappedByteBuffer。mappedByteBuffer 已经通过 mmap 映射，此时操作系统中只是记录了该文件和该 Buffer 的映射关系，而没有映射到物理内存中。这里就通过对该 MappedFile 的每个 Page Cache 进行写入一个字节，通过读写操作把 mmap 映射全部加载到物理内存中。
     *
     * @param type
     * @param pages
     */
    public void warmMappedFile(FlushDiskType type, int pages) {
        long beginTime = System.currentTimeMillis();
        ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
        int flush = 0;
        long time = System.currentTimeMillis();
        for (int i = 0, j = 0; i < this.fileSize; i += MappedFile.OS_PAGE_SIZE, j++) {
            byteBuffer.put(i, (byte) 0);
            // force flush when flush disk type is sync
            // 如果是同步写盘操作，则进行强行刷盘操作
            if (type == FlushDiskType.SYNC_FLUSH) {
                if ((i / OS_PAGE_SIZE) - (flush / OS_PAGE_SIZE) >= pages) {
                    flush = i;
                    mappedByteBuffer.force();
                }
            }

            // prevent gc
            if (j % 1000 == 0) {
                log.info("j={}, costTime={}", j, System.currentTimeMillis() - time);
                time = System.currentTimeMillis();
                try {
                    Thread.sleep(0);
                } catch (InterruptedException e) {
                    log.error("Interrupted", e);
                }
            }
        }

        // force flush when prepare load finished
        // 把剩余的数据强制刷新到磁盘中
        if (type == FlushDiskType.SYNC_FLUSH) {
            log.info("mapped file warm-up done, force to disk, mappedFile={}, costTime={}",
                this.getFileName(), System.currentTimeMillis() - beginTime);
            mappedByteBuffer.force();
        }
        log.info("mapped file warm-up done. mappedFile={}, costTime={}", this.getFileName(),
            System.currentTimeMillis() - beginTime);

        this.mlock();
    }

    public String getFileName() {
        return fileName;
    }

    public MappedByteBuffer getMappedByteBuffer() {
        return mappedByteBuffer;
    }

    public ByteBuffer sliceByteBuffer() {
        return this.mappedByteBuffer.slice();
    }

    public long getStoreTimestamp() {
        return storeTimestamp;
    }

    public boolean isFirstCreateInQueue() {
        return firstCreateInQueue;
    }

    public void setFirstCreateInQueue(boolean firstCreateInQueue) {
        this.firstCreateInQueue = firstCreateInQueue;
    }

    /**
     * 该方法主要是实现文件预热后，防止把预热过的文件被操作系统调到 swap 空间中。当程序再次读取交换出去的数据的时候会产生缺页异常。
     * LibC.INSTANCE.mlock 和 LibC.INSTANCE.madvise 都是调用的 Native 方法。
     */
    public void mlock() {
        final long beginTime = System.currentTimeMillis();
        final long address = ((DirectBuffer) (this.mappedByteBuffer)).address();
        Pointer pointer = new Pointer(address);
        {
            // 将锁住指定的内存区域避免被操作系统调到 swap 空间中。
            int ret = LibC.INSTANCE.mlock(pointer, new NativeLong(this.fileSize));
            log.info("mlock {} {} {} ret = {} time consuming = {}", address, this.fileName, this.fileSize, ret, System.currentTimeMillis() - beginTime);
        }

        {
            // 一次性先将一段数据读入到映射内存区域，这样就减少了缺页异常的产生。
            int ret = LibC.INSTANCE.madvise(pointer, new NativeLong(this.fileSize), LibC.MADV_WILLNEED);
            log.info("madvise {} {} {} ret = {} time consuming = {}", address, this.fileName, this.fileSize, ret, System.currentTimeMillis() - beginTime);
        }
    }

    public void munlock() {
        final long beginTime = System.currentTimeMillis();
        final long address = ((DirectBuffer) (this.mappedByteBuffer)).address();
        Pointer pointer = new Pointer(address);
        int ret = LibC.INSTANCE.munlock(pointer, new NativeLong(this.fileSize));
        log.info("munlock {} {} {} ret = {} time consuming = {}", address, this.fileName, this.fileSize, ret, System.currentTimeMillis() - beginTime);
    }

    //testable
    File getFile() {
        return this.file;
    }

    @Override
    public String toString() {
        return this.fileName;
    }
}
