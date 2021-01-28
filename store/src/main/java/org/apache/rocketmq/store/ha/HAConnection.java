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
package org.apache.rocketmq.store.ha;

import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.remoting.common.RemotingUtil;
import org.apache.rocketmq.store.SelectMappedBufferResult;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;

/**
 * HA Master 服务端 HA 连接对象的封装，与 Broker 从服务器的网络读写实现类
 * Master 服务器在收到从服务器的连接请求后，会将主从服务器的连接 SocketChannel 封装成 HAConnection 对象，实现主服务器与从服务器的读写操作
 */
public class HAConnection {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
    // HAService 对象
    private final HAService haService;
    // 网络 socket 通道
    private final SocketChannel socketChannel;
    // 客户端连接地址
    private final String clientAddr;
    // 服务端向从服务器写数据服务类
    private WriteSocketService writeSocketService;
    // 服务端从从服务器读数据服务类
    private ReadSocketService readSocketService;

    // 从服务器请求拉取数据的偏移量
    private volatile long slaveRequestOffset = -1;
    // 从服务器反馈已拉取完成的数据偏移量
    private volatile long slaveAckOffset = -1;

    public HAConnection(final HAService haService, final SocketChannel socketChannel) throws IOException {
        this.haService = haService;
        this.socketChannel = socketChannel;
        this.clientAddr = this.socketChannel.socket().getRemoteSocketAddress().toString();
        this.socketChannel.configureBlocking(false);
        this.socketChannel.socket().setSoLinger(false, -1);
        this.socketChannel.socket().setTcpNoDelay(true);
        this.socketChannel.socket().setReceiveBufferSize(1024 * 64);
        this.socketChannel.socket().setSendBufferSize(1024 * 64);
        this.writeSocketService = new WriteSocketService(this.socketChannel);
        this.readSocketService = new ReadSocketService(this.socketChannel);
        this.haService.getConnectionCount().incrementAndGet();
    }

    public void start() {
        this.readSocketService.start();
        this.writeSocketService.start();
    }

    public void shutdown() {
        this.writeSocketService.shutdown(true);
        this.readSocketService.shutdown(true);
        this.close();
    }

    public void close() {
        if (this.socketChannel != null) {
            try {
                this.socketChannel.close();
            } catch (IOException e) {
                HAConnection.log.error("", e);
            }
        }
    }

    public SocketChannel getSocketChannel() {
        return socketChannel;
    }

    /**
     * HA Master 网络写实现类
     * HAConnection 的网络读请求由其内部类 ReadSocketService 线程来实现
     */
    class ReadSocketService extends ServiceThread {
        // 网络读缓存区大小，默认为 1M
        private static final int READ_MAX_BUFFER_SIZE = 1024 * 1024;
        // NIO 网络事件选择器
        private final Selector selector;
        // 网络通道，用于读写的 socket 通道
        private final SocketChannel socketChannel;
        // 网络读写缓存区，默认为 1M
        private final ByteBuffer byteBufferRead = ByteBuffer.allocate(READ_MAX_BUFFER_SIZE);
        // byteBuffer 当前处理指针
        private int processPosition = 0;
        // 上次读取数据的时间戳
        private volatile long lastReadTimestamp = System.currentTimeMillis();

        public ReadSocketService(final SocketChannel socketChannel) throws IOException {
            this.selector = RemotingUtil.openSelector();
            this.socketChannel = socketChannel;
            this.socketChannel.register(this.selector, SelectionKey.OP_READ);
            this.setDaemon(true);
        }

        @Override
        public void run() {
            HAConnection.log.info(this.getServiceName() + " service started");

            while (!this.isStopped()) {
                try {
                    // 每隔 1s 处理一次读就绪事件
                    this.selector.select(1000);
                    // 每次读请求调用其 processReadEvent 来解析从服务器的拉取请求。
                    boolean ok = this.processReadEvent();
                    if (!ok) {
                        HAConnection.log.error("processReadEvent error");
                        break;
                    }

                    long interval = HAConnection.this.haService.getDefaultMessageStore().getSystemClock().now() - this.lastReadTimestamp;
                    if (interval > HAConnection.this.haService.getDefaultMessageStore().getMessageStoreConfig().getHaHousekeepingInterval()) {
                        log.warn("ha housekeeping, found this connection[" + HAConnection.this.clientAddr + "] expired, " + interval);
                        break;
                    }
                } catch (Exception e) {
                    HAConnection.log.error(this.getServiceName() + " service has exception.", e);
                    break;
                }
            }

            this.makeStop();

            writeSocketService.makeStop();

            haService.removeConnection(HAConnection.this);

            HAConnection.this.haService.getConnectionCount().decrementAndGet();

            SelectionKey sk = this.socketChannel.keyFor(this.selector);
            if (sk != null) {
                sk.cancel();
            }

            try {
                this.selector.close();
                this.socketChannel.close();
            } catch (IOException e) {
                HAConnection.log.error("", e);
            }

            HAConnection.log.info(this.getServiceName() + " service end");
        }

        @Override
        public String getServiceName() {
            return ReadSocketService.class.getSimpleName();
        }

        /**
         * Slave 汇报进度唤醒 GroupTransferService ， 等待同步完成唤醒 GroupCommitRequest 的 CountDownLatch
         * @return
         */
        private boolean processReadEvent() {
            int readSizeZeroTimes = 0;

            // 如果 byteBufferRead 没有剩余空间，说明该 position==limit==capacity
            if (!this.byteBufferRead.hasRemaining()) {
                // 调用 byteBufferRead.flip()， 产生的效果为 position=O, limit=capacity
                this.byteBufferRead.flip();
                // 并设置 processPosition 为 0，表示从头开始处理，这里调用 byteBuffer.clear() 方法会更加容易理解
                this.processPosition = 0;
            }

            // NIO 网络读的常规方法，一般使用循环的方式进行读写直到 byteBuffer 中没有剩余的空间
            while (this.byteBufferRead.hasRemaining()) {
                try {
                    int readSize = this.socketChannel.read(this.byteBufferRead);
                    if (readSize > 0) {
                        readSizeZeroTimes = 0;
                        // 最后一次读数据时间
                        this.lastReadTimestamp = HAConnection.this.haService.getDefaultMessageStore().getSystemClock().now();
                        // 如果读取的字节大于 0 并且本次读取到的内容大于等于 8，表明收到了从服务器一条拉取消息的请求
                        if ((this.byteBufferRead.position() - this.processPosition) >= 8) {
                            int pos = this.byteBufferRead.position() - (this.byteBufferRead.position() % 8);
                            // 拉取消息偏移量
                            long readOffset = this.byteBufferRead.getLong(pos - 8);
                            this.processPosition = pos;

                            // 设置从服务器反馈已拉取完成的数据偏移量
                            HAConnection.this.slaveAckOffset = readOffset;
                            if (HAConnection.this.slaveRequestOffset < 0) {
                                HAConnection.this.slaveRequestOffset = readOffset;
                                log.info("slave[" + HAConnection.this.clientAddr + "] request offset " + readOffset);
                            }
                            // 由于有新的从服务器反馈拉取偏移量，服务端会通知由于同步等待 HA 复制结果而阻塞的消息发送者线程
                            // 唤醒 GroupTransferService
                            HAConnection.this.haService.notifyTransferSome(HAConnection.this.slaveAckOffset);
                        }
                    } else if (readSize == 0) {
                        // 如果读取到的字节数等于 0，则重复 3 次
                        if (++readSizeZeroTimes >= 3) {
                            // 否则结束本次读请求处理
                            break;
                        }
                    } else {
                        // 如果读取到的字节数小于 0，表示连接处于半关闭状态，返回 false，则意味着消息服务器将关闭该连接
                        log.error("read socket[" + HAConnection.this.clientAddr + "] < 0");
                        return false;
                    }
                } catch (IOException e) {
                    log.error("processReadEvent exception", e);
                    return false;
                }
            }

            return true;
        }
    }

    /**
     * HA Master 网络写实现类
     * 网络写请求由内部 WriteSocketService 线程来实现
     */
    class WriteSocketService extends ServiceThread {
        // NIO 网络事件选择器
        private final Selector selector;
        // 网络 socket 通道
        private final SocketChannel socketChannel;
        // 消息头长度，消息物理偏移量 ＋ 消息长度
        private final int headerSize = 8 + 4;
        private final ByteBuffer byteBufferHeader = ByteBuffer.allocate(headerSize);
        // 下一次传输的物理偏移量
        private long nextTransferFromWhere = -1;
        // 根据偏移量查找消息的结果
        private SelectMappedBufferResult selectMappedBufferResult;
        // 上一次数据是否传输完毕
        private boolean lastWriteOver = true;
        // 上次写入的时间戳
        private long lastWriteTimestamp = System.currentTimeMillis();

        public WriteSocketService(final SocketChannel socketChannel) throws IOException {
            this.selector = RemotingUtil.openSelector();
            this.socketChannel = socketChannel;
            this.socketChannel.register(this.selector, SelectionKey.OP_WRITE);
            this.setDaemon(true);
        }

        @Override
        public void run() {
            HAConnection.log.info(this.getServiceName() + " service started");

            while (!this.isStopped()) {
                try {
                    // 每隔 1s 处理一次写就绪事件
                    this.selector.select(1000);
                    // 如果 slaveRequestOffset（从服务器请求拉取数据的偏移量） 等于 -1，说明 Master 还未收到从服务器的拉取请求，放弃本次事件处理
                    // slaveRequestOffset 在收到从服务器拉取请求时更新
                    if (-1 == HAConnection.this.slaveRequestOffset) {
                        Thread.sleep(10);
                        continue;
                    }

                    // 如果 nextTransferFromWhere（下一次传输的物理偏移量）为 -1，表示初次进行数据传输，计算待传输的物理偏移量
                    if (-1 == this.nextTransferFromWhere) {
                        // 如果 slaveRequestOffset（从服务器请求拉取数据的偏移量）为 0，则从当前 commitlog 文件最大偏移量开始传输
                        if (0 == HAConnection.this.slaveRequestOffset) {
                            long masterOffset = HAConnection.this.haService.getDefaultMessageStore().getCommitLog().getMaxOffset();
                            masterOffset =
                                masterOffset
                                    - (masterOffset % HAConnection.this.haService.getDefaultMessageStore().getMessageStoreConfig()
                                    .getMappedFileSizeCommitLog());

                            if (masterOffset < 0) {
                                masterOffset = 0;
                            }

                            this.nextTransferFromWhere = masterOffset;
                        } else {
                            // 否则根据从服务器的拉取请求偏移量开始传输
                            this.nextTransferFromWhere = HAConnection.this.slaveRequestOffset;
                        }

                        log.info("master transfer data from " + this.nextTransferFromWhere + " to slave[" + HAConnection.this.clientAddr
                            + "], and slave request " + HAConnection.this.slaveRequestOffset);
                    }

                    // 判断上次写事件是否已将信息全部写入客户端
                    if (this.lastWriteOver) {
                        // 如果已全部写入，且当前系统时间与上次最后写入的时间间隔大于 HA 心跳检测时间，则发送一个心跳包
                        long interval =
                            HAConnection.this.haService.getDefaultMessageStore().getSystemClock().now() - this.lastWriteTimestamp;

                        if (interval > HAConnection.this.haService.getDefaultMessageStore().getMessageStoreConfig()
                            .getHaSendHeartbeatInterval()) {
                            // 心跳包的长度为 12 个字节(从服务器待拉取偏移量 ＋ size) 消息长度默认为 0，避免长连接由于空闲被关闭。HA 心跳包发送间隔通过 haSendHeartbeatInterval 放置，默认值为 5s
                            // Build Header
                            this.byteBufferHeader.position(0);
                            this.byteBufferHeader.limit(headerSize);
                            this.byteBufferHeader.putLong(this.nextTransferFromWhere);
                            this.byteBufferHeader.putInt(0);
                            this.byteBufferHeader.flip();

                            this.lastWriteOver = this.transferData();
                            if (!this.lastWriteOver)
                                continue;
                        }
                    } else {
                        // 如果上次数据未写完，则先传输上一次的数据，如果消息还是未全部传输，则结束此次事件处理
                        this.lastWriteOver = this.transferData();
                        if (!this.lastWriteOver)
                            continue;
                    }

                    // 传输消息到从服务器
                    // 根据消息从服务器请求的待拉取偏移量，查找该偏移量之后所有的可读消息，如果未查到匹配的消息，通知所有等待线程继续等待 1OOms
                    SelectMappedBufferResult selectResult =
                        HAConnection.this.haService.getDefaultMessageStore().getCommitLogData(this.nextTransferFromWhere);
                    if (selectResult != null) {
                        // 传输（写入）消息
                        // 如果匹配到消息，且查找到的消息总长度大于配置 HA 传输一次同步任务最大传输的字节数，则通过设置 ByteBuffer 的 limit 来控制只传输指定长度的字节
                        // 这就意味着 HA 客户端收到的消息会包含不完整的消息。HA 一批次传输消息最大字节通过 haTransferBatchSize 设置，默认值为 32k。
                        int size = selectResult.getSize();
                        if (size > HAConnection.this.haService.getDefaultMessageStore().getMessageStoreConfig().getHaTransferBatchSize()) {
                            size = HAConnection.this.haService.getDefaultMessageStore().getMessageStoreConfig().getHaTransferBatchSize();
                        }

                        long thisOffset = this.nextTransferFromWhere;
                        this.nextTransferFromWhere += size;

                        selectResult.getByteBuffer().limit(size);
                        this.selectMappedBufferResult = selectResult;

                        // Build Header
                        this.byteBufferHeader.position(0);
                        this.byteBufferHeader.limit(headerSize);
                        this.byteBufferHeader.putLong(thisOffset);
                        this.byteBufferHeader.putInt(size);
                        this.byteBufferHeader.flip();
                        // 传输数据
                        this.lastWriteOver = this.transferData();
                    } else {
                        // 通知所有等待线程继续等待 1OOms
                        // 等待 100 毫秒或者提前被唤醒
                        HAConnection.this.haService.getWaitNotifyObject().allWaitForRunning(100);
                    }
                } catch (Exception e) {

                    HAConnection.log.error(this.getServiceName() + " service has exception.", e);
                    break;
                }
            }

            HAConnection.this.haService.getWaitNotifyObject().removeFromWaitingThreadTable();

            if (this.selectMappedBufferResult != null) {
                this.selectMappedBufferResult.release();
            }

            this.makeStop();

            readSocketService.makeStop();

            haService.removeConnection(HAConnection.this);

            SelectionKey sk = this.socketChannel.keyFor(this.selector);
            if (sk != null) {
                sk.cancel();
            }

            try {
                this.selector.close();
                this.socketChannel.close();
            } catch (IOException e) {
                HAConnection.log.error("", e);
            }

            HAConnection.log.info(this.getServiceName() + " service end");
        }

        private boolean transferData() throws Exception {
            int writeSizeZeroTimes = 0;
            // Write Header
            while (this.byteBufferHeader.hasRemaining()) {
                int writeSize = this.socketChannel.write(this.byteBufferHeader);
                if (writeSize > 0) {
                    writeSizeZeroTimes = 0;
                    this.lastWriteTimestamp = HAConnection.this.haService.getDefaultMessageStore().getSystemClock().now();
                } else if (writeSize == 0) {
                    if (++writeSizeZeroTimes >= 3) {
                        break;
                    }
                } else {
                    throw new Exception("ha master write header error < 0");
                }
            }

            if (null == this.selectMappedBufferResult) {
                return !this.byteBufferHeader.hasRemaining();
            }

            writeSizeZeroTimes = 0;

            // Write Body
            if (!this.byteBufferHeader.hasRemaining()) {
                while (this.selectMappedBufferResult.getByteBuffer().hasRemaining()) {
                    int writeSize = this.socketChannel.write(this.selectMappedBufferResult.getByteBuffer());
                    if (writeSize > 0) {
                        writeSizeZeroTimes = 0;
                        this.lastWriteTimestamp = HAConnection.this.haService.getDefaultMessageStore().getSystemClock().now();
                    } else if (writeSize == 0) {
                        if (++writeSizeZeroTimes >= 3) {
                            break;
                        }
                    } else {
                        throw new Exception("ha master write body error < 0");
                    }
                }
            }

            boolean result = !this.byteBufferHeader.hasRemaining() && !this.selectMappedBufferResult.getByteBuffer().hasRemaining();

            if (!this.selectMappedBufferResult.getByteBuffer().hasRemaining()) {
                this.selectMappedBufferResult.release();
                this.selectMappedBufferResult = null;
            }

            return result;
        }

        @Override
        public String getServiceName() {
            return WriteSocketService.class.getSimpleName();
        }

        @Override
        public void shutdown() {
            super.shutdown();
        }
    }
}
