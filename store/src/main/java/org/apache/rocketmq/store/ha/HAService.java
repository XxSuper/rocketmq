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

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.remoting.common.RemotingUtil;
import org.apache.rocketmq.store.CommitLog;
import org.apache.rocketmq.store.DefaultMessageStore;

/**
 * 为了提高消息消费的高可用性，避免 Broker 发生单点故障引起存储在 Broker 上的消息无法及时消费，RocketMQ 引入了 Broker 主备机制即消息消费到达主服务器后需要
 * 消息同步到消息从服务器，如果主服务器 Broker 宕机后，消息消费者可以从从服务器拉取消息.
 *
 * RocketMQ HA 实现原理：
 * 1、主服务器启动，并在特定端口上监昕从服务器的连接
 * 2、从服务器主动连接主服务器，主服务器客户端的连接，并建立相关 TCP 连接
 * 3、从服务器主动向主服务器发送待拉取消息偏移，主服务器解析请求并返回消息给从服务器
 * 4、从服务器保存消息并继续发送新的消息同步请求
 *
 * RocketMQ 主从同步核心实现类
 */
public class HAService {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    private final AtomicInteger connectionCount = new AtomicInteger(0);

    private final List<HAConnection> connectionList = new LinkedList<>();

    private final AcceptSocketService acceptSocketService;

    private final DefaultMessageStore defaultMessageStore;

    private final WaitNotifyObject waitNotifyObject = new WaitNotifyObject();
    private final AtomicLong push2SlaveMaxOffset = new AtomicLong(0);

    private final GroupTransferService groupTransferService;

    private final HAClient haClient;

    public HAService(final DefaultMessageStore defaultMessageStore) throws IOException {
        this.defaultMessageStore = defaultMessageStore;
        this.acceptSocketService =
            new AcceptSocketService(defaultMessageStore.getMessageStoreConfig().getHaListenPort());
        this.groupTransferService = new GroupTransferService();
        this.haClient = new HAClient();
    }

    public void updateMasterAddress(final String newAddr) {
        if (this.haClient != null) {
            this.haClient.updateMasterAddress(newAddr);
        }
    }

    public void putRequest(final CommitLog.GroupCommitRequest request) {
        this.groupTransferService.putRequest(request);
    }

    public boolean isSlaveOK(final long masterPutWhere) {
        boolean result = this.connectionCount.get() > 0;
        result =
            result
                && ((masterPutWhere - this.push2SlaveMaxOffset.get()) < this.defaultMessageStore
                .getMessageStoreConfig().getHaSlaveFallbehindMax());
        return result;
    }

    /**
     * 该方法在 Master 收到从服务器的拉取请求后被调用，表示从服务器当前已同步的偏移量，既然收到从服务器的反馈信息，需要唤醒某些消息发送者线程。如果从从服务器中
     * 收到的确认偏移量大于 push2SlaveMaxOffsset，则更新 push2SlaveMaxOffset，然后唤醒 GroupTransferService 线程，各消息发送者线程再次判断
     * 自己本次发送的消息是否已经成功复制到从服务器
     * @param offset
     */
    public void notifyTransferSome(final long offset) {
        for (long value = this.push2SlaveMaxOffset.get(); offset > value; ) {
            // 确认偏移量大于 push2SlaveMaxOffsset，则更新 push2SlaveMaxOffset
            boolean ok = this.push2SlaveMaxOffset.compareAndSet(value, offset);
            if (ok) {
                // 唤醒 GroupTransferService 线程（线程在 doWaitTransfer() 方法中等待）
                // GroupTransferService 线程再调用 CommitLog.GroupCommitRequest.wakeupCustomer(transferOK) 唤醒消息发送者线程
                this.groupTransferService.notifyTransferSome();
                break;
            } else {
                value = this.push2SlaveMaxOffset.get();
            }
        }
    }

    public AtomicInteger getConnectionCount() {
        return connectionCount;
    }

    // public void notifyTransferSome() {
    // this.groupTransferService.notifyTransferSome();
    // }

    /**
     * 主服务器启动，并在特定端口上监听从服务器的连接
     * 从服务器主动连接主服务器，主服务器接收客户端的连接，并建立相关 TCP 连接
     * 从服务器主动向主服务器发送待拉取消息偏移量，主服务器解析请求并返回消息给从服务器
     * 从服务器保存消息并继续发送新的消息同步请求
     *
     * @throws Exception
     */
    public void start() throws Exception {
        this.acceptSocketService.beginAccept();
        this.acceptSocketService.start();
        this.groupTransferService.start();
        this.haClient.start();
    }

    public void addConnection(final HAConnection conn) {
        synchronized (this.connectionList) {
            this.connectionList.add(conn);
        }
    }

    public void removeConnection(final HAConnection conn) {
        synchronized (this.connectionList) {
            this.connectionList.remove(conn);
        }
    }

    public void shutdown() {
        this.haClient.shutdown();
        this.acceptSocketService.shutdown(true);
        this.destroyConnections();
        this.groupTransferService.shutdown();
    }

    public void destroyConnections() {
        synchronized (this.connectionList) {
            for (HAConnection c : this.connectionList) {
                c.shutdown();
            }

            this.connectionList.clear();
        }
    }

    public DefaultMessageStore getDefaultMessageStore() {
        return defaultMessageStore;
    }

    public WaitNotifyObject getWaitNotifyObject() {
        return waitNotifyObject;
    }

    public AtomicLong getPush2SlaveMaxOffset() {
        return push2SlaveMaxOffset;
    }

    /**
     * HA Master 端监昕客户端连接实现类
     * Listens to slave connections to create {@link HAConnection}.
     */
    class AcceptSocketService extends ServiceThread {
        // Broker 服务监听套接字（本地 IP ＋ 端口号）
        private final SocketAddress socketAddressListen;
        // 服务端 Socket 通道， 基于 NIO
        private ServerSocketChannel serverSocketChannel;
        // 事件选择器，基于 NIO
        private Selector selector;

        public AcceptSocketService(final int port) {
            this.socketAddressListen = new InetSocketAddress(port);
        }

        /**
         * 创建 ServerSocketChannel、创建 Selector、设置 TCP reuseAddress、绑定监昕端口、设置为非阻塞模式，并注册 OP_ACCEPT(连接事件)
         * Starts listening to slave connections.
         *
         * @throws Exception If fails.
         */
        public void beginAccept() throws Exception {
            // 创建 ServerSocketChannel
            this.serverSocketChannel = ServerSocketChannel.open();
            // 创建 Selector
            this.selector = RemotingUtil.openSelector();
            // 设置 TCP reuseAddress
            // 为了确保一个进程被关闭后，即使它还没有释放该端口，同一个主机上的其他进程还可以立刻重用该端口，可以调用 socket().setReuseAddress(true) 方法
            this.serverSocketChannel.socket().setReuseAddress(true);
            // 绑定监听端口
            this.serverSocketChannel.socket().bind(this.socketAddressListen);
            // 设置为非阻塞模式
            this.serverSocketChannel.configureBlocking(false);
            // 注册 OP_ACCEPT（连接事件）
            this.serverSocketChannel.register(this.selector, SelectionKey.OP_ACCEPT);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void shutdown(final boolean interrupt) {
            super.shutdown(interrupt);
            try {
                this.serverSocketChannel.close();
                this.selector.close();
            } catch (IOException e) {
                log.error("AcceptSocketService shutdown exception", e);
            }
        }

        /**
         * 该方法是标准的基于 NIO 的服务端程式实例
         * {@inheritDoc}
         */
        @Override
        public void run() {
            log.info(this.getServiceName() + " service started");

            while (!this.isStopped()) {
                try {
                    // 选择器每 1s 处理一次连接就绪事件
                    this.selector.select(1000);
                    Set<SelectionKey> selected = this.selector.selectedKeys();

                    if (selected != null) {
                        for (SelectionKey k : selected) {
                            if ((k.readyOps() & SelectionKey.OP_ACCEPT) != 0) {
                                // 连接事件就绪后，调用 ServerSocketChannel 的 accept() 方法创建 SocketChannel
                                SocketChannel sc = ((ServerSocketChannel) k.channel()).accept();

                                if (sc != null) {
                                    HAService.log.info("HAService receive new connection, "
                                        + sc.socket().getRemoteSocketAddress());

                                    try {
                                        // 为每一个连接创建一个 HAConnection 对象，该 HAConnection 将负责 M-S 数据同步逻辑
                                        HAConnection conn = new HAConnection(HAService.this, sc);
                                        conn.start();
                                        HAService.this.addConnection(conn);
                                    } catch (Exception e) {
                                        log.error("new HAConnection exception", e);
                                        sc.close();
                                    }
                                }
                            } else {
                                log.warn("Unexpected ops in select " + k.readyOps());
                            }
                        }

                        selected.clear();
                    }
                } catch (Exception e) {
                    log.error(this.getServiceName() + " service has exception.", e);
                }
            }

            log.info(this.getServiceName() + " service end");
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public String getServiceName() {
            return AcceptSocketService.class.getSimpleName();
        }
    }

    /**
     * 主从同步通知实现类
     *
     * GroupTransferService 主从同步阻塞实现，如果是主从同步模式，消息发送者将消息刷写到磁盘后，需要继续等待新数据被传输到从服务器，从服务器数据的复制是在另外一个线程 HAConnection 中去拉取，
     * 所以消息发送者在这里需要等待数据传输的结果，GroupTransferService 就是实现该功能，该类的整体结构与同步刷盘实现类（ CommitLog$GroupCommitService）类似，核心业务逻辑 doWaitTransfer 的实现
     *
     * GroupTransferService Service
     */
    class GroupTransferService extends ServiceThread {

        private final WaitNotifyObject notifyTransferObject = new WaitNotifyObject();
        private volatile List<CommitLog.GroupCommitRequest> requestsWrite = new ArrayList<>();
        private volatile List<CommitLog.GroupCommitRequest> requestsRead = new ArrayList<>();

        public synchronized void putRequest(final CommitLog.GroupCommitRequest request) {
            synchronized (this.requestsWrite) {
                this.requestsWrite.add(request);
            }
            if (hasNotified.compareAndSet(false, true)) {
                // 通知 GroupTransferService#run
                waitPoint.countDown(); // notify
            }
        }

        // 被唤醒
        public void notifyTransferSome() {
            this.notifyTransferObject.wakeup();
        }

        private void swapRequests() {
            List<CommitLog.GroupCommitRequest> tmp = this.requestsWrite;
            this.requestsWrite = this.requestsRead;
            this.requestsRead = tmp;
        }

        /**
         * GroupTransferService 的职责是负责当主从同步复制结束后通知由于等待 HA 同步结果而阻塞的消息发送者线程判断主从同步是否完成的依据是 Slave 中已成功复制的最大偏移
         * 量是否大于等于消息生产者发送消息后消息服务端返回下一条消息的起始偏移量，如果是则表示主从同步复制已经完成，唤醒消息发送线程，否则等待 1s 再次判断，每一个任务在一批任务
         * 中循环判断 5 次。消息发送者返回有两种情况： 等待超过 5s 或 GroupTransferService 通知主从复制完成。可以通过 syncFlushTimeout 设置发送线程等待超时时间
         */
        private void doWaitTransfer() {
            synchronized (this.requestsRead) {
                if (!this.requestsRead.isEmpty()) {
                    for (CommitLog.GroupCommitRequest req : this.requestsRead) {
                        // Slave 中已成功复制的最大偏移量是否大于等于消息生产者发送消息后消息服务端返回下一条消息的起始偏移量
                        boolean transferOK = HAService.this.push2SlaveMaxOffset.get() >= req.getNextOffset();
                        // 等待超时时间可以通过 syncFlushTimeout 设置发送线程等待超时时间
                        long waitUntilWhen = HAService.this.defaultMessageStore.getSystemClock().now()
                            + HAService.this.defaultMessageStore.getMessageStoreConfig().getSyncFlushTimeout();
                        while (!transferOK && HAService.this.defaultMessageStore.getSystemClock().now() < waitUntilWhen) {
                            // 等待 1s 再次判断
                            // 等待被唤醒或者超时，HAConnection.this.haService.notifyTransferSome(HAConnection.this.slaveAckOffset) 会进行唤醒
                            this.notifyTransferObject.waitForRunning(1000);
                            transferOK = HAService.this.push2SlaveMaxOffset.get() >= req.getNextOffset();
                        }

                        if (!transferOK) {
                            log.warn("transfer messsage to slave timeout, " + req.getNextOffset());
                        }
                        // 唤醒消息发送线程
                        // 唤醒 GroupCommitRequest 的 CountDownLatch，唤醒消息发送者等待线程
                        req.wakeupCustomer(transferOK);
                    }

                    this.requestsRead.clear();
                }
            }
        }

        public void run() {
            log.info(this.getServiceName() + " service started");

            while (!this.isStopped()) {
                try {
                    // putRequest 会提前唤醒这句话
                    this.waitForRunning(10);
                    this.doWaitTransfer();
                } catch (Exception e) {
                    log.warn(this.getServiceName() + " service has exception. ", e);
                }
            }

            log.info(this.getServiceName() + " service end");
        }

        @Override
        protected void onWaitEnd() {
            this.swapRequests();
        }

        @Override
        public String getServiceName() {
            return GroupTransferService.class.getSimpleName();
        }
    }

    /**
     * HA Client 是主从同步 Slave 端的核心实现类
     * HA Client 端实现类
     */
    class HAClient extends ServiceThread {
        // Socket 读缓存区大小
        private static final int READ_MAX_BUFFER_SIZE = 1024 * 1024 * 4;
        // master 地址
        private final AtomicReference<String> masterAddress = new AtomicReference<>();
        // Slave 向 Master 发起主从同步的拉取偏移量
        private final ByteBuffer reportOffset = ByteBuffer.allocate(8);
        // 网络传输通道
        private SocketChannel socketChannel;
        // NIO 事件选择器
        private Selector selector;
        // 上一次写入时间戳
        private long lastWriteTimestamp = System.currentTimeMillis();

        // 反馈 Slave 当前的复制进度，commitlog 文件最大偏移量
        private long currentReportedOffset = 0;
        // 本次己处理读缓存区的指针
        private int dispatchPosition = 0;
        // 读缓存区，大小为 4M
        private ByteBuffer byteBufferRead = ByteBuffer.allocate(READ_MAX_BUFFER_SIZE);
        // 读缓存区备份，与 byteBufferRead 进行交换
        private ByteBuffer byteBufferBackup = ByteBuffer.allocate(READ_MAX_BUFFER_SIZE);

        public HAClient() throws IOException {
            this.selector = RemotingUtil.openSelector();
        }

        public void updateMasterAddress(final String newAddr) {
            String currentAddr = this.masterAddress.get();
            if (currentAddr == null || !currentAddr.equals(newAddr)) {
                this.masterAddress.set(newAddr);
                log.info("update master address, OLD: " + currentAddr + " NEW: " + newAddr);
            }
        }

        /**
         * 判断是否需要向 Master 反馈当前待拉取偏移量，Master 与 Slave 的 HA 心跳发送间隔默认为 5s，可通过配置 haSendHeartbeatInterval 来改变心跳间隔
         * @return
         */
        private boolean isTimeToReportOffset() {
            // 当前时间减去上一次写入时间戳
            long interval =
                HAService.this.defaultMessageStore.getSystemClock().now() - this.lastWriteTimestamp;
            // 大于心跳发送间隔则需要发送心跳
            boolean needHeart = interval > HAService.this.defaultMessageStore.getMessageStoreConfig()
                .getHaSendHeartbeatInterval();

            return needHeart;
        }

        /**
         * 向 Master 服务器反馈拉取偏移量。这里有两重意义，对于 Slave 端来说，是发送下次待拉取消息偏移量，而对于 Master 服务端来说，
         * 既可以认为是 Slave 本次请求拉取的消息偏移量，也可以理解为 Slave 的消息同步 ACK 确认消息
         *
         * 这里 RocketMQ 作者提供了一个基于 NIO 的网络写示例程序：首先先将 ByteBuffer 的 position 设置为 0, limit 设置为待写入字节长度 然后调用 putLong 将待拉取偏移量写入
         * ByteBuffer 中，需要将 ByteBuffer 从写模式切换到读模式，这里的用法是手动将 position 设置为 0, limit 设置为可读长度，其实这里可以直接调用 ByteBuffer 的 flip() 方法来切换
         * ByteBuffer 的读写状态。特别需要留意的是，调用网络通道的 write 方法是在一个 while 循环中反复判断 byteBuffer 是否全部写入到通道中，这是由于 NIO 是一个非阻塞 IO，调用一次 write 方法不一定会将 ByteBuffer 可读字节全部写入
         *
         * @param maxOffset
         * @return
         */
        private boolean reportSlaveMaxOffset(final long maxOffset) {
            this.reportOffset.position(0);
            this.reportOffset.limit(8);
            this.reportOffset.putLong(maxOffset);
            this.reportOffset.position(0);
            this.reportOffset.limit(8);

            // NIO 是一个非阻塞 IO，调用一次 write 方法不一定会将 ByteBuffer 可读字节全部写入
            for (int i = 0; i < 3 && this.reportOffset.hasRemaining(); i++) {
                try {
                    this.socketChannel.write(this.reportOffset);
                } catch (IOException e) {
                    log.error(this.getServiceName()
                        + "reportSlaveMaxOffset this.socketChannel.write exception", e);
                    return false;
                }
            }

            lastWriteTimestamp = HAService.this.defaultMessageStore.getSystemClock().now();
            return !this.reportOffset.hasRemaining();
        }

        private void reallocateByteBuffer() {
            int remain = READ_MAX_BUFFER_SIZE - this.dispatchPosition;
            if (remain > 0) {
                this.byteBufferRead.position(this.dispatchPosition);

                this.byteBufferBackup.position(0);
                this.byteBufferBackup.limit(READ_MAX_BUFFER_SIZE);
                this.byteBufferBackup.put(this.byteBufferRead);
            }

            this.swapByteBuffer();

            this.byteBufferRead.position(remain);
            this.byteBufferRead.limit(READ_MAX_BUFFER_SIZE);
            this.dispatchPosition = 0;
        }

        private void swapByteBuffer() {
            ByteBuffer tmp = this.byteBufferRead;
            this.byteBufferRead = this.byteBufferBackup;
            this.byteBufferBackup = tmp;
        }

        /**
         * 处理网络读请求，即处理从 Master 服务器传回的消息数据。同样 RocketMQ 作者给出了一个处理网络读的 NIO 示例。
         * @return
         */
        private boolean processReadEvent() {
            int readSizeZeroTimes = 0;
            // 循环判断 byteBufferRead 是否还有剩余空间，如果存在剩余空间，则调用 SocketChannel#read(ByteBuffer readByteBuffer)，将通道中的数据读入到读缓存区中
            while (this.byteBufferRead.hasRemaining()) {
                try {
                    int readSize = this.socketChannel.read(this.byteBufferRead);
                    // 如果读取到的字节数大于 0，重置读取到 0 字节的次数，并更新最后一次写入时间戳(lastWriteTimestamp)
                    if (readSize > 0) {
                        readSizeZeroTimes = 0;
                        // 然后调用 dispatchReadRequest 方法将读取到的所有消息全部追加到消息内存映射文件中,然后再次反馈拉取进度给服务器
                        boolean result = this.dispatchReadRequest();
                        if (!result) {
                            log.error("HAClient, dispatchReadRequest error");
                            return false;
                        }
                    } else if (readSize == 0) {
                        // 如果连续3次从网络通道读取到 0 个字节，则结束本次读，返回 true
                        if (++readSizeZeroTimes >= 3) {
                            break;
                        }
                    } else {
                        // 如果读取到的字节数小于 0，则返回 false
                        log.info("HAClient, processReadEvent read socket < 0");
                        return false;
                    }
                } catch (IOException e) {
                    // 如果发生 IO 异常，则返回 false
                    log.info("HAClient, processReadEvent read socket exception", e);
                    return false;
                }
            }

            return true;
        }

        /**
         * 读取 Master 传输的 CommitLog 数据，并返回是否异常，如果读取到数据，写入 CommitLog
         * 异常原因：
         * 1、Master 传输来的数据 offset 不等于 Slave 的 CommitLog 数据最大 offset
         * 2、上报到 Master 进度失败
         * @return
         */
        private boolean dispatchReadRequest() {
            final int msgHeaderSize = 8 + 4; // phyoffset + size
            int readSocketPos = this.byteBufferRead.position();

            while (true) {
                // 读缓存区的当前读位置减去本次己处理读缓存区的指针
                // 读取到请求
                int diff = this.byteBufferRead.position() - this.dispatchPosition;
                if (diff >= msgHeaderSize) {
                    // 读取 masterPhyOffset、bodySize. 使用 dispatchPosition 的原因是：处理数据“粘包”导致数据读取不完整
                    long masterPhyOffset = this.byteBufferRead.getLong(this.dispatchPosition);
                    int bodySize = this.byteBufferRead.getInt(this.dispatchPosition + 8);

                    // 校验 Master 传输来的数据 offset 是否和 Slave 的 CommitLog 数据最大 offset 相同
                    long slavePhyOffset = HAService.this.defaultMessageStore.getMaxPhyOffset();

                    if (slavePhyOffset != 0) {
                        if (slavePhyOffset != masterPhyOffset) {
                            // 数据中断了, Slave 的最大 Offset 匹配不上推送过来的 Offset
                            log.error("master pushed offset not equal the max phy offset in slave, SLAVE: "
                                + slavePhyOffset + " MASTER: " + masterPhyOffset);
                            return false;
                        }
                    }
                    // 读取到消息
                    if (diff >= (msgHeaderSize + bodySize)) {
                        // 写入 CommitLog
                        byte[] bodyData = new byte[bodySize];
                        this.byteBufferRead.position(this.dispatchPosition + msgHeaderSize);
                        this.byteBufferRead.get(bodyData);

                        HAService.this.defaultMessageStore.appendToCommitLog(masterPhyOffset, bodyData);

                        this.byteBufferRead.position(readSocketPos);
                        // 设置处理到的位置
                        this.dispatchPosition += msgHeaderSize + bodySize;

                        // 上报到 Master 进度
                        if (!reportSlaveMaxOffsetPlus()) {
                            return false;
                        }

                        // 继续循环
                        continue;
                    }
                }

                // 空间写满，重新分配空间
                if (!this.byteBufferRead.hasRemaining()) {
                    this.reallocateByteBuffer();
                }

                break;
            }

            return true;
        }

        private boolean reportSlaveMaxOffsetPlus() {
            boolean result = true;
            long currentPhyOffset = HAService.this.defaultMessageStore.getMaxPhyOffset();
            if (currentPhyOffset > this.currentReportedOffset) {
                this.currentReportedOffset = currentPhyOffset;
                result = this.reportSlaveMaxOffset(this.currentReportedOffset);
                if (!result) {
                    this.closeMaster();
                    log.error("HAClient, reportSlaveMaxOffset error, " + this.currentReportedOffset);
                }
            }

            return result;
        }

        /**
         * Slave 服务器连接 Master 服务器
         * 在 Broker 启动时，如果 Broker 角色为 SLAVE 时将读取 Broker 配置文件中的 haMasterAddress 属性并更新 HAClient masterAddress，如果角色为 SLAVE 并且 haMasterAddress 为空
         * 启动并不会报错，但不会执行主从同步复制，该方法最终返回是否成功连接上 Master
         * @return
         * @throws ClosedChannelException
         */
        private boolean connectMaster() throws ClosedChannelException {
            // 如果 socketChannel 为空， 则尝试连接 Master
            if (null == socketChannel) {
                // 如果 master 地址为空，返回 false
                String addr = this.masterAddress.get();
                if (addr != null) {
                    // 如果 master 地址不为空，则建立到 Master 的 TCP 连接，然后注册 OP_READ（网络读事件）
                    SocketAddress socketAddress = RemotingUtil.string2SocketAddress(addr);
                    if (socketAddress != null) {
                        this.socketChannel = RemotingUtil.connect(socketAddress);
                        if (this.socketChannel != null) {
                            // 注册 OP_READ（网络读事件）
                            this.socketChannel.register(this.selector, SelectionKey.OP_READ);
                        }
                    }
                }
                // 初始化 currentReportedOffset 为 commitlog 文件的最大偏移量
                this.currentReportedOffset = HAService.this.defaultMessageStore.getMaxPhyOffset();
                // lastWriteTimestamp 上次写入时间戳为当前时间戳，并返 true
                this.lastWriteTimestamp = System.currentTimeMillis();
            }

            return this.socketChannel != null;
        }

        private void closeMaster() {
            if (null != this.socketChannel) {
                try {

                    SelectionKey sk = this.socketChannel.keyFor(this.selector);
                    if (sk != null) {
                        sk.cancel();
                    }

                    this.socketChannel.close();

                    this.socketChannel = null;
                } catch (IOException e) {
                    log.warn("closeMaster exception. ", e);
                }

                this.lastWriteTimestamp = 0;
                this.dispatchPosition = 0;

                this.byteBufferBackup.position(0);
                this.byteBufferBackup.limit(READ_MAX_BUFFER_SIZE);

                this.byteBufferRead.position(0);
                this.byteBufferRead.limit(READ_MAX_BUFFER_SIZE);
            }
        }

        @Override
        public void run() {
            log.info(this.getServiceName() + " service started");
            // 是否停止循环
            while (!this.isStopped()) {
                try {
                    // 是否连接到 master 服务器
                    if (this.connectMaster()) {
                        // 是否发送心跳，反馈待拉取偏移量
                        if (this.isTimeToReportOffset()) {
                            // 向 Master 服务器反馈拉取偏移量
                            boolean result = this.reportSlaveMaxOffset(this.currentReportedOffset);
                            if (!result) {
                                // 失败关闭与 master 连接
                                this.closeMaster();
                            }
                        }
                        // 进行事件选择，其执行间隔为 1s
                        this.selector.select(1000);

                        boolean ok = this.processReadEvent();
                        if (!ok) {
                            // 处理网络读请求失败，关闭与 Master 连接
                            this.closeMaster();
                        }

                        // 上报最大偏移量
                        if (!reportSlaveMaxOffsetPlus()) {
                            continue;
                        }

                        long interval =
                            HAService.this.getDefaultMessageStore().getSystemClock().now()
                                - this.lastWriteTimestamp;
                        if (interval > HAService.this.getDefaultMessageStore().getMessageStoreConfig()
                            .getHaHousekeepingInterval()) {
                            log.warn("HAClient, housekeeping, found this connection[" + this.masterAddress
                                + "] expired, " + interval);
                            this.closeMaster();
                            log.warn("HAClient, master not response some time, so close connection");
                        }
                    } else {
                        this.waitForRunning(1000 * 5);
                    }
                } catch (Exception e) {
                    log.warn(this.getServiceName() + " service has exception. ", e);
                    this.waitForRunning(1000 * 5);
                }
            }

            log.info(this.getServiceName() + " service end");
        }
        // private void disableWriteFlag() {
        // if (this.socketChannel != null) {
        // SelectionKey sk = this.socketChannel.keyFor(this.selector);
        // if (sk != null) {
        // int ops = sk.interestOps();
        // ops &= ~SelectionKey.OP_WRITE;
        // sk.interestOps(ops);
        // }
        // }
        // }
        // private void enableWriteFlag() {
        // if (this.socketChannel != null) {
        // SelectionKey sk = this.socketChannel.keyFor(this.selector);
        // if (sk != null) {
        // int ops = sk.interestOps();
        // ops |= SelectionKey.OP_WRITE;
        // sk.interestOps(ops);
        // }
        // }
        // }

        @Override
        public String getServiceName() {
            return HAClient.class.getSimpleName();
        }
    }
}
