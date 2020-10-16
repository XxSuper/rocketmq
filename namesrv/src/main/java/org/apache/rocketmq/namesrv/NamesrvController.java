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
package org.apache.rocketmq.namesrv;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.common.Configuration;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.common.namesrv.NamesrvConfig;
import org.apache.rocketmq.namesrv.kvconfig.KVConfigManager;
import org.apache.rocketmq.namesrv.processor.ClusterTestRequestProcessor;
import org.apache.rocketmq.namesrv.processor.DefaultRequestProcessor;
import org.apache.rocketmq.namesrv.routeinfo.BrokerHousekeepingService;
import org.apache.rocketmq.namesrv.routeinfo.RouteInfoManager;
import org.apache.rocketmq.remoting.RemotingServer;
import org.apache.rocketmq.remoting.common.TlsMode;
import org.apache.rocketmq.remoting.netty.NettyRemotingServer;
import org.apache.rocketmq.remoting.netty.NettyServerConfig;
import org.apache.rocketmq.remoting.netty.TlsSystemConfig;
import org.apache.rocketmq.srvutil.FileWatchService;

/**
 * NameServer 主要作用是为消息生产者和消息消费者提供关于主题 Topic 的路由信息，那么 NameServer 需要存储路由的基础信息，还要能够管理 Broker 节点，包括路由注册、路由删除等功能
 *
 * Broker 消息服务器在启动时向所有 NameServer 注册，消息生产者（Producer）在发送消息之前先从 NameServer 获取 Broker 服务器地址列表，然后根据负载算法从列表中选择一台消息服务器进行消息发送
 * NameServer 与每台 Broker 服务器保持长连接，并间隔 30s 检查 Broker 是否存活，如果检测到 Broker 宕机，则从路由注册表中将其移除。但是路由变化不会马上通知消息生产者，
 * 为什么要这样设计呢？这是为了降低 NameServer 实现的复杂性，在消息发送端提供容错机制来保证消息发送的高可用性，
 * NameServer 本身的高可用可通过部署多台 NameServer 服务器来实现，但彼此之间互不通信，也就是 NameServer 服务器之间在某一时刻的数据并不会完全相同，但这对消息发送不会造成任何影响，
 * 这也是 RocketMQ NameServer 设计的一个亮点， RocketMQ NameServer 设计追求简单高效
 */
public class NamesrvController {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.NAMESRV_LOGGER_NAME);

    private final NamesrvConfig namesrvConfig;

    private final NettyServerConfig nettyServerConfig;

    private final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryImpl(
        "NSScheduledThread"));
    private final KVConfigManager kvConfigManager;
    private final RouteInfoManager routeInfoManager;

    private RemotingServer remotingServer;

    private BrokerHousekeepingService brokerHousekeepingService;

    private ExecutorService remotingExecutor;

    private Configuration configuration;
    private FileWatchService fileWatchService;

    public NamesrvController(NamesrvConfig namesrvConfig, NettyServerConfig nettyServerConfig) {
        // nameserv 参数配置
        this.namesrvConfig = namesrvConfig;
        // netty 的参数配置
        this.nettyServerConfig = nettyServerConfig;
        this.kvConfigManager = new KVConfigManager(this);
        // 初始化 RouteInfoManager
        this.routeInfoManager = new RouteInfoManager();
        // 监听客户端连接(Channel)的变化，通知 RouteInfoManager 检查 broker 是否有变化
        this.brokerHousekeepingService = new BrokerHousekeepingService(this);
        this.configuration = new Configuration(
            log,
            this.namesrvConfig, this.nettyServerConfig
        );
        // Nameserv 的配置参数会保存到磁盘文件中
        this.configuration.setStorePathFromConfig(this.namesrvConfig, "configStorePath");
    }

    /**
     * 启动过程中新建了一个 Controller 的实例后会调用它的 initialize() 方法
     * @return
     */
    public boolean initialize() {
        // 加载 KV 配置，初始化 KVConfigManager
        this.kvConfigManager.load();
        // 创建 NettyServer 网络处理对象，初始化 netty server（初始化 nameserv 的 Server，用来接收客户端请求）
        this.remotingServer = new NettyRemotingServer(this.nettyServerConfig, this.brokerHousekeepingService);

        // 客户端请求处理的线程池
        this.remotingExecutor =
            Executors.newFixedThreadPool(nettyServerConfig.getServerWorkerThreads(), new ThreadFactoryImpl("RemotingExecutorThread_"));

        // 注册 DefaultRequestProcessor，所有的客户端请求都会转给这个 Processor 来处理（所有的客户端请求都会转到 DefaultRequestProcessor 来处理）
        this.registerProcessor();

        // 启动定时调度，每10秒钟扫描所有 Broker 从 brokerLiveTable 缓存，检查存活状态，移除处于不激活状态的 Broker。
        // Broker 每隔 30s 向 NameServer 发送一个心跳包，心跳包中包含 BrokerId、Broker 地址、Broker 名称、Broker 所属集群名称、Broker 关联的 FilterServer 列表。但是如果 Broker 宕机，NameServer 无法收到心跳包，
        // 此时 NameServer 如何来剔除这些失效的 Broker 呢？
        // NameServer 会每隔 1Os 扫描 brokerLiveTable 状态表，如果 BrokerLive 的 lastUpdateTimestamp 的时间戳距当前时间超过 120s，则认为 Broker 失效，移除该 Broker，
        // 关闭与 Broker 的连接，并同时更新 topicQueueTable、brokerAddrTable、brokerLiveTable、filterServerTable
        // RocktMQ 有两个触发点来触发路由删除。
        // 1) NameServer 定时扫描 brokerLiveTable 检测上次心跳包与当前系统时间的时间差，如果时间戳大于 120s ，则需要移除 Broker 信息
        // 2) Broker 在正常被关闭的情况下，会执行 unregisterBroker 指令
        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {

            @Override
            public void run() {
                NamesrvController.this.routeInfoManager.scanNotActiveBroker();
            }
        }, 5, 10, TimeUnit.SECONDS);

        // NameServer 每隔 10 分钟打印一次 KV配置（日志打印的调度器，定时打印 kvConfigManager 的内容）
        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {

            @Override
            public void run() {
                NamesrvController.this.kvConfigManager.printAllPeriodically();
            }
        }, 1, 10, TimeUnit.MINUTES);

        // 监听 ssl 证书文件变化，
        if (TlsSystemConfig.tlsMode != TlsMode.DISABLED) {
            // Register a listener to reload SslContext
            try {
                fileWatchService = new FileWatchService(
                    new String[] {
                        TlsSystemConfig.tlsServerCertPath,
                        TlsSystemConfig.tlsServerKeyPath,
                        TlsSystemConfig.tlsServerTrustCertPath
                    },
                    new FileWatchService.Listener() {
                        boolean certChanged, keyChanged = false;
                        @Override
                        public void onChanged(String path) {
                            if (path.equals(TlsSystemConfig.tlsServerTrustCertPath)) {
                                log.info("The trust certificate changed, reload the ssl context");
                                reloadServerSslContext();
                            }
                            if (path.equals(TlsSystemConfig.tlsServerCertPath)) {
                                certChanged = true;
                            }
                            if (path.equals(TlsSystemConfig.tlsServerKeyPath)) {
                                keyChanged = true;
                            }
                            if (certChanged && keyChanged) {
                                log.info("The certificate and private key changed, reload the ssl context");
                                certChanged = keyChanged = false;
                                reloadServerSslContext();
                            }
                        }
                        private void reloadServerSslContext() {
                            ((NettyRemotingServer) remotingServer).loadSslContext();
                        }
                    });
            } catch (Exception e) {
                log.warn("FileWatchService created error, can't load the certificate dynamically");
            }
        }

        return true;
    }

    private void registerProcessor() {
        if (namesrvConfig.isClusterTest()) {

            this.remotingServer.registerDefaultProcessor(new ClusterTestRequestProcessor(this, namesrvConfig.getProductEnvName()),
                this.remotingExecutor);
        } else {
            // DefaultRequestProcessor 网络处理器解析请求类型，如果请求类型为 RequestCode.REGISTER_BROKER，则请求最终转发到 RoutelnfoManager#registerBroker
            this.remotingServer.registerDefaultProcessor(new DefaultRequestProcessor(this), this.remotingExecutor);
        }
    }

    public void start() throws Exception {
        this.remotingServer.start();

        if (this.fileWatchService != null) {
            this.fileWatchService.start();
        }
    }

    public void shutdown() {
        this.remotingServer.shutdown();
        this.remotingExecutor.shutdown();
        this.scheduledExecutorService.shutdown();

        if (this.fileWatchService != null) {
            this.fileWatchService.shutdown();
        }
    }

    public NamesrvConfig getNamesrvConfig() {
        return namesrvConfig;
    }

    public NettyServerConfig getNettyServerConfig() {
        return nettyServerConfig;
    }

    public KVConfigManager getKvConfigManager() {
        return kvConfigManager;
    }

    public RouteInfoManager getRouteInfoManager() {
        return routeInfoManager;
    }

    public RemotingServer getRemotingServer() {
        return remotingServer;
    }

    public void setRemotingServer(RemotingServer remotingServer) {
        this.remotingServer = remotingServer;
    }

    public Configuration getConfiguration() {
        return configuration;
    }
}
