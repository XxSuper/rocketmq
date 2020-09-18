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
package org.apache.rocketmq.client.impl.consumer;

import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.common.utils.ThreadUtils;

/**
 * 消息拉取服务线程，PullMessageService 继承 ServiceThread，是服务线程，通过 run() 方法启动。
 * PullMessageService 只有在拿到 pullRequest 对象时才会执行拉取任务
 *
 * PullMessageService 负责对消息队列进行消息拉取，从远端服务器拉取消息后将消息存入 ProcessQueue 消息队列处理队列中，然后调用 ConsumeMessageeServer#submitConsumeRequest 方法进行消息消费
 * 使用线程池来消费消息确保了消息拉取与消息消费的解祸。RocketMQ 使用 ConsumeMessageService 来实现消息消费的处理逻辑，RocketMQ 支持顺序消费与并发消费
 */
public class PullMessageService extends ServiceThread {
    private final InternalLogger log = ClientLogger.getLog();
    private final LinkedBlockingQueue<PullRequest> pullRequestQueue = new LinkedBlockingQueue<PullRequest>();
    private final MQClientInstance mQClientFactory;
    private final ScheduledExecutorService scheduledExecutorService = Executors
        .newSingleThreadScheduledExecutor(new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                return new Thread(r, "PullMessageServiceScheduledThread");
            }
        });

    public PullMessageService(MQClientInstance mQClientFactory) {
        this.mQClientFactory = mQClientFactory;
    }

    //
    public void executePullRequestLater(final PullRequest pullRequest, final long timeDelay) {
        if (!isStopped()) {
            this.scheduledExecutorService.schedule(new Runnable() {
                @Override
                public void run() {
                    PullMessageService.this.executePullRequestImmediately(pullRequest);
                }
            }, timeDelay, TimeUnit.MILLISECONDS);
        } else {
            log.warn("PullMessageServiceScheduledThread has shutdown");
        }
    }

    public void executePullRequestImmediately(final PullRequest pullRequest) {
        try {
            this.pullRequestQueue.put(pullRequest);
        } catch (InterruptedException e) {
            log.error("executePullRequestImmediately pullRequestQueue.put", e);
        }
    }

    public void executeTaskLater(final Runnable r, final long timeDelay) {
        if (!isStopped()) {
            this.scheduledExecutorService.schedule(r, timeDelay, TimeUnit.MILLISECONDS);
        } else {
            log.warn("PullMessageServiceScheduledThread has shutdown");
        }
    }

    public ScheduledExecutorService getScheduledExecutorService() {
        return scheduledExecutorService;
    }

    /**
     * 消息拉取线程 PullMessageService 默认使用异步方式从服务器拉取消息，消息消费端会通过 PullAPIWrapper 从响应结果解析出拉取到的消息。如果消息
     * 过滤模式为 TAG 模式，并且订阅 TAG 集合不为空，则对消息的 tag 进行判断，如果集合中包含消息的 TAG 则返回给消费者消费，否则跳过
     * @param pullRequest
     */
    private void pullMessage(final PullRequest pullRequest) {
        // 根据消费组名，从 MQClientInstance 中获取消费者内部实现 MQConsumerInner
        final MQConsumerInner consumer = this.mQClientFactory.selectConsumer(pullRequest.getConsumerGroup());
        if (consumer != null) {
            // 这里将 consumer 强制转换为 DefaultMQPushConsumerlmpl，也就是 PullMessageService，该线程只为 PUSH 模式服务，那拉模式如何拉取消息呢？
            // PULL 模式 RocketMQ 需要提供拉取消息 API 即可， 具体由应用程序显示调用拉取 API
            DefaultMQPushConsumerImpl impl = (DefaultMQPushConsumerImpl) consumer;
            impl.pullMessage(pullRequest);
        } else {
            log.warn("No matched consumer for the PullRequest {}, drop it", pullRequest);
        }
    }

    /**
     * PullMessageService 只有在拿到 pullRequest 对象时才会执行拉取任务
     */
    @Override
    public void run() {
        log.info(this.getServiceName() + " service started");

        // 这是一种通用的设计技巧， stopped 声明为 volatile， 每执行一次业务逻辑检测一下其运行状态，可以通过其他线程将 stopped 设置为 true 从而停止该线程
        while (!this.isStopped()) {
            try {
                // 从 pullRequestQueue 中获取 PullRequest 消息拉取任务，如果 pullRequestQueue 为空，则线程将阻塞，直到有拉取任务被放入
                PullRequest pullRequest = this.pullRequestQueue.take();
                // 调用 pullMessage 方法进行消息拉取
                // PullMessageService 提供延迟添加与立即添加2种方式将 PullRequest 放入到 pullRequestQueue 中，PullRequest 是在 PullMessageService#executePullRequestLater#executePullRequestImmediately 中放入的
                // 通过跟踪发现，主要有两个地方会调用，一个是在 RocketMQ 根据 pullRequest 拉取任务执行完一次消息拉取任务后，又将 pullRequest 对象放入到 pullRequestQueue ，
                // 第二个是在 Rebalancelmpl 中创建。 Rebalancelmpl 消息队列负载机制，也是 PullRequest 对象真正创建的地方。
                this.pullMessage(pullRequest);
            } catch (InterruptedException ignored) {
            } catch (Exception e) {
                log.error("Pull Message Service Run Method exception", e);
            }
        }

        log.info(this.getServiceName() + " service end");
    }

    @Override
    public void shutdown(boolean interrupt) {
        super.shutdown(interrupt);
        ThreadUtils.shutdownGracefully(this.scheduledExecutorService, 1000, TimeUnit.MILLISECONDS);
    }

    @Override
    public String getServiceName() {
        return PullMessageService.class.getSimpleName();
    }

}
