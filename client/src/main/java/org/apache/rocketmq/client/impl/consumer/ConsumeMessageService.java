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

import java.util.List;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.body.ConsumeMessageDirectlyResult;

/**
 * PullMessageService 负责对消息队列进行消息拉取，从远端服务器拉取消息后将消息存入 ProcessQueue 消息队列处理队列中，然后调用 ConsumeMessageService#submitConsumeRequest 方法进行消息消费，使用线程池来消费消息，
 * 确保了消息拉取与消息消费的解耦。RocketMQ 使用 ConsumeMessageService 来实现消息消费的处理逻辑，RocketMQ 支持顺序消费与并发消费。
 *
 * 从服务器拉取到消息后回调 PullCallBack 回调方法后，先将消息放入到 ProcessQueue 中，然后把消息提交到消费线程池中执行，也就是调用 ConsumeMessageService#submitConsumeRequest 开始进入到消息消费
 */
public interface ConsumeMessageService {
    void start();

    void shutdown();

    void updateCorePoolSize(int corePoolSize);

    void incCorePoolSize();

    void decCorePoolSize();

    int getCorePoolSize();

    /**
     * 直接消费消息，主要用于通过管理命令收到消费消息
     * @param msg 消息
     * @param brokerName Broker 名称
     * @return
     */
    ConsumeMessageDirectlyResult consumeMessageDirectly(final MessageExt msg, final String brokerName);

    /**
     * 提交消息消费
     * @param msgs 消息列表，默认一次从服务器最多拉取 32 条
     * @param processQueue 消息处理队列
     * @param messageQueue 消息所属消费队列
     * @param dispathToConsume 是否转发到消费线程池，并发消费时忽略该参数
     */
    void submitConsumeRequest(
        final List<MessageExt> msgs,
        final ProcessQueue processQueue,
        final MessageQueue messageQueue,
        final boolean dispathToConsume);
}
