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
package org.apache.rocketmq.client.consumer;

import java.util.Set;
import org.apache.rocketmq.client.MQAdmin;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.exception.RemotingException;

/**
 * 消息消费以组的模式开展，一个消费组内可以包含多个消费者，每一个消费组可订阅多个主题，消费组之间有集群模式与广播模式两种消费模式。集群模式，主题下的同一条
 * 消息只允许被其中一个消费者消费。广播模式，主题下的同一条消息将被集群内的所有消费者消费。消息服务器与消费者之间的消息传送也有两种方式：推模式、拉模式。所
 * 谓的拉模式，是消费端主动发起拉消息请求，而推模式是消息到达消息服务器后，推送给消息消费者。RocketMQ 消息推模式的实现基于拉模式，在拉模式上包装一层，一个拉取任
 * 务完成后开始下一个拉取任务。
 * 集群模式下，多个消费者如何对消息队列进行负载呢？消息队列负载机制遵循一个通用的思想：一个消息队列同一时间，只允许被一个消费者消费，一个消费者可以消费多个消息队列。
 * RocketMQ 支持局部顺序消息消费，也就是保证消息队列上的消息顺序消费。不支持全局顺序消费，如果要实现，某一主题的全局顺序消息消费，可以将该主题的队列数设置为 1，牺牲可用性。
 * RocketMQ 支持两种消息过滤模式：表达式（ TAG、SQL92 ）与类过滤模式。
 * 消息拉模式，主要是由客户端手动调用消息拉取 API，而消息推模式是消息服务器主动将消息推送到消息消费端
 *
 * Message queue consumer interface
 */
public interface MQConsumer extends MQAdmin {
    /**
     * 发送消息 ACK 确认
     * If consuming failure,message will be send back to the brokers,and delay consuming some time
     */
    @Deprecated
    void sendMessageBack(final MessageExt msg, final int delayLevel) throws RemotingException,
        MQBrokerException, InterruptedException, MQClientException;

    /**
     * 发送消息 ACK 确认
     * If consuming failure,message will be send back to the broker,and delay consuming some time
     * @param msg 消息
     * @param delayLevel 消息延迟级别
     * @param brokerName 消息服务器名称
     */
    void sendMessageBack(final MessageExt msg, final int delayLevel, final String brokerName)
        throws RemotingException, MQBrokerException, InterruptedException, MQClientException;

    /**
     * Fetch message queues from consumer cache according to the topic
     * 获取消费者对主题 topic 分配了哪些消息队列
     *
     * @param topic message topic 主题名称
     * @return queue set
     */
    Set<MessageQueue> fetchSubscribeMessageQueues(final String topic) throws MQClientException;
}
