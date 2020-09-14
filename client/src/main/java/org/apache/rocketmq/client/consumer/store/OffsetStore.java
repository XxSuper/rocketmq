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
package org.apache.rocketmq.client.consumer.store;

import java.util.Map;
import java.util.Set;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.exception.RemotingException;

/**
 * 消费进度管理
 * 消息消费者在消费一批消息后，需要记录该批消息已经消费完毕，否则当消费者重新启动时又得从消息消费队列的开始消费，这显然是不能接受的。一
 * 次消息消费后会从 ProcessQueue 处理队列中移除该批消息，返回 ProceeQueue 最小偏移量并存入消息进度表中，那消息进度文件存储在哪合适呢？
 * 广播模式：同一个消费组的所有消息消费者都需要消费主题下的所有消息，也就是同组内的消费者的消息消费行为是对立的，互相不影响，故消息进度需要独立存储，最理想的存储地方应该是与消费者绑定
 * 集群模式：同一个消费组内的所有消息消费者共享消息主题下的所有消息， 一条消息（同一个消息消费队列）在同一时间只会被消费组内的一个消费者消费，并且随着消费队列的动态变化重新负载，
 * 所以消费进度需要保存在一个每个消费者都能访问到的地方。
 *
 * 消息消费进度的存储，广播模式与消费组无关，集群模式下以主题与消费组为键保存该主题所有队列的消费进度。
 * 1) 消费者线程池每处理完一个消息消费任务 (ConsumeRequest) 时会从 ProceeQueue 中移除本批消费的消息，并返回 ProceeQueue 的最小的偏移量，用该偏移量更新消息队列消费进度，也就是说更新消费进度与消费任务中的消息没什么关系
 * 2) 触发消息消费进度更新的另外一个是在进行消息负载时，如果消息消费队列被分配给其他消费者时，此时会将该 ProcessQueue 状态设置为 drop，持久化该消息队列的消费进度，并从内存中移除
 *
 * Offset store interface
 */
public interface OffsetStore {
    /**
     * 从消息进度存储文件加载消息进度到内存
     * Load
     */
    void load() throws MQClientException;

    /**
     * 更新内存中的消息消费进度
     * @param mq 消息消费队列
     * @param offset 消息消费偏移量
     * @param increaseOnly  true 表示 offset 必须大于内存中当前的消费偏移量才更新
     * Update the offset,store it in memory
     */
    void updateOffset(final MessageQueue mq, final long offset, final boolean increaseOnly);

    /**
     * 读取消息消费进度
     * @param mq 消息消费队列
     * @param type 读取方式，可选值 READ_FROM_MEMORY：从内存中；READ_FROM_STORE：从磁盘中；MEMORY_FIRST_THEN_STORE：先从内存读取再从磁盘读取
     * Get offset from local storage
     *
     * @return The fetched offset
     */
    long readOffset(final MessageQueue mq, final ReadOffsetType type);

    /**
     * 持久化指定消息队列消费进度到磁盘
     * @param mqs 消息消费队列集合
     * Persist all offsets,may be in local storage or remote name server
     */
    void persistAll(final Set<MessageQueue> mqs);

    /**
     * 持久化指定消息队列消费进度到磁盘
     * @param mq 消息消费队列
     * Persist the offset,may be in local storage or remote name server
     */
    void persist(final MessageQueue mq);

    /**
     * 将消息队列的消息消费进度从内存中移除
     * Remove offset
     */
    void removeOffset(MessageQueue mq);

    /**
     * 克隆该主题下所有消息队列的消息消费进度
     * @return The cloned offset table of given topic
     */
    Map<MessageQueue, Long> cloneOffsetTable(String topic);

    /**
     * 更新存储在 Brokder 端的消息消费进度，使用集群模式
     * @param mq
     * @param offset
     * @param isOneway
     */
    void updateConsumeOffsetToBroker(MessageQueue mq, long offset, boolean isOneway) throws RemotingException,
        MQBrokerException, InterruptedException, MQClientException;
}
