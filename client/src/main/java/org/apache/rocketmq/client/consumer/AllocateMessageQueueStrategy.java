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

import java.util.List;
import org.apache.rocketmq.common.message.MessageQueue;

/**
 * RocketMQ 默认提供 5 种分配算法
 * 1、AllocateMessageQueueAveragely：平均分配，推荐指数为五颗星。
 * 举例来说，如果现在有 8 个消息消费队列 q1、q2、q3、q4、q5、q6、q7、q8，有 3 个消费者 c1、c2、c3，那么根据该负载算法，消息队列分配如下：
 * c1：q1、q2、q3
 * c2：q4、q5、q6
 * c3：q7、q8
 *
 * 2、AllocateMessageQueueAveragelyByCircle：平均轮询分配，推荐指数为五颗星。
 * 举例来说，如果现在有 8 个消息消费队列 q1、q2、q3、q4、q5、q6、q7、q8，有 3 个消费者 c1、c2、c3，那么根据该负载算法，消息队列分配如下：
 * c1：q1、q4、q7
 * c2：q2、q5、q8
 * c3：q3、q6
 *
 * 3、AllocateMessageQueueConsistentHash：一致性 hash。不推荐使用，因为消息队列负载信息不容易跟踪
 * 4、AllocateMessageQueueByConfig：根据配置，为每一个消费者配置固定的消息队列
 * 5、AllocateMessageQueueByMachineRoom：根据 Broker 部署机房名，对每个消费者负责不同的 Broker 上的队列。
 *
 * 消息负载算法如果没有特殊的要求，尽量使用 AllocateMessageQueueAveragely、AllocateMessageQueueAveragelyByCircle，因为分配算法比较直观.消息队列分配遵循
 * 一个消费者可以分配多个消息队列，但同一个消息队列只会分配给一个消费者，故如果消费者个数大于消息队列数量，则有些消费者无法消费消息
 *
 * Strategy Algorithm for message allocating between consumers
 */
public interface AllocateMessageQueueStrategy {

    /**
     * Allocating by consumer id
     *
     * @param consumerGroup current consumer group
     * @param currentCID current consumer id
     * @param mqAll message queue set in current topic
     * @param cidAll consumer set in current consumer group
     * @return The allocate result of given strategy
     */
    List<MessageQueue> allocate(
        final String consumerGroup,
        final String currentCID,
        final List<MessageQueue> mqAll,
        final List<String> cidAll
    );

    /**
     * Algorithm name
     *
     * @return The strategy name
     */
    String getName();
}
