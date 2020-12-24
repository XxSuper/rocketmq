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

import java.nio.ByteBuffer;
import java.util.Map;

/**
 * RocketMQ 支持表达式过滤与类过滤两种模式，其中表达式又分为 TAG 和 SQL92。类过滤模式允许提交一个过滤类到 FilterServer，消息消费者从 FilterServer 拉取消息，消息经过 FilterServer 时会执行过滤逻辑。
 * 表达式模式分为 TAG 和 SQL92 表达式，SQL92 表达式以消息属性过滤上下文，实现 SQL 条件过滤表达式而 TAG 模式就是简单为消息定义标签，根据消息属性 tag 进行匹配.
 * RocketMQ 消息过滤方式不同于其它消息中间件，是在订阅时做过滤
 *
 * 消息发送者在消息发送时如果设置了消息的 tags 属性，存储在消息属性中，先存储在 CommitLog 文件中，然后转发到消息消费队列，消息消费队列会用 8 个字节存储消息 tag 的 hashcode，
 * 之所以不直接存储 tag 字符串，是因为将 consumeQueue 设计为定长结构，加快消息消费的加载性能。在 Broker 端拉取消息时，遍历 ConsumeQueue，只对比消息 tag 的 hashcode。
 * 如果匹配则返回，否则忽略该消息。Consume 在收到消息后，同样需要先对消息进行过滤，只是此时比较的是消息 tag 的值而不再是 hashcode
 */
public interface MessageFilter {
    /**
     * match by tags code or filter bit map which is calculated when message received
     * and stored in consume queue ext.
     * 根据 consumeQueue 判断消息是否匹配
     *
     * @param tagsCode tagsCode. 消息 tag 的 hashcode
     * @param cqExtUnit extend unit of consume queue. consumeQueue 条目扩展属性
     */
    boolean isMatchedByConsumeQueue(final Long tagsCode,
        final ConsumeQueueExt.CqExtUnit cqExtUnit);

    /**
     * match by message content which are stored in commit log.
     * <br>{@code msgBuffer} and {@code properties} are not all null.If invoked in store,
     * {@code properties} is null;If invoked in {@code PullRequestHoldService}, {@code msgBuffer} is null.
     *
     * 根据存储在 commitlog 文件中的内容判断消息是否匹配
     *
     * @param msgBuffer message buffer in commit log, may be null if not invoked in store. 消息内容，如果为空，该方法返回 true
     * @param properties message properties, should decode from buffer if null by yourself. 消息属性，主要用于表达式 SQL92 过滤模式
     */
    boolean isMatchedByCommitLog(final ByteBuffer msgBuffer,
        final Map<String, String> properties);
}
