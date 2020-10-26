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

package org.apache.rocketmq.client.latency;

import org.apache.rocketmq.client.impl.producer.TopicPublishInfo;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.common.message.MessageQueue;

/**
 * 消息失败策略，延迟实现的门面类
 * MQFaultStrategy 有两个作用：
 * 1、用于挑选 MessageQueue
 * 2、如果开启了故障延迟机制，则在消息发送失败的时候，会生成一个 FaultItem，标记 broker 故障
 */
public class MQFaultStrategy {
    private final static InternalLogger log = ClientLogger.getLog();
    private final LatencyFaultTolerance<String> latencyFaultTolerance = new LatencyFaultToleranceImpl();

    private boolean sendLatencyFaultEnable = false;

    // 根据 currentLatency 本次消息发送延迟，从 latencyMax 尾部向前找到第一个比 currentLatency 小的索引 index，如果没有找到，返回 0，
    // 然后根据这个索引从 notAvailableDuration 数组中取出对应的时间间隔，在这个时长内， Broker 将设置为不可用
    private long[] latencyMax = {50L, 100L, 550L, 1000L, 2000L, 3000L, 15000L};
    private long[] notAvailableDuration = {0L, 0L, 30000L, 60000L, 120000L, 180000L, 600000L};

    public long[] getNotAvailableDuration() {
        return notAvailableDuration;
    }

    public void setNotAvailableDuration(final long[] notAvailableDuration) {
        this.notAvailableDuration = notAvailableDuration;
    }

    public long[] getLatencyMax() {
        return latencyMax;
    }

    public void setLatencyMax(final long[] latencyMax) {
        this.latencyMax = latencyMax;
    }

    public boolean isSendLatencyFaultEnable() {
        return sendLatencyFaultEnable;
    }

    public void setSendLatencyFaultEnable(final boolean sendLatencyFaultEnable) {
        this.sendLatencyFaultEnable = sendLatencyFaultEnable;
    }

    public MessageQueue selectOneMessageQueue(final TopicPublishInfo tpInfo, final String lastBrokerName) {
        // sendLatencyFaultEnable=false，默认不启用 Broker 故障延迟机制，sendLatencyFaultEnable=true，启用 Broker 障延迟机制
        if (this.sendLatencyFaultEnable) {
            try {
                // 故障延迟机制，Broker 宕机期间，如果一次消息发送失败后，可以将该 Broker 暂时排除在消息队列的选择范围中
                int index = tpInfo.getSendWhichQueue().getAndIncrement();
                for (int i = 0; i < tpInfo.getMessageQueueList().size(); i++) {
                    // 根据对消息队列进行轮询获取一个消息队列
                    int pos = Math.abs(index++) % tpInfo.getMessageQueueList().size();
                    if (pos < 0)
                        pos = 0;
                    MessageQueue mq = tpInfo.getMessageQueueList().get(pos);
                    // 验证该消息队列是否可用
                    if (latencyFaultTolerance.isAvailable(mq.getBrokerName())) {
                        // 1、非失败重试（首次调用） 2、失败重试，如果选择的队列和上次重试的一样。则返回
                        if (null == lastBrokerName || mq.getBrokerName().equals(lastBrokerName))
                            return mq;
                    }
                }

                // 尝试从规避的 Broker 中选择一个可用的 Broker ，如果没有找到，将返回 null
                final String notBestBroker = latencyFaultTolerance.pickOneAtLeast();
                // 写队列个数
                int writeQueueNums = tpInfo.getQueueIdByBroker(notBestBroker);
                // 有可写队列
                if (writeQueueNums > 0) {
                    // 往后取一个
                    final MessageQueue mq = tpInfo.selectOneMessageQueue();
                    if (notBestBroker != null) {
                        mq.setBrokerName(notBestBroker);
                        mq.setQueueId(tpInfo.getSendWhichQueue().getAndIncrement() % writeQueueNums);
                    }
                    return mq;
                } else {
                    // 如果返回的 MessageQueue 可用，移除 latencyFaultTolerance 关于该 topic 条目，表明该 Broker 故障已经恢复
                    latencyFaultTolerance.remove(notBestBroker);
                }
            } catch (Exception e) {
                log.error("Error occurred when selecting message queue", e);
            }
            // 轮训选择一个消息队列
            return tpInfo.selectOneMessageQueue();
        }
        //获得 lastBrokerName 对应的一个消息队列，不考虑该队列的可用性
        return tpInfo.selectOneMessageQueue(lastBrokerName);
    }

    /**
     *
     * @param brokerName
     * @param currentLatency 本次消息发送时延
     * @param isolation
     */
    public void updateFaultItem(final String brokerName, final long currentLatency, boolean isolation) {
        // 如果 isolation 为 true ，则使用 30s 作为 computeNotAvailableDuration 方法的参数。如果 isolation 为 false ，则使用本次消息发送时延作为 computeNotAvailableDuration 方法的参数
        if (this.sendLatencyFaultEnable) {
            // 计算因本次消息发送故障需要 Broker 规避的时长，也就是接下来多久的时间内该 Broker 将不参与消息发送队列负载
            long duration = computeNotAvailableDuration(isolation ? 30000 : currentLatency);
            // 调用 LatencyFaultTolerance 的 updateFaultltem
            this.latencyFaultTolerance.updateFaultItem(brokerName, currentLatency, duration);
        }
    }

    private long computeNotAvailableDuration(final long currentLatency) {
        // 从 latencyMax 数组尾部开始寻找，找到第一个比 currentLatency 小的下标，然后从 notAvailableDuration 数组中获取需要规避的时长
        for (int i = latencyMax.length - 1; i >= 0; i--) {
            if (currentLatency >= latencyMax[i])
                return this.notAvailableDuration[i];
        }

        return 0;
    }
}
