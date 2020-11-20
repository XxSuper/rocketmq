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
package org.apache.rocketmq.store.index;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.util.List;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.store.MappedFile;

/**
 * 消息消费队列是 RocketMQ 为消息订阅构建的索引文件，提高根据主题与消息队列检索消息的速度，另外 RocketMQ 引入了 Hash 索引机制为消息建立索引，HashMap 的设计包含两个基本点 Hash 槽与 Hash 的链表结构。
 *
 * 消息索引文件，主要存储消息 Key 与 Offset 的对应关系
 *
 * --------------------------------------------------------------------------------------------------------
 * |     IndexHead    |    500w 个 hash 槽          |        2000w 个 Index 条目                             |
 * ---------------------------------------------------------------------------------------------------------
 * |                  | 该 hash 槽对应的 Index 条目  |     4字节    |    8字节     |   4字节    |   4字节       |
 * |                  | 索引每个 hash 槽占 4 字节    |   hashcode  |  phyoffset  |   timedif |  pre index no |
 * -------------------------------------------------------------------------------------------------------
 *
 * IndexFile 总共包含 IndexHeader、Hash 槽、Hash 条目（数据）
 *
 * IndexHeader 头部，包含 40 个字节，记录该 IndexFile 的统计信息，其结构如下：
 * beginTimestamp：该索引文件中包含消息的最小存储时间
 * endTimestamp：该索引文件中包含消息的最大存储时间
 * beginPhyoffset：该索引文件中包含消息的最小物理偏移量（commitlog 文件偏移量）
 * endPhyoffset：该索引文件中包含消息的最大物理偏移量（commitlog 件偏移量）
 * hashslotCount: hashslot 个数，并不是 hash 槽使用的个数，在这里意义不大
 * indexCount: Index 条目列表当前已使用的个数，Index 条目在 Index 条目列表中按顺序存储
 *
 * Hash 槽：IndexFile 默认包含 500 万个 Hash 槽，每个 Hash 槽存储的是落在该 Hash 槽的 hashcode 最新的 Index 的索引，每个 hash 槽占 4 个字节
 *
 * Index 条目列表，默认一个索引文件包含 2000 万个条目，每一个 Index 条目结构如下：
 * 1、hashcode: key 的 hashcode
 * 2、phyoffset 消息对应的物理偏移量
 * 3、timedif：该消息存储时间与第一条消息的时间戳的差值，小于 0 该消息无效
 * 4、preIndexNo ：该条目的前一条记录的 Index 索引，当出现 hash 冲突时，构建的链表结构。
 */
public class IndexFile {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
    private static int hashSlotSize = 4;
    private static int indexSize = 20;
    private static int invalidIndex = 0;
    private final int hashSlotNum;
    private final int indexNum;
    private final MappedFile mappedFile;
    private final FileChannel fileChannel;
    private final MappedByteBuffer mappedByteBuffer;
    // 包含 40 个字节，记录该 IndexFile 的统计信息
    // ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------—--------------
    // |    8字节（该索引文件中包含消息的最小存储时间）    |    8字节（该索引文件中包含消息的最大存储时间） (该索引文件中包含消息的最小物理偏移量(commitlog 文件偏移量))  |   8字节（该索引文件中包含消息的最大物理偏移量(commitlog 文件偏移量)） |     4字节 hashslot数，并不是hash槽使用的个数  |  4字节 Index条目列表当前已使用的个数，Index条目在Index条目列表中按顺序存储   |
    // |             beginTimestamp                |              endTimestamp                               beginPhyoffset                       |                        endPhyoffset                         |              hashslotCount               |                            indexCount                             |
    // |                                           |                                                                                              |                                                             |                                          |                                                                   |
    // ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    private final IndexHeader indexHeader;

    public IndexFile(final String fileName, final int hashSlotNum, final int indexNum,
        final long endPhyOffset, final long endTimestamp) throws IOException {
        int fileTotalSize =
            IndexHeader.INDEX_HEADER_SIZE + (hashSlotNum * hashSlotSize) + (indexNum * indexSize);
        this.mappedFile = new MappedFile(fileName, fileTotalSize);
        this.fileChannel = this.mappedFile.getFileChannel();
        this.mappedByteBuffer = this.mappedFile.getMappedByteBuffer();
        this.hashSlotNum = hashSlotNum;
        this.indexNum = indexNum;

        ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
        this.indexHeader = new IndexHeader(byteBuffer);

        if (endPhyOffset > 0) {
            this.indexHeader.setBeginPhyOffset(endPhyOffset);
            this.indexHeader.setEndPhyOffset(endPhyOffset);
        }

        if (endTimestamp > 0) {
            this.indexHeader.setBeginTimestamp(endTimestamp);
            this.indexHeader.setEndTimestamp(endTimestamp);
        }
    }

    public String getFileName() {
        return this.mappedFile.getFileName();
    }

    public void load() {
        this.indexHeader.load();
    }

    public void flush() {
        long beginTime = System.currentTimeMillis();
        if (this.mappedFile.hold()) {
            this.indexHeader.updateByteBuffer();
            this.mappedByteBuffer.force();
            this.mappedFile.release();
            log.info("flush index file elapsed time(ms) " + (System.currentTimeMillis() - beginTime));
        }
    }

    public boolean isWriteFull() {
        return this.indexHeader.getIndexCount() >= this.indexNum;
    }

    public boolean destroy(final long intervalForcibly) {
        return this.mappedFile.destroy(intervalForcibly);
    }

    /**
     * Hash 冲突链式解决方案的关键实现， Hash 槽中存储的是该 HashCode 所对应的最新的 Index 条目的下标，新的 Index 条目的最后 4 个字节存储该 HashCode 上一个条目的 Index 下标，如果 Hash 槽中存储的值为 0 或大于当前 IndexFile 最大条目数或小于
     * -1，则表示该 Hash 槽当前并没有与之对应的 Index 条目。值得关注的是 IndexFile 条目中存储的不是消息索引 key 而是消息属性 key 的 HashCode ，在根据 key 查找时需要根据消息物理偏移量找到消息进而再验证消息 key 的值，
     * 之所以只存储 HashCode 而不存储具体的 key ，是为了将 Index 条目设计为定长结构，才能方便地检索与定位条目。
     *
     * 将消息索引键与消息偏移量映射关系写入到 IndexFile
     * @param key 消息索引
     * @param phyOffset 消息物理偏移量
     * @param storeTimestamp 消息存储时间
     * @return
     */
    public boolean putKey(final String key, final long phyOffset, final long storeTimestamp) {
        // 如果当前已使用条目大于等于允许最大条目数时，则返回 false，表示当前索引文件已写满
        if (this.indexHeader.getIndexCount() < this.indexNum) {
            // 如果当前索引文件未写满则根据 key 算出 key 的 hashcode
            int keyHash = indexKeyHashMethod(key);
            // keyHash 对 hash 槽数量取余定位到 hashcode 对应的 hash 槽下标
            int slotPos = keyHash % this.hashSlotNum;
            // hashcode 对应的 hash 槽的物理地址为 IndexHeader 头部（40 字节）加上下标乘以每个 hash 槽的大小（ 字节）
            int absSlotPos = IndexHeader.INDEX_HEADER_SIZE + slotPos * hashSlotSize;

            FileLock fileLock = null;

            try {

                // fileLock = this.fileChannel.lock(absSlotPos, hashSlotSize,
                // false);
                // 读取 hash 槽中存储的数据，如果 hash 槽存储的数据小于等于 0 或大于当前索引文件中的已使用索引条目的个数，则 slotValue 设置为 0
                int slotValue = this.mappedByteBuffer.getInt(absSlotPos);
                if (slotValue <= invalidIndex || slotValue > this.indexHeader.getIndexCount()) {
                    slotValue = invalidIndex;
                }

                // 计算待存储消息的时间戳与第一条消息时间戳的差值，并转换成秒
                long timeDiff = storeTimestamp - this.indexHeader.getBeginTimestamp();

                timeDiff = timeDiff / 1000;

                if (this.indexHeader.getBeginTimestamp() <= 0) {
                    timeDiff = 0;
                } else if (timeDiff > Integer.MAX_VALUE) {
                    timeDiff = Integer.MAX_VALUE;
                } else if (timeDiff < 0) {
                    timeDiff = 0;
                }

                // 将条目信息存储在 IndexFile 中
                // 计算新添加条目的起始物理偏移，等于头部字节长度＋ hash 槽数量 * 单个 hash 槽大小（4个字节）＋当前 Index 条目个数 * 单个 Index 条目大小（20 个字节）
                int absIndexPos =
                    IndexHeader.INDEX_HEADER_SIZE + this.hashSlotNum * hashSlotSize
                        + this.indexHeader.getIndexCount() * indexSize;

                // 依次将 hashcode、消息物理偏移量、消息存储时间戳与索引文件时间戳、当前 Hash 槽的值存入 MappedByteBuffer
                // 写入 hashcode（4个字节）
                this.mappedByteBuffer.putInt(absIndexPos, keyHash);
                // 写入消息物理偏移量（8个字节）
                this.mappedByteBuffer.putLong(absIndexPos + 4, phyOffset);
                // 写入消息存储时间戳差值（4个字节）
                this.mappedByteBuffer.putInt(absIndexPos + 4 + 8, (int) timeDiff);
                // 写入当前 Hash 槽的值（该 HashCode 上一个条目的 index 下标）
                this.mappedByteBuffer.putInt(absIndexPos + 4 + 8 + 4, slotValue);

                // 将当前 Index 中包含的条目数量存入 Hash 槽中 ，将覆盖原先 Hash 槽的值
                this.mappedByteBuffer.putInt(absSlotPos, this.indexHeader.getIndexCount());

                // 更新文件索引头信息。如果当前文件只包含 1 个条目，更新 beginPhyoffset 与 beginTimestamp
                if (this.indexHeader.getIndexCount() <= 1) {
                    this.indexHeader.setBeginPhyOffset(phyOffset);
                    this.indexHeader.setBeginTimestamp(storeTimestamp);
                }

                // 更新 endPhyOffset、endTimestamp、当前文件使用索引条目等信息
                // 更新 hashSlotCount、IndexCount、endPhyOffset、endTimestamp
                this.indexHeader.incHashSlotCount();
                this.indexHeader.incIndexCount();
                this.indexHeader.setEndPhyOffset(phyOffset);
                this.indexHeader.setEndTimestamp(storeTimestamp);

                return true;
            } catch (Exception e) {
                log.error("putKey exception, Key: " + key + " KeyHashCode: " + key.hashCode(), e);
            } finally {
                if (fileLock != null) {
                    try {
                        fileLock.release();
                    } catch (IOException e) {
                        log.error("Failed to release the lock", e);
                    }
                }
            }
        } else {
            log.warn("Over index file capacity: index count = " + this.indexHeader.getIndexCount()
                + "; index max num = " + this.indexNum);
        }

        return false;
    }

    public int indexKeyHashMethod(final String key) {
        int keyHash = key.hashCode();
        int keyHashPositive = Math.abs(keyHash);
        if (keyHashPositive < 0)
            keyHashPositive = 0;
        return keyHashPositive;
    }

    public long getBeginTimestamp() {
        return this.indexHeader.getBeginTimestamp();
    }

    public long getEndTimestamp() {
        return this.indexHeader.getEndTimestamp();
    }

    public long getEndPhyOffset() {
        return this.indexHeader.getEndPhyOffset();
    }

    public boolean isTimeMatched(final long begin, final long end) {
        boolean result = begin < this.indexHeader.getBeginTimestamp() && end > this.indexHeader.getEndTimestamp();
        result = result || (begin >= this.indexHeader.getBeginTimestamp() && begin <= this.indexHeader.getEndTimestamp());
        result = result || (end >= this.indexHeader.getBeginTimestamp() && end <= this.indexHeader.getEndTimestamp());
        return result;
    }

    /**
     * 根据索引 key 查找消息
     * @param phyOffsets 查找到的消息物理偏移量
     * @param key 索引 key
     * @param maxNum 本次查找最大消息条数
     * @param begin 开始时间戳
     * @param end 结束时间戳
     * @param lock
     */
    public void selectPhyOffset(final List<Long> phyOffsets, final String key, final int maxNum,
        final long begin, final long end, boolean lock) {
        if (this.mappedFile.hold()) {
            // 首先根据 key 算出 key 的 hashcode
            int keyHash = indexKeyHashMethod(key);
            // keyHash 对 hash 槽数量取余定位到 hashcode 对应的 hash 槽下标
            int slotPos = keyHash % this.hashSlotNum;
            // hashcode 对应的 hash 槽的物理地址为 IndexHeader 头部（ 40 字节）加上下标乘以每个 hash 槽的大小（4 字节）
            int absSlotPos = IndexHeader.INDEX_HEADER_SIZE + slotPos * hashSlotSize;

            FileLock fileLock = null;
            try {
                if (lock) {
                    // fileLock = this.fileChannel.lock(absSlotPos,
                    // hashSlotSize, true);
                }
                // 如果对应的 Hash 槽中存储的数据小于 0 或大于当前索引条目个数则表示 HashCode 没有对应的条目，直接返回
                int slotValue = this.mappedByteBuffer.getInt(absSlotPos);
                // if (fileLock != null) {
                // fileLock.release();
                // fileLock = null;
                // }

                if (slotValue <= invalidIndex || slotValue > this.indexHeader.getIndexCount()
                    || this.indexHeader.getIndexCount() <= 1) {
                } else {
                    // 由于会存在 hash 冲突，根据 slotValue 定位该 hash 槽最新的一个 Item 条目，将存储的物理偏量加入到 phyOffsets 中，然后继续验证 Item 条目中存储的上一个 Index 下标，如果大于等于 1 并且小于最大条目数，则继续查找，否则结束查找
                    for (int nextIndexToRead = slotValue; ; ) {
                        if (phyOffsets.size() >= maxNum) {
                            break;
                        }
                        // 根据 Index 下标定位到条目的起始物理偏移量，然后依次读取 hashcode、物理偏移量、时间差、上一个条目的 Index 下标
                        int absIndexPos =
                            IndexHeader.INDEX_HEADER_SIZE + this.hashSlotNum * hashSlotSize
                                + nextIndexToRead * indexSize;
                        // 读取 4 字节 hashcode
                        int keyHashRead = this.mappedByteBuffer.getInt(absIndexPos);
                        // 读取 8 字节物理偏移量
                        long phyOffsetRead = this.mappedByteBuffer.getLong(absIndexPos + 4);
                        // 读取 4 字节时间差
                        long timeDiff = (long) this.mappedByteBuffer.getInt(absIndexPos + 4 + 8);
                        // 读取 4 字上一个条目的 Index 下标
                        int prevIndexRead = this.mappedByteBuffer.getInt(absIndexPos + 4 + 8 + 4);

                        // 如果存储的时间小于 0，则直接结束
                        if (timeDiff < 0) {
                            break;
                        }

                        timeDiff *= 1000L;

                        // 如果 hashcode 匹配并且消息存储时间介于待查找时间 start、end 之间则将消息物理偏移量加入到 phyOffsets
                        long timeRead = this.indexHeader.getBeginTimestamp() + timeDiff;
                        boolean timeMatched = (timeRead >= begin) && (timeRead <= end);

                        if (keyHash == keyHashRead && timeMatched) {
                            phyOffsets.add(phyOffsetRead);
                        }

                        // 验证条目的前一个 Index 索引，如果索引大于等于 1 并且小于 Index 条目数，则继续查找，否则结束整个查找
                        if (prevIndexRead <= invalidIndex
                            || prevIndexRead > this.indexHeader.getIndexCount()
                            || prevIndexRead == nextIndexToRead || timeRead < begin) {
                            break;
                        }

                        nextIndexToRead = prevIndexRead;
                    }
                }
            } catch (Exception e) {
                log.error("selectPhyOffset exception ", e);
            } finally {
                if (fileLock != null) {
                    try {
                        fileLock.release();
                    } catch (IOException e) {
                        log.error("Failed to release the lock", e);
                    }
                }

                this.mappedFile.release();
            }
        }
    }
}
