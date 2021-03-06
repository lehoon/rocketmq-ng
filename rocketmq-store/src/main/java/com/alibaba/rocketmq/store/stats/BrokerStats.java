/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package com.alibaba.rocketmq.store.stats;

import com.alibaba.rocketmq.common.constant.LoggerName;
import com.alibaba.rocketmq.store.DefaultMessageStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * broker消息数据统计
 * @author shijia.wxr
 */
public class BrokerStats {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.BrokerLoggerName);
    
    /**
     * message store
     */
    private final DefaultMessageStore defaultMessageStore;

    /**
     * 昨日接收消息统计
     */
    private volatile long msgPutTotalYesterdayMorning;

    /**
     * 当天接收消息统计
     */
    private volatile long msgPutTotalTodayMorning;

    /**
     * 昨日发送消息统计
     */
    private volatile long msgGetTotalYesterdayMorning;

    /**
     * 今日发送消息统计
     */
    private volatile long msgGetTotalTodayMorning;


    public BrokerStats(DefaultMessageStore defaultMessageStore) {
        this.defaultMessageStore = defaultMessageStore;
    }


    /**
     * 统计数据打印
     */
    public void record() {
        this.msgPutTotalYesterdayMorning = this.msgPutTotalTodayMorning;
        this.msgGetTotalYesterdayMorning = this.msgGetTotalTodayMorning;

        this.msgPutTotalTodayMorning =
                this.defaultMessageStore.getStoreStatsService().getPutMessageTimesTotal();
        this.msgGetTotalTodayMorning =
                this.defaultMessageStore.getStoreStatsService().getGetMessageTransferedMsgCount().get();

        log.info("yesterday put message total: {}", msgPutTotalTodayMorning - msgPutTotalYesterdayMorning);
        log.info("yesterday get message total: {}", msgGetTotalTodayMorning - msgGetTotalYesterdayMorning);
    }


    public long getMsgPutTotalYesterdayMorning() {
        return msgPutTotalYesterdayMorning;
    }


    public void setMsgPutTotalYesterdayMorning(long msgPutTotalYesterdayMorning) {
        this.msgPutTotalYesterdayMorning = msgPutTotalYesterdayMorning;
    }


    public long getMsgPutTotalTodayMorning() {
        return msgPutTotalTodayMorning;
    }


    public void setMsgPutTotalTodayMorning(long msgPutTotalTodayMorning) {
        this.msgPutTotalTodayMorning = msgPutTotalTodayMorning;
    }


    public long getMsgGetTotalYesterdayMorning() {
        return msgGetTotalYesterdayMorning;
    }


    public void setMsgGetTotalYesterdayMorning(long msgGetTotalYesterdayMorning) {
        this.msgGetTotalYesterdayMorning = msgGetTotalYesterdayMorning;
    }


    public long getMsgGetTotalTodayMorning() {
        return msgGetTotalTodayMorning;
    }


    public void setMsgGetTotalTodayMorning(long msgGetTotalTodayMorning) {
        this.msgGetTotalTodayMorning = msgGetTotalTodayMorning;
    }


    public long getMsgPutTotalTodayNow() {
        return this.defaultMessageStore.getStoreStatsService().getPutMessageTimesTotal();
    }


    public long getMsgGetTotalTodayNow() {
        return this.defaultMessageStore.getStoreStatsService().getGetMessageTransferedMsgCount().get();
    }
}
