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
package com.alibaba.rocketmq.common;

import com.alibaba.rocketmq.common.annotation.ImportantField;
import com.alibaba.rocketmq.common.constant.PermName;
import com.alibaba.rocketmq.remoting.common.RemotingUtil;

import java.net.InetAddress;
import java.net.UnknownHostException;


/**
 * @author shijia.wxr
 */
public class BrokerConfig {
	/**
	 * rocketmq主目录
	 */
    private String rocketmqHome = System.getProperty(MixAll.ROCKETMQ_HOME_PROPERTY, System.getenv(MixAll.ROCKETMQ_HOME_ENV));
    /**
     * nameserver地址
     */
    @ImportantField
    private String namesrvAddr = System.getProperty(MixAll.NAMESRV_ADDR_PROPERTY, System.getenv(MixAll.NAMESRV_ADDR_ENV));
    /**
     * broker ip地址1
     * 获取主机地址
     */
    @ImportantField
    private String brokerIP1 = RemotingUtil.getLocalAddress();
    /**
     * broker ip地址2
     * 获取主机地址
     */
    private String brokerIP2 = RemotingUtil.getLocalAddress();
    /**
     * broker名称
     * 默认主机名称,主机名称获取不到的话  设置为: “DEFAULT_BROKER”
     */
    @ImportantField
    private String brokerName = localHostName();
 
    /**
     * broker 集群名称
     */
    @ImportantField
    private String brokerClusterName = "DefaultCluster";
    
    /**
     * broker编号, 默认为0 
     */
    @ImportantField
    private long brokerId = MixAll.MASTER_ID;

    /**
     * 默认权限  读+写
     */
    private int brokerPermission = PermName.PERM_READ | PermName.PERM_WRITE;

    /**
     * 默认主题队列数量为8
     */
    private int defaultTopicQueueNums = 8;

    /**
     * 自动创建主题开启标志默认为true
     */
    @ImportantField
    private boolean autoCreateTopicEnable = true;

    /**
     * 集群主题开启标志
     * 主要影响topic的读写标志,如果为true, 则权限为读+写
     * 参考：com.alibaba.rocketmq.broker.topic.TopicConfigManager line 118
     */
    private boolean clusterTopicEnable = true;

    /**
     * broker主题开启标志
     * 主要影响topic的读写标志,如果为true, 则权限为读+写
     * 参考：com.alibaba.rocketmq.broker.topic.TopicConfigManager line 118
     */
    private boolean brokerTopicEnable = true;
    
    /**
     * 自动创建订阅组
     * 参考：com.alibaba.rocketmq.broker.subscription.findSubscriptionGroupConfig
     */
    @ImportantField
    private boolean autoCreateSubscriptionGroup = true;
    
    /**
     * 消息存储插件目录
     */
    private String messageStorePlugIn = "";

    /**
     * 发送消息线程池大小 16 + cpu数目* 4
     */
    private int sendMessageThreadPoolNums = 16 + Runtime.getRuntime().availableProcessors() * 4;
    /**
     * 拉取消息线程池大小 16 + cpu数目 * 2
     */
    private int pullMessageThreadPoolNums = 16 + Runtime.getRuntime().availableProcessors() * 2;
    
    /**
     * broker管理线程池大小
     */
    private int adminBrokerThreadPoolNums = 16;
    
    /**
     * 客户端管理线程池大小
     */
    private int clientManageThreadPoolNums = 16;

    /**
     * 刷新消费偏移定时间隔5秒
     */
    private int flushConsumerOffsetInterval = 1000 * 5;

    /**
     * 定时刷新消费者偏移历史数据  1分钟
     */
    private int flushConsumerOffsetHistoryInterval = 1000 * 60;

    /**
     * 是否拒绝事务管理
     */
    @ImportantField
    private boolean rejectTransactionMessage = false;
    
    /**
     * 如果配置文件中没有配置nameserver地址的话，是否定时获取名称服务地址
     * 通过http协议获取
     * 参考:com.alibaba.rocketmq.broker.out.BrokerOuterAPI
     */
    @ImportantField
    private boolean fetchNamesrvAddrByAddressServer = false;

    /**
     * 发送线程池队列大小 1w
     */
    private int sendThreadPoolQueueCapacity = 10000;

    /**
     * 拉取线程池队列大小 1w
     */
    private int pullThreadPoolQueueCapacity = 10000;

    /**
     * 过滤服务数
     */
    private int filterServerNums = 0;

    /**
     * 是否支持长轮询 默认10秒
     * 主要在拉取消息中使用
     * 如果不支持长轮询，则使用shortPollingTimeMills
     */
    private boolean longPollingEnable = true;

    /**
     * 拉取消息等待的等待时间
     */
    private long shortPollingTimeMills = 1000;

    /**
     * 是否通知消息id变化
     */
    private boolean notifyConsumerIdsChangedEnable = true;

    /**
     * 高速模式
     */
    private boolean highSpeedMode = false;

    private boolean commercialEnable = true;
    
    private int commercialTimerCount = 1;
    
    private int commercialTransCount = 1;
    
    private int commercialBigCount = 1;

    /**
     * 是否通过堆来传输消息
     */
    private boolean transferMsgByHeap = true;
    
    private int maxDelayTime = 40;

    /**
     * 区域编号
     */
    private String regionId = "DefaultRegion";
    
    /**
     * 注册broker超时时间
     */
    private int registerBrokerTimeoutMills = 6000;

    /**
     * slave是否支持读
     */
    private boolean slaveReadEnable = false;

    /**
     * 如果消费速度慢是否进制消费
     */
    private boolean disableConsumeIfConsumerReadSlowly = false;
    
    /**
     * 消费进度是否慢的指标
     * 单位字节
     * 默认为：16M
     */
    private long consumerFallbehindThreshold = 1024 * 1024 * 1024 * 16;

    private long waitTimeMillsInSendQueue = 200;

    private long startAcceptSendRequestTimeStamp = 0L;

    public long getStartAcceptSendRequestTimeStamp() {
        return startAcceptSendRequestTimeStamp;
    }

    public void setStartAcceptSendRequestTimeStamp(final long startAcceptSendRequestTimeStamp) {
        this.startAcceptSendRequestTimeStamp = startAcceptSendRequestTimeStamp;
    }

    public long getWaitTimeMillsInSendQueue() {
        return waitTimeMillsInSendQueue;
    }

    public void setWaitTimeMillsInSendQueue(final long waitTimeMillsInSendQueue) {
        this.waitTimeMillsInSendQueue = waitTimeMillsInSendQueue;
    }

    public long getConsumerFallbehindThreshold() {
        return consumerFallbehindThreshold;
    }

    public void setConsumerFallbehindThreshold(final long consumerFallbehindThreshold) {
        this.consumerFallbehindThreshold = consumerFallbehindThreshold;
    }

    public boolean isDisableConsumeIfConsumerReadSlowly() {
        return disableConsumeIfConsumerReadSlowly;
    }

    public void setDisableConsumeIfConsumerReadSlowly(final boolean disableConsumeIfConsumerReadSlowly) {
        this.disableConsumeIfConsumerReadSlowly = disableConsumeIfConsumerReadSlowly;
    }

    public boolean isSlaveReadEnable() {
        return slaveReadEnable;
    }

    public void setSlaveReadEnable(final boolean slaveReadEnable) {
        this.slaveReadEnable = slaveReadEnable;
    }

    public static String localHostName() {
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }

        return "DEFAULT_BROKER";
    }

    public int getRegisterBrokerTimeoutMills() {
        return registerBrokerTimeoutMills;
    }

    public void setRegisterBrokerTimeoutMills(final int registerBrokerTimeoutMills) {
        this.registerBrokerTimeoutMills = registerBrokerTimeoutMills;
    }

    public String getRegionId() {
        return regionId;
    }

    public void setRegionId(final String regionId) {
        this.regionId = regionId;
    }

    public boolean isTransferMsgByHeap() {
        return transferMsgByHeap;
    }

    public void setTransferMsgByHeap(final boolean transferMsgByHeap) {
        this.transferMsgByHeap = transferMsgByHeap;
    }

    public String getMessageStorePlugIn() {
        return messageStorePlugIn;
    }

    public void setMessageStorePlugIn(String messageStorePlugIn) {
        this.messageStorePlugIn = messageStorePlugIn;
    }

    public boolean isHighSpeedMode() {
        return highSpeedMode;
    }


    public void setHighSpeedMode(final boolean highSpeedMode) {
        this.highSpeedMode = highSpeedMode;
    }


    public String getRocketmqHome() {
        return rocketmqHome;
    }


    public void setRocketmqHome(String rocketmqHome) {
        this.rocketmqHome = rocketmqHome;
    }


    public String getBrokerName() {
        return brokerName;
    }


    public void setBrokerName(String brokerName) {
        this.brokerName = brokerName;
    }


    public int getBrokerPermission() {
        return brokerPermission;
    }


    public void setBrokerPermission(int brokerPermission) {
        this.brokerPermission = brokerPermission;
    }


    public int getDefaultTopicQueueNums() {
        return defaultTopicQueueNums;
    }


    public void setDefaultTopicQueueNums(int defaultTopicQueueNums) {
        this.defaultTopicQueueNums = defaultTopicQueueNums;
    }


    public boolean isAutoCreateTopicEnable() {
        return autoCreateTopicEnable;
    }


    public void setAutoCreateTopicEnable(boolean autoCreateTopic) {
        this.autoCreateTopicEnable = autoCreateTopic;
    }


    public String getBrokerClusterName() {
        return brokerClusterName;
    }


    public void setBrokerClusterName(String brokerClusterName) {
        this.brokerClusterName = brokerClusterName;
    }


    public String getBrokerIP1() {
        return brokerIP1;
    }


    public void setBrokerIP1(String brokerIP1) {
        this.brokerIP1 = brokerIP1;
    }


    public String getBrokerIP2() {
        return brokerIP2;
    }


    public void setBrokerIP2(String brokerIP2) {
        this.brokerIP2 = brokerIP2;
    }


    public int getSendMessageThreadPoolNums() {
        return sendMessageThreadPoolNums;
    }


    public void setSendMessageThreadPoolNums(int sendMessageThreadPoolNums) {
        this.sendMessageThreadPoolNums = sendMessageThreadPoolNums;
    }


    public int getPullMessageThreadPoolNums() {
        return pullMessageThreadPoolNums;
    }


    public void setPullMessageThreadPoolNums(int pullMessageThreadPoolNums) {
        this.pullMessageThreadPoolNums = pullMessageThreadPoolNums;
    }


    public int getAdminBrokerThreadPoolNums() {
        return adminBrokerThreadPoolNums;
    }


    public void setAdminBrokerThreadPoolNums(int adminBrokerThreadPoolNums) {
        this.adminBrokerThreadPoolNums = adminBrokerThreadPoolNums;
    }


    public int getFlushConsumerOffsetInterval() {
        return flushConsumerOffsetInterval;
    }


    public void setFlushConsumerOffsetInterval(int flushConsumerOffsetInterval) {
        this.flushConsumerOffsetInterval = flushConsumerOffsetInterval;
    }


    public int getFlushConsumerOffsetHistoryInterval() {
        return flushConsumerOffsetHistoryInterval;
    }


    public void setFlushConsumerOffsetHistoryInterval(int flushConsumerOffsetHistoryInterval) {
        this.flushConsumerOffsetHistoryInterval = flushConsumerOffsetHistoryInterval;
    }


    public boolean isClusterTopicEnable() {
        return clusterTopicEnable;
    }


    public void setClusterTopicEnable(boolean clusterTopicEnable) {
        this.clusterTopicEnable = clusterTopicEnable;
    }


    public String getNamesrvAddr() {
        return namesrvAddr;
    }


    public void setNamesrvAddr(String namesrvAddr) {
        this.namesrvAddr = namesrvAddr;
    }


    public long getBrokerId() {
        return brokerId;
    }


    public void setBrokerId(long brokerId) {
        this.brokerId = brokerId;
    }


    public boolean isAutoCreateSubscriptionGroup() {
        return autoCreateSubscriptionGroup;
    }


    public void setAutoCreateSubscriptionGroup(boolean autoCreateSubscriptionGroup) {
        this.autoCreateSubscriptionGroup = autoCreateSubscriptionGroup;
    }


    public boolean isRejectTransactionMessage() {
        return rejectTransactionMessage;
    }


    public void setRejectTransactionMessage(boolean rejectTransactionMessage) {
        this.rejectTransactionMessage = rejectTransactionMessage;
    }


    public boolean isFetchNamesrvAddrByAddressServer() {
        return fetchNamesrvAddrByAddressServer;
    }


    public void setFetchNamesrvAddrByAddressServer(boolean fetchNamesrvAddrByAddressServer) {
        this.fetchNamesrvAddrByAddressServer = fetchNamesrvAddrByAddressServer;
    }


    public int getSendThreadPoolQueueCapacity() {
        return sendThreadPoolQueueCapacity;
    }


    public void setSendThreadPoolQueueCapacity(int sendThreadPoolQueueCapacity) {
        this.sendThreadPoolQueueCapacity = sendThreadPoolQueueCapacity;
    }


    public int getPullThreadPoolQueueCapacity() {
        return pullThreadPoolQueueCapacity;
    }


    public void setPullThreadPoolQueueCapacity(int pullThreadPoolQueueCapacity) {
        this.pullThreadPoolQueueCapacity = pullThreadPoolQueueCapacity;
    }


    public boolean isBrokerTopicEnable() {
        return brokerTopicEnable;
    }


    public void setBrokerTopicEnable(boolean brokerTopicEnable) {
        this.brokerTopicEnable = brokerTopicEnable;
    }


    public int getFilterServerNums() {
        return filterServerNums;
    }


    public void setFilterServerNums(int filterServerNums) {
        this.filterServerNums = filterServerNums;
    }


    public boolean isLongPollingEnable() {
        return longPollingEnable;
    }


    public void setLongPollingEnable(boolean longPollingEnable) {
        this.longPollingEnable = longPollingEnable;
    }


    public boolean isNotifyConsumerIdsChangedEnable() {
        return notifyConsumerIdsChangedEnable;
    }


    public void setNotifyConsumerIdsChangedEnable(boolean notifyConsumerIdsChangedEnable) {
        this.notifyConsumerIdsChangedEnable = notifyConsumerIdsChangedEnable;
    }


    public long getShortPollingTimeMills() {
        return shortPollingTimeMills;
    }


    public void setShortPollingTimeMills(long shortPollingTimeMills) {
        this.shortPollingTimeMills = shortPollingTimeMills;
    }


    public int getClientManageThreadPoolNums() {
        return clientManageThreadPoolNums;
    }


    public void setClientManageThreadPoolNums(int clientManageThreadPoolNums) {
        this.clientManageThreadPoolNums = clientManageThreadPoolNums;
    }


    public boolean isCommercialEnable() {
        return commercialEnable;
    }


    public void setCommercialEnable(final boolean commercialEnable) {
        this.commercialEnable = commercialEnable;
    }

    public int getCommercialTimerCount() {
        return commercialTimerCount;
    }

    public void setCommercialTimerCount(final int commercialTimerCount) {
        this.commercialTimerCount = commercialTimerCount;
    }

    public int getCommercialTransCount() {
        return commercialTransCount;
    }

    public void setCommercialTransCount(final int commercialTransCount) {
        this.commercialTransCount = commercialTransCount;
    }

    public int getCommercialBigCount() {
        return commercialBigCount;
    }

    public void setCommercialBigCount(final int commercialBigCount) {
        this.commercialBigCount = commercialBigCount;
    }

    public int getMaxDelayTime() {
        return maxDelayTime;
    }


    public void setMaxDelayTime(final int maxDelayTime) {
        this.maxDelayTime = maxDelayTime;
    }
}
