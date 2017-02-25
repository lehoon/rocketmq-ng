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
package com.alibaba.rocketmq.broker.client;

import com.alibaba.rocketmq.broker.BrokerController;
import com.alibaba.rocketmq.common.ThreadFactoryImpl;
import com.alibaba.rocketmq.common.constant.LoggerName;
import com.alibaba.rocketmq.remoting.ChannelEventListener;
import io.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;


/**
 * 客户端状态维护服务
 * @author shijia.wxr
 */
public class ClientHousekeepingService implements ChannelEventListener {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.BrokerLoggerName);
    
    /**
     * broker对象
     */
    private final BrokerController brokerController;

    /**
     * 定义任务调度服务对象
     */
    private ScheduledExecutorService scheduledExecutorService = Executors
            .newSingleThreadScheduledExecutor(new ThreadFactoryImpl("ClientHousekeepingScheduledThread"));


    public ClientHousekeepingService(final BrokerController brokerController) {
        this.brokerController = brokerController;
    }

    /**
     * @描述: 10秒运行一次检查异常的socket通道 
     * @作者: zgzhang@txbds.com
     * @日期:2016年11月24日
     * @修改内容
     * @参数：     
     * @throws
     */
    public void start() {

        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                try {
                    ClientHousekeepingService.this.scanExceptionChannel();
                } catch (Exception e) {
                    log.error("", e);
                }
            }
        }, 1000 * 10, 1000 * 10, TimeUnit.MILLISECONDS);
    }

    /**
     * @描述: 扫描异常的socket通道 
     * @作者: zgzhang@txbds.com
     * @日期:2016年11月24日
     * @修改内容
     * @参数：     
     * @throws
     */
    private void scanExceptionChannel() {
        this.brokerController.getProducerManager().scanNotActiveChannel();
        this.brokerController.getConsumerManager().scanNotActiveChannel();
        this.brokerController.getFilterServerManager().scanNotActiveChannel();
    }

    /**
     * @描述: 关闭任务调度服务 
     * @作者: zgzhang@txbds.com
     * @日期:2016年11月24日
     * @修改内容
     * @参数：     
     * @throws
     */
    public void shutdown() {
        this.scheduledExecutorService.shutdown();
    }

    @Override
    public void onChannelConnect(String remoteAddr, Channel channel) {

    }

    /**
     * 远程socket关闭事件
     */
    @Override
    public void onChannelClose(String remoteAddr, Channel channel) {
        this.brokerController.getProducerManager().doChannelCloseEvent(remoteAddr, channel);
        this.brokerController.getConsumerManager().doChannelCloseEvent(remoteAddr, channel);
        this.brokerController.getFilterServerManager().doChannelCloseEvent(remoteAddr, channel);
    }

    /**
     * 远程socket异常事件
     */
    @Override
    public void onChannelException(String remoteAddr, Channel channel) {
        this.brokerController.getProducerManager().doChannelCloseEvent(remoteAddr, channel);
        this.brokerController.getConsumerManager().doChannelCloseEvent(remoteAddr, channel);
        this.brokerController.getFilterServerManager().doChannelCloseEvent(remoteAddr, channel);
    }

    /**
     * 远程socket指定时间内无任何活动
     */
    @Override
    public void onChannelIdle(String remoteAddr, Channel channel) {
        this.brokerController.getProducerManager().doChannelCloseEvent(remoteAddr, channel);
        this.brokerController.getConsumerManager().doChannelCloseEvent(remoteAddr, channel);
        this.brokerController.getFilterServerManager().doChannelCloseEvent(remoteAddr, channel);
    }
}
