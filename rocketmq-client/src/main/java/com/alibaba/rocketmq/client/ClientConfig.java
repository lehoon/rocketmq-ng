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
package com.alibaba.rocketmq.client;

import com.alibaba.rocketmq.common.MixAll;
import com.alibaba.rocketmq.common.UtilAll;
import com.alibaba.rocketmq.remoting.common.RemotingUtil;


/**
 * Client Common configuration
 *
 * @author shijia.wxr
 * @author vongosling
 */
public class ClientConfig {
    public static final String SendMessageWithVIPChannelProperty = "com.rocketmq.sendMessageWithVIPChannel";
    /**
     * 命名服务地址
     */
    private String namesrvAddr = System.getProperty(MixAll.NAMESRV_ADDR_PROPERTY, System.getenv(MixAll.NAMESRV_ADDR_ENV));
    
    /**
     * 消费者ip地址
     */
    private String clientIP = RemotingUtil.getLocalAddress();
    /**
     * 实例名称
     */
    private String instanceName = System.getProperty("rocketmq.client.name", "DEFAULT");
    
    /**
     * 客户端回调线程数
     */
    private int clientCallbackExecutorThreads = Runtime.getRuntime().availableProcessors();
    /**
     * 定时从命名服务器拉取订阅主题信息间隔
     * 默认 30秒一次
     * Pulling topic information interval from the named server
     */
    private int pollNameServerInteval = 1000 * 30;
    /**
     * Heartbeat interval in microseconds with message broker
     * 维护和broker服务心跳时间间隔
     * 默认30秒
     */
    private int heartbeatBrokerInterval = 1000 * 30;
    /**
     * Offset persistent interval for consumer
     * 消费者消息偏移持久化时间间隔 默认5秒
     * 
     */
    private int persistConsumerOffsetInterval = 1000 * 5;
    
    /**
     * 模式
     */
    private boolean unitMode = false;
    
    /**
     * 名称
     */
    private String unitName;
    
    /**
     * 是否启用vip通道
     * 默认为true  
     * 将维持一个vip的tcp长连接，  端口号为broker的listenport-2
     */
    private boolean vipChannelEnabled = Boolean.parseBoolean(System.getProperty(SendMessageWithVIPChannelProperty, "true"));

    /**
     * @描述: 生成客户端id,  生成规则为：客户端ip@实例名    如果unitName不为空则 @unitName
     * @作者: zgzhang@txbds.com
     * @日期:2016年11月23日
     * @修改内容
     * @参数：@return     
     * @throws
     */
    public String buildMQClientId() {
        StringBuilder sb = new StringBuilder();
        sb.append(this.getClientIP());

        sb.append("@");
        sb.append(this.getInstanceName());
        if (!UtilAll.isBlank(this.unitName)) {
            sb.append("@");
            sb.append(this.unitName);
        }

        return sb.toString();
    }

    public String getClientIP() {
        return clientIP;
    }

    public void setClientIP(String clientIP) {
        this.clientIP = clientIP;
    }

    public String getInstanceName() {
        return instanceName;
    }

    public void setInstanceName(String instanceName) {
        this.instanceName = instanceName;
    }

    public void changeInstanceNameToPID() {
        if (this.instanceName.equals("DEFAULT")) {
            this.instanceName = String.valueOf(UtilAll.getPid());
        }
    }

    /**
     * @描述: 根据指定的配置文件重新修改配置文件 
     * @作者: zgzhang@txbds.com
     * @日期:2016年11月23日
     * @修改内容
     * @参数：@param cc     
     * @throws
     */
    public void resetClientConfig(final ClientConfig cc) {
        this.namesrvAddr = cc.namesrvAddr;
        this.clientIP = cc.clientIP;
        this.instanceName = cc.instanceName;
        this.clientCallbackExecutorThreads = cc.clientCallbackExecutorThreads;
        this.pollNameServerInteval = cc.pollNameServerInteval;
        this.heartbeatBrokerInterval = cc.heartbeatBrokerInterval;
        this.persistConsumerOffsetInterval = cc.persistConsumerOffsetInterval;
        this.unitMode = cc.unitMode;
        this.unitName = cc.unitName;
        this.vipChannelEnabled = cc.vipChannelEnabled;
    }

    /**
     * @描述: 复制一份配置文件
     * @作者: zgzhang@txbds.com
     * @日期:2016年11月23日
     * @修改内容
     * @参数：@return     
     * @throws
     */
    public ClientConfig cloneClientConfig() {
        ClientConfig cc = new ClientConfig();
        cc.namesrvAddr = namesrvAddr;
        cc.clientIP = clientIP;
        cc.instanceName = instanceName;
        cc.clientCallbackExecutorThreads = clientCallbackExecutorThreads;
        cc.pollNameServerInteval = pollNameServerInteval;
        cc.heartbeatBrokerInterval = heartbeatBrokerInterval;
        cc.persistConsumerOffsetInterval = persistConsumerOffsetInterval;
        cc.unitMode = unitMode;
        cc.unitName = unitName;
        cc.vipChannelEnabled = vipChannelEnabled;
        return cc;
    }

    public String getNamesrvAddr() {
        return namesrvAddr;
    }

    public void setNamesrvAddr(String namesrvAddr) {
        this.namesrvAddr = namesrvAddr;
    }

    public int getClientCallbackExecutorThreads() {
        return clientCallbackExecutorThreads;
    }


    public void setClientCallbackExecutorThreads(int clientCallbackExecutorThreads) {
        this.clientCallbackExecutorThreads = clientCallbackExecutorThreads;
    }


    public int getPollNameServerInteval() {
        return pollNameServerInteval;
    }


    public void setPollNameServerInteval(int pollNameServerInteval) {
        this.pollNameServerInteval = pollNameServerInteval;
    }


    public int getHeartbeatBrokerInterval() {
        return heartbeatBrokerInterval;
    }


    public void setHeartbeatBrokerInterval(int heartbeatBrokerInterval) {
        this.heartbeatBrokerInterval = heartbeatBrokerInterval;
    }


    public int getPersistConsumerOffsetInterval() {
        return persistConsumerOffsetInterval;
    }


    public void setPersistConsumerOffsetInterval(int persistConsumerOffsetInterval) {
        this.persistConsumerOffsetInterval = persistConsumerOffsetInterval;
    }


    public String getUnitName() {
        return unitName;
    }


    public void setUnitName(String unitName) {
        this.unitName = unitName;
    }


    public boolean isUnitMode() {
        return unitMode;
    }


    public void setUnitMode(boolean unitMode) {
        this.unitMode = unitMode;
    }


    public boolean isVipChannelEnabled() {
        return vipChannelEnabled;
    }


    public void setVipChannelEnabled(final boolean vipChannelEnabled) {
        this.vipChannelEnabled = vipChannelEnabled;
    }


    @Override
    public String toString() {
        return "ClientConfig [namesrvAddr=" + namesrvAddr + ", clientIP=" + clientIP + ", instanceName=" + instanceName
                + ", clientCallbackExecutorThreads=" + clientCallbackExecutorThreads + ", pollNameServerInteval=" + pollNameServerInteval
                + ", heartbeatBrokerInterval=" + heartbeatBrokerInterval + ", persistConsumerOffsetInterval="
                + persistConsumerOffsetInterval + ", unitMode=" + unitMode + ", unitName=" + unitName + ", vipChannelEnabled="
                + vipChannelEnabled + "]";
    }
}
