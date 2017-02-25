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
     * ���������ַ
     */
    private String namesrvAddr = System.getProperty(MixAll.NAMESRV_ADDR_PROPERTY, System.getenv(MixAll.NAMESRV_ADDR_ENV));
    
    /**
     * ������ip��ַ
     */
    private String clientIP = RemotingUtil.getLocalAddress();
    /**
     * ʵ������
     */
    private String instanceName = System.getProperty("rocketmq.client.name", "DEFAULT");
    
    /**
     * �ͻ��˻ص��߳���
     */
    private int clientCallbackExecutorThreads = Runtime.getRuntime().availableProcessors();
    /**
     * ��ʱ��������������ȡ����������Ϣ���
     * Ĭ�� 30��һ��
     * Pulling topic information interval from the named server
     */
    private int pollNameServerInteval = 1000 * 30;
    /**
     * Heartbeat interval in microseconds with message broker
     * ά����broker��������ʱ����
     * Ĭ��30��
     */
    private int heartbeatBrokerInterval = 1000 * 30;
    /**
     * Offset persistent interval for consumer
     * ��������Ϣƫ�Ƴ־û�ʱ���� Ĭ��5��
     * 
     */
    private int persistConsumerOffsetInterval = 1000 * 5;
    
    /**
     * ģʽ
     */
    private boolean unitMode = false;
    
    /**
     * ����
     */
    private String unitName;
    
    /**
     * �Ƿ�����vipͨ��
     * Ĭ��Ϊtrue  
     * ��ά��һ��vip��tcp�����ӣ�  �˿ں�Ϊbroker��listenport-2
     */
    private boolean vipChannelEnabled = Boolean.parseBoolean(System.getProperty(SendMessageWithVIPChannelProperty, "true"));

    /**
     * @����: ���ɿͻ���id,  ���ɹ���Ϊ���ͻ���ip@ʵ����    ���unitName��Ϊ���� @unitName
     * @����: zgzhang@txbds.com
     * @����:2016��11��23��
     * @�޸�����
     * @������@return     
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
     * @����: ����ָ���������ļ������޸������ļ� 
     * @����: zgzhang@txbds.com
     * @����:2016��11��23��
     * @�޸�����
     * @������@param cc     
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
     * @����: ����һ�������ļ�
     * @����: zgzhang@txbds.com
     * @����:2016��11��23��
     * @�޸�����
     * @������@return     
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
