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
package com.alibaba.rocketmq.client.impl.factory;

import com.alibaba.rocketmq.client.ClientConfig;
import com.alibaba.rocketmq.client.admin.MQAdminExtInner;
import com.alibaba.rocketmq.client.exception.MQBrokerException;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.client.impl.*;
import com.alibaba.rocketmq.client.impl.consumer.*;
import com.alibaba.rocketmq.client.impl.producer.DefaultMQProducerImpl;
import com.alibaba.rocketmq.client.impl.producer.MQProducerInner;
import com.alibaba.rocketmq.client.impl.producer.TopicPublishInfo;
import com.alibaba.rocketmq.client.log.ClientLogger;
import com.alibaba.rocketmq.client.producer.DefaultMQProducer;
import com.alibaba.rocketmq.client.stat.ConsumerStatsManager;
import com.alibaba.rocketmq.common.MQVersion;
import com.alibaba.rocketmq.common.MixAll;
import com.alibaba.rocketmq.common.ServiceState;
import com.alibaba.rocketmq.common.UtilAll;
import com.alibaba.rocketmq.common.constant.PermName;
import com.alibaba.rocketmq.common.message.MessageExt;
import com.alibaba.rocketmq.common.message.MessageQueue;
import com.alibaba.rocketmq.common.protocol.body.ConsumeMessageDirectlyResult;
import com.alibaba.rocketmq.common.protocol.body.ConsumerRunningInfo;
import com.alibaba.rocketmq.common.protocol.heartbeat.*;
import com.alibaba.rocketmq.common.protocol.route.BrokerData;
import com.alibaba.rocketmq.common.protocol.route.QueueData;
import com.alibaba.rocketmq.common.protocol.route.TopicRouteData;
import com.alibaba.rocketmq.remoting.RPCHook;
import com.alibaba.rocketmq.remoting.common.RemotingHelper;
import com.alibaba.rocketmq.remoting.exception.RemotingException;
import com.alibaba.rocketmq.remoting.netty.NettyClientConfig;
import com.alibaba.rocketmq.remoting.protocol.RemotingCommand;
import org.slf4j.Logger;

import java.io.UnsupportedEncodingException;
import java.net.DatagramSocket;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;


/**
 * �ͻ�������ʵ��
 * @author shijia.wxr
 */
public class MQClientInstance {
    private final static long LockTimeoutMillis = 3000;
    private final Logger log = ClientLogger.getLog();
    /**
     * �����ļ�
     */
    private final ClientConfig clientConfig;
    
    /**
     * ʵ���������
     */
    private final int instanceIndex;
    
    /**
     * �ͻ��˱��
     */
    private final String clientId;
    
    /**
     * ����ʱ��
     */
    private final long bootTimestamp = System.currentTimeMillis();
    
    /**
     * �����߶���map
     */
    private final ConcurrentHashMap<String/* group */, MQProducerInner> producerTable = new ConcurrentHashMap<String, MQProducerInner>();
    
    /**
     * �����߶���map
     */
    private final ConcurrentHashMap<String/* group */, MQConsumerInner> consumerTable = new ConcurrentHashMap<String, MQConsumerInner>();
    
    /**
     * �����߶���map
     */
    private final ConcurrentHashMap<String/* group */, MQAdminExtInner> adminExtTable = new ConcurrentHashMap<String, MQAdminExtInner>();
    
    /**
     * netty���������ļ�
     */
    private final NettyClientConfig nettyClientConfig;
    
    /**
     * �ͻ���ͨ��ʵ�ֶ���
     */
    private final MQClientAPIImpl mQClientAPIImpl;
    
    /**
     * 
     */
    private final MQAdminImpl mQAdminImpl;
    
    /**
     * ���������·����Ϣ��Ӧ��ϵ
     */
    private final ConcurrentHashMap<String/* Topic */, TopicRouteData> topicRouteTable = new ConcurrentHashMap<String, TopicRouteData>();
    
    /**
     * ����������������
     */
    private final Lock lockNamesrv = new ReentrantLock();
    
    /**
     * ����������
     */
    private final Lock lockHeartbeat = new ReentrantLock();
    
    /**
     * broker��ַ��ϵ
     */
    private final ConcurrentHashMap<String/* Broker Name */, HashMap<Long/* brokerId */, String/* address */>> brokerAddrTable =
            new ConcurrentHashMap<String, HashMap<Long, String>>();
    
    /**
     * �����̳߳س�ʼ��
     */
    private final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
        @Override
        public Thread newThread(Runnable r) {
            return new Thread(r, "MQClientFactoryScheduledThread");
        }
    });
    
    /**
     * �ͻ���������봦��������
     */
    private final ClientRemotingProcessor clientRemotingProcessor;
    
    /**
     * ��ȡ��Ϣ�������
     */
    private final PullMessageService pullMessageService;
    
    /**
     * ���ؾ���������
     */
    private final RebalanceService rebalanceService;
    
    /**
     * Ĭ�ϵ���Ϣ�����߶���
     */
    private final DefaultMQProducer defaultMQProducer;
    
    /**
     * ������״̬������� 
     */
    private final ConsumerStatsManager consumerStatsManager;
    
    /**
     * ��־����Ƶ��
     * 20�μ�¼��־
     * ��Ҫ�����¼��������������־
     */
    private final AtomicLong storeTimesTotal = new AtomicLong(0);
    
    /**
     * ����״̬,Ĭ��Ϊ����δ����
     */
    private ServiceState serviceState = ServiceState.CREATE_JUST;
    
    /**
     * udp���ݰ�����
     * 3.5.8û���õ�
     */
    private DatagramSocket datagramSocket;

    /**
     * ���캯��
     * @param clientConfig
     * @param instanceIndex
     * @param clientId
     */
    public MQClientInstance(ClientConfig clientConfig, int instanceIndex, String clientId) {
        this(clientConfig, instanceIndex, clientId, null);
    }

    /**
     * ��ʼ������
     * @param clientConfig
     * @param instanceIndex
     * @param clientId
     * @param rpcHook
     */
    public MQClientInstance(ClientConfig clientConfig, int instanceIndex, String clientId, RPCHook rpcHook) 
    {
        this.clientConfig = clientConfig;
        this.instanceIndex = instanceIndex;
        this.nettyClientConfig = new NettyClientConfig();
        this.nettyClientConfig.setClientCallbackExecutorThreads(clientConfig.getClientCallbackExecutorThreads());
        this.clientRemotingProcessor = new ClientRemotingProcessor(this);
        
        /**
         * �ͻ��˲���ʵ�������
         */
        this.mQClientAPIImpl = new MQClientAPIImpl(this.nettyClientConfig, this.clientRemotingProcessor, rpcHook, clientConfig);

        /**
         * ������������ַ�Ƿ�Ϸ�
         */
        if (this.clientConfig.getNamesrvAddr() != null) {
            this.mQClientAPIImpl.updateNameServerAddressList(this.clientConfig.getNamesrvAddr());
            log.info("user specified name server address: {}", this.clientConfig.getNamesrvAddr());
        }

        this.clientId = clientId;

        this.mQAdminImpl = new MQAdminImpl(this);

        this.pullMessageService = new PullMessageService(this);

        this.rebalanceService = new RebalanceService(this);

        this.defaultMQProducer = new DefaultMQProducer(MixAll.CLIENT_INNER_PRODUCER_GROUP);
        this.defaultMQProducer.resetClientConfig(clientConfig);

        this.consumerStatsManager = new ConsumerStatsManager(this.scheduledExecutorService);

        log.info("created a new client Instance, FactoryIndex: {} ClinetID: {} {} {}, serializeType={}", //
                this.instanceIndex, //
                this.clientId, //
                this.clientConfig, //
                MQVersion.getVersionDesc(MQVersion.CurrentVersion), RemotingCommand.getSerializeTypeConfigInThisServer());
    }

    /**
     * @����: ���� 
     * @����: zgzhang@txbds.com
     * @����:2016��11��23��
     * @�޸�����
     * @������@throws MQClientException     
     * @throws
     */
    public void start() throws MQClientException {
        synchronized (this) {
            switch (this.serviceState) {
                case CREATE_JUST:
                    this.serviceState = ServiceState.START_FAILED;
                    // If not specified,looking address from name server
                    //��������ļ�û������namesrv��ַ�Ļ�������Ҫͨ��http��ȡ
                    if (null == this.clientConfig.getNamesrvAddr()) 
                    {
                        this.clientConfig.setNamesrvAddr(this.mQClientAPIImpl.fetchNameServerAddr());
                    }

                    // Start request-response channel
                    //������Ӧchannel
                    this.mQClientAPIImpl.start();
                    //��������
                    // Start various schedule tasks
                    this.startScheduledTask();
                    //��ȡ��Ϣ����
                    // Start pull service
                    this.pullMessageService.start();
                    //���ؾ������
                    // Start rebalance service
                    this.rebalanceService.start();
                    //��ȡ��Ϣ
                    // Start push service
                    this.defaultMQProducer.getDefaultMQProducerImpl().start(false);
                    log.info("the client factory [{}] start OK", this.clientId);
                    this.serviceState = ServiceState.RUNNING;
                    break;
                case RUNNING:
                    break;
                case SHUTDOWN_ALREADY:
                    break;
                case START_FAILED:
                    throw new MQClientException("The Factory object[" + this.getClientId() + "] has been created before, and failed.", null);
                default:
                    break;
            }
        }
    }

    /**
     * @����: ������ȷ������� 
     * @����: zgzhang@txbds.com
     * @����:2016��11��23��
     * @�޸�����
     * @������     
     * @throws
     */
    private void startScheduledTask() {
    	/**
    	 * �����õ�namesrv�����˵Ļ����򲻲�Ҫ��ʱͨ��http��ȡnamesrv
    	 * 10�������һ��
    	 * �Ժ�ÿ2���Ӹ���һ��
    	 */
        if (null == this.clientConfig.getNamesrvAddr()) {
            this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
                @Override
                public void run() {
                    try {
                        MQClientInstance.this.mQClientAPIImpl.fetchNameServerAddr();
                    } catch (Exception e) {
                        log.error("ScheduledTask fetchNameServerAddr exception", e);
                    }
                }
            }, 1000 * 10 /* 10��*/, 1000 * 60 * 2 /*2����*/, TimeUnit.MILLISECONDS);
        }

        /**
         * ��namesrv�ϸ��¶���·����Ϣ
         * 10���������
         * Ĭ�ϼ��ʱ��Ϊ30��
         */
        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                try {
                    MQClientInstance.this.updateTopicRouteInfoFromNameServer();
                } catch (Exception e) {
                    log.error("ScheduledTask updateTopicRouteInfoFromNameServer exception", e);
                }
            }
        }, 10, this.clientConfig.getPollNameServerInteval(), TimeUnit.MILLISECONDS);

        /**
         * 1�������쳣or���ߵ�broker
         * 2�����������������е�broker
         * 
         * 1�������
         * Ĭ�ϼ��ʱ��Ϊ30��
         */
        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {

            @Override
            public void run() {
                try {
                    MQClientInstance.this.cleanOfflineBroker();
                    MQClientInstance.this.sendHeartbeatToAllBrokerWithLock();
                } catch (Exception e) {
                    log.error("ScheduledTask sendHeartbeatToAllBroker exception", e);
                }
            }
        }, 1000, this.clientConfig.getHeartbeatBrokerInterval(), TimeUnit.MILLISECONDS);

        /**
         * �־ñ������������ѽ���
         * 10�������
         * Ĭ�ϳ־û�ʱ���� Ĭ��5��
         */
        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {

            @Override
            public void run() {
                try {
                    MQClientInstance.this.persistAllConsumerOffset();
                } catch (Exception e) {
                    log.error("ScheduledTask persistAllConsumerOffset exception", e);
                }
            }
        }, 1000 * 10, this.clientConfig.getPersistConsumerOffsetInterval(), TimeUnit.MILLISECONDS);

        /**
         * ��ʱ�����̳߳�
         * 1�������
         * ���м��Ϊ1��
         */
        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {

            @Override
            public void run() {
                try {
                	//����push�������̳߳� ֻ����push���������Ҫ pull��ʱ����Ҫ����
                    MQClientInstance.this.adjustThreadPool();
                } catch (Exception e) {
                    log.error("ScheduledTask adjustThreadPool exception", e);
                }
            }
        }, 1, 1, TimeUnit.MINUTES);
    }

    public String getClientId() {
        return clientId;
    }

    /**
     * @����: ��namesrv���¶��ĵ�����·����Ϣ 
     * @����: zgzhang@txbds.com
     * @����:2016��11��23��
     * @�޸�����
     * @������     
     * @throws
     */
    public void updateTopicRouteInfoFromNameServer() {
    	/**
    	 * ���������ߡ����������еĶ��������б�
    	 */
        Set<String> topicList = new HashSet<String>();

        //������
        // Consumer
        {
            Iterator<Entry<String, MQConsumerInner>> it = this.consumerTable.entrySet().iterator();
            while (it.hasNext()) {
                Entry<String, MQConsumerInner> entry = it.next();
                MQConsumerInner impl = entry.getValue();
                if (impl != null) {
                    Set<SubscriptionData> subList = impl.subscriptions();
                    if (subList != null) {
                        for (SubscriptionData subData : subList) {
                            topicList.add(subData.getTopic());
                        }
                    }
                }
            }
        }

        //������
        // Producer
        {
            Iterator<Entry<String, MQProducerInner>> it = this.producerTable.entrySet().iterator();
            while (it.hasNext()) {
                Entry<String, MQProducerInner> entry = it.next();
                MQProducerInner impl = entry.getValue();
                if (impl != null) {
                    Set<String> lst = impl.getPublishTopicList();
                    topicList.addAll(lst);
                }
            }
        }

        /**
         * ѭ���������е�����,��namesrv�ϻ�ȡ��Ӧ��·����Ϣ
         */
        for (String topic : topicList) {
            this.updateTopicRouteInfoFromNameServer(topic);
        }
    }

    /**
     * ������Ҫ��broker
     * Remove offline broker
     */
    private void cleanOfflineBroker() {
        try {
            if (this.lockNamesrv.tryLock(LockTimeoutMillis, TimeUnit.MILLISECONDS))
                try {
                    ConcurrentHashMap<String, HashMap<Long, String>> updatedTable = new ConcurrentHashMap<String, HashMap<Long, String>>();

                    Iterator<Entry<String, HashMap<Long, String>>> itBrokerTable = this.brokerAddrTable.entrySet().iterator();
                    while (itBrokerTable.hasNext()) {
                        Entry<String, HashMap<Long, String>> entry = itBrokerTable.next();
                        String brokerName = entry.getKey();
                        HashMap<Long, String> oneTable = entry.getValue();

                        HashMap<Long, String> cloneAddrTable = new HashMap<Long, String>();
                        cloneAddrTable.putAll(oneTable);

                        Iterator<Entry<Long, String>> it = cloneAddrTable.entrySet().iterator();
                        while (it.hasNext()) {
                            Entry<Long, String> ee = it.next();
                            String addr = ee.getValue();
                            
                            /**
                             * �������Ҫ��broker
                             */
                            if (!this.isBrokerAddrExistInTopicRouteTable(addr)) {
                                it.remove();
                                log.info("the broker addr[{} {}] is offline, remove it", brokerName, addr);
                            }
                        }

                        /**
                         * ����ԭ�е�broker��
                         */
                        if (cloneAddrTable.isEmpty()) {
                        	//���ȫ��ʧЧ,��ֱ�����
                            itBrokerTable.remove();
                            log.info("the broker[{}] name's host is offline, remove it", brokerName);
                        } else {
                        	//����Ϊ��Ч�ļ����б�
                            updatedTable.put(brokerName, cloneAddrTable);
                        }
                    }

                    /**
                     * ɸѡʣ�µ��Ƿ�Ϊ��,�����Ϊ������±�����Ч��broker��
                     */
                    if (!updatedTable.isEmpty()) {
                        this.brokerAddrTable.putAll(updatedTable);
                    }
                } finally {
                	/**
                	 * �ͷ���
                	 */
                    this.lockNamesrv.unlock();
                }
        } catch (InterruptedException e) {
            log.warn("cleanOfflineBroker Exception", e);
        }
    }

    /**
     * @����: ���������������й�����broker 
     * @����: zgzhang@txbds.com
     * @����:2016��11��23��
     * @�޸�����
     * @������     
     * @throws
     */
    public void sendHeartbeatToAllBrokerWithLock() {
        if (this.lockHeartbeat.tryLock()) {
            try {
            	//����������
                this.sendHeartbeatToAllBroker();
                //���¹����ļ�
                this.uploadFilterClassSource();
            } catch (final Exception e) {
                log.error("sendHeartbeatToAllBroker exception", e);
            } finally {
                this.lockHeartbeat.unlock();
            }
        } else {
            log.warn("lock heartBeat, but failed.");
        }
    }

    /**
     * @����: �־û����������ߵ����ѽ��� 
     * @����: zgzhang@txbds.com
     * @����:2016��11��23��
     * @�޸�����
     * @������     
     * @throws
     */
    private void persistAllConsumerOffset() {
        Iterator<Entry<String, MQConsumerInner>> it = this.consumerTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, MQConsumerInner> entry = it.next();
            MQConsumerInner impl = entry.getValue();
            impl.persistConsumerOffset();
        }
    }

    /**
     * @����: ����push�������̳߳� ֻ����push���������Ҫ pull��ʱ����Ҫ����
     * @����: zgzhang@txbds.com
     * @����:2016��11��23��
     * @�޸�����
     * @������     
     * @throws
     */
    public void adjustThreadPool() {
        Iterator<Entry<String, MQConsumerInner>> it = this.consumerTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, MQConsumerInner> entry = it.next();
            MQConsumerInner impl = entry.getValue();
            if (impl != null) {
                try {
                    if (impl instanceof DefaultMQPushConsumerImpl) {
                        DefaultMQPushConsumerImpl dmq = (DefaultMQPushConsumerImpl) impl;
                        dmq.adjustThreadPool();
                    }
                } catch (Exception e) {
                }
            }
        }
    }

    /**
     * @����: ��nameserver���¶��ĵ�·����Ϣ 
     * @����: zgzhang@txbds.com
     * @����:2016��11��23��
     * @�޸�����
     * @������@param topic
     * @������@return     
     * @return boolean   
     * @throws
     */
    public boolean updateTopicRouteInfoFromNameServer(final String topic) {
        return updateTopicRouteInfoFromNameServer(topic, false, null);
    }

    /**
     * @����: �ж�broker�Ƿ��ڱ��ض���������Ҫ��Χ֮�� 
     * @����: zgzhang@txbds.com
     * @����:2016��11��23��
     * @�޸�����
     * @������@param addr
     * @������@return     
     * @throws
     */
    private boolean isBrokerAddrExistInTopicRouteTable(final String addr) {
    	/**
    	 * ������ͼ·����Ϣ
    	 */
        Iterator<Entry<String, TopicRouteData>> it = this.topicRouteTable.entrySet().iterator();
        
        while (it.hasNext()) {
            Entry<String, TopicRouteData> entry = it.next();
            TopicRouteData topicRouteData = entry.getValue();
            List<BrokerData> bds = topicRouteData.getBrokerDatas();
            for (BrokerData bd : bds) {
                if (bd.getBrokerAddrs() != null) {
                    boolean exist = bd.getBrokerAddrs().containsValue(addr);
                    if (exist)
                        return true;
                }
            }
        }

        return false;
    }

    /**
     * @����: ������broker���������� 
     * @����: zgzhang@txbds.com
     * @����:2016��11��23��
     * @�޸�����
     * @������     
     * @throws
     */
    private void sendHeartbeatToAllBroker() {
    	/**
    	 * ��������������
    	 */
        final HeartbeatData heartbeatData = this.prepareHeartbeatData();
        
        /**
         * �Ƿ���������
         */
        final boolean producerEmpty = heartbeatData.getProducerDataSet().isEmpty();
        
        /**
         * �Ƿ���������
         */
        final boolean consumerEmpty = heartbeatData.getConsumerDataSet().isEmpty();
        
        /**
         * ���û�����ɺ������ߵ�ʱ�򲻷���������
         */
        if (producerEmpty && consumerEmpty) {
            log.warn("sending hearbeat, but no consumer and no producer");
            return;
        }

        /**
         * �洢����ͳ��
         */
        long times = this.storeTimesTotal.getAndIncrement();
        
        /**
         * broker�б�
         */
        Iterator<Entry<String, HashMap<Long, String>>> it = this.brokerAddrTable.entrySet().iterator();
        
        while (it.hasNext()) 
        {
            Entry<String, HashMap<Long, String>> entry = it.next();
            /**
             * broker�ĵ�ַ ip:port
             */
            String brokerName = entry.getKey();
            HashMap<Long, String> oneTable = entry.getValue();
            if (oneTable != null) {
                for (Map.Entry<Long, String> entry1 : oneTable.entrySet()) {
                    Long id = entry1.getKey();
                    String addr = entry1.getValue();
                    if (addr != null) {
                    	/**
                    	 * ���û�������ߣ�����brokerΪslave��ʱ�򣬲�����������
                    	 * ��Ϊslave��Ҫ�Ƕ�ȡ��Ϣ, ���û������������Ҫά�������Ӽ���
                    	 */
                        if (consumerEmpty) 
                        {
                        	/**
                        	 * �ж��Ƿ���slave
                        	 */
                            if (id != MixAll.MASTER_ID)
                                continue;
                        }

                        try 
                        {
                        	/**
                        	 * ����������
                        	 * ��ʱʱ��3��
                        	 */
                            this.mQClientAPIImpl.sendHearbeat(addr, heartbeatData, 3000);
                            if (times % 20 == 0) 
                            {
                            	/**
                            	 * 20�μ�¼��־
                            	 * ��Ҫ�����¼��������������־
                            	 */
                                log.info("send heart beat to broker[{} {} {}] success", brokerName, id, addr);
                                log.info(heartbeatData.toString());
                            }
                        } 
                        catch (Exception e) 
                        {
                            if (this.isBrokerInNameServer(addr)) 
                            {
                                log.error("send heart beat to broker exception", e);
                            } 
                            else 
                            {
                                log.info("send heart beat to broker[{} {} {}] exception, because the broker not up, forget it", brokerName,
                                        id, addr);
                            }
                        }
                    }
                }
            }
        }
    }

    /**
     * @����: ���¹����ļ� �����������ߵ�����²Ż�����
     * @����: zgzhang@txbds.com
     * @����:2016��11��23��
     * @�޸�����
     * @������     
     * @throws
     */
    private void uploadFilterClassSource() 
    {
    	
        Iterator<Entry<String, MQConsumerInner>> it = this.consumerTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, MQConsumerInner> next = it.next();
            MQConsumerInner consumer = next.getValue();
            /**
             * pushģʽ
             */
            if (ConsumeType.CONSUME_PASSIVELY == consumer.consumeType()) {
                Set<SubscriptionData> subscriptions = consumer.subscriptions();
                for (SubscriptionData sub : subscriptions) 
                {
                	/**
                	 * �����Ҫ�ϴ����������ϴ�·��
                	 */
                    if (sub.isClassFilterMode() && sub.getFilterClassSource() != null) {
                        final String consumerGroup = consumer.groupName();
                        final String className = sub.getSubString();
                        final String topic = sub.getTopic();
                        final String filterClassSource = sub.getFilterClassSource();
                        try {
                        	/**
                        	 * �ϴ������ļ���������
                        	 */
                            this.uploadFilterClassToAllFilterServer(consumerGroup, className, topic, filterClassSource);
                        } catch (Exception e) {
                            log.error("uploadFilterClassToAllFilterServer Exception", e);
                        }
                    }
                }
            }
        }
    }

    /**
     * @����: ��namesrv�ϸ��¶���·����Ϣ 
     * @����: zgzhang@txbds.com
     * @����:2016��11��23��
     * @�޸�����
     * @������@param topic ����
     * @������@param isDefault  �Ƿ�Ĭ��   Ĭ��false
     * @������@param defaultMQProducer  Ĭ�ϵ�������  Ĭ��null
     * @������@return     
     * @throws
     */
    public boolean updateTopicRouteInfoFromNameServer(final String topic, boolean isDefault, DefaultMQProducer defaultMQProducer) {
        try {
            if (this.lockNamesrv.tryLock(LockTimeoutMillis, TimeUnit.MILLISECONDS)) {
                try {
                    TopicRouteData topicRouteData;
                    if (isDefault && defaultMQProducer != null) {
                        topicRouteData = this.mQClientAPIImpl.getDefaultTopicRouteInfoFromNameServer(defaultMQProducer.getCreateTopicKey(),
                                1000 * 3);
                        if (topicRouteData != null) {
                            for (QueueData data : topicRouteData.getQueueDatas()) {
                                int queueNums = Math.min(defaultMQProducer.getDefaultTopicQueueNums(), data.getReadQueueNums());
                                data.setReadQueueNums(queueNums);
                                data.setWriteQueueNums(queueNums);
                            }
                        }
                    } else {
                    	/**
                    	 * 3.5.8V
                    	 * Ĭ�������� 
                    	 */
                        topicRouteData = this.mQClientAPIImpl.getTopicRouteInfoFromNameServer(topic, 1000 * 3);
                    }

                    /**
                     * 
                     */
                    if (topicRouteData != null) {
                        TopicRouteData old = this.topicRouteTable.get(topic);
                        //�¾�·����Ϣ�Ƚ��Ƿ�һֱ
                        boolean changed = topicRouteDataIsChange(old, topicRouteData);
                        
                        if (!changed) {
                        	/**
                        	 * һ�£���Ҫ�ж��Ƿ���Ҫ����
                        	 */
                            changed = this.isNeedUpdateTopicRouteInfo(topic);
                        } 
                        else 
                        {
                            log.info("the topic[{}] route info changed, old[{}] ,new[{}]", topic, old, topicRouteData);
                        }

                        /**
                         * ��Ҫ����·����Ϣ
                         */
                        if (changed) 
                        {
                            TopicRouteData cloneTopicRouteData = topicRouteData.cloneTopicRouteData();

                            for (BrokerData bd : topicRouteData.getBrokerDatas()) {
                                this.brokerAddrTable.put(bd.getBrokerName(), bd.getBrokerAddrs());
                            }

                            // ���·�����Ϣ--������
                            // Update Pub info
                            {
                                TopicPublishInfo publishInfo = topicRouteData2TopicPublishInfo(topic, topicRouteData);
                                publishInfo.setHaveTopicRouterInfo(true);
                                Iterator<Entry<String, MQProducerInner>> it = this.producerTable.entrySet().iterator();
                                while (it.hasNext()) {
                                    Entry<String, MQProducerInner> entry = it.next();
                                    MQProducerInner impl = entry.getValue();
                                    if (impl != null) {
                                        impl.updateTopicPublishInfo(topic, publishInfo);
                                    }
                                }
                            }

                            // ���¶�����Ϣ -- ������
                            // Update sub info
                            {
                                Set<MessageQueue> subscribeInfo = topicRouteData2TopicSubscribeInfo(topic, topicRouteData);
                                Iterator<Entry<String, MQConsumerInner>> it = this.consumerTable.entrySet().iterator();
                                while (it.hasNext()) {
                                    Entry<String, MQConsumerInner> entry = it.next();
                                    MQConsumerInner impl = entry.getValue();
                                    if (impl != null) {
                                        impl.updateTopicSubscribeInfo(topic, subscribeInfo);
                                    }
                                }
                            }
                            
                            log.info("topicRouteTable.put TopicRouteData[{}]", cloneTopicRouteData);
                            /**
                             * ���¶��������·����Ϣ
                             */
                            this.topicRouteTable.put(topic, cloneTopicRouteData);
                            return true;
                        }
                    } else {
                        log.warn("updateTopicRouteInfoFromNameServer, getTopicRouteInfoFromNameServer return null, Topic: {}", topic);
                    }
                } catch (Exception e) {
                    if (!topic.startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX) && !topic.equals(MixAll.DEFAULT_TOPIC)) {
                        log.warn("updateTopicRouteInfoFromNameServer Exception", e);
                    }
                } finally {
                    this.lockNamesrv.unlock();
                }
            } else {
                log.warn("updateTopicRouteInfoFromNameServer tryLock timeout {}ms", LockTimeoutMillis);
            }
        } catch (InterruptedException e) {
            log.warn("updateTopicRouteInfoFromNameServer Exception", e);
        }

        return false;
    }

    /**
     * @����: �������������� 
     * @����: zgzhang@txbds.com
     * @����:2016��11��23��
     * @�޸�����
     * @������@return     
     * @throws
     */
    private HeartbeatData prepareHeartbeatData() 
    {
    	/**
    	 * ����������
    	 */
        HeartbeatData heartbeatData = new HeartbeatData();

        // clientID �ͻ��˱��
        heartbeatData.setClientID(this.clientId);

        // Consumer
        // �������б�
        for(Map.Entry<String,MQConsumerInner> entry: this.consumerTable.entrySet()){
            MQConsumerInner impl = entry.getValue();
            if (impl != null) {
                ConsumerData consumerData = new ConsumerData();
                consumerData.setGroupName(impl.groupName());
                consumerData.setConsumeType(impl.consumeType());
                consumerData.setMessageModel(impl.messageModel());
                consumerData.setConsumeFromWhere(impl.consumeFromWhere());
                consumerData.getSubscriptionDataSet().addAll(impl.subscriptions());
                consumerData.setUnitMode(impl.isUnitMode());

                heartbeatData.getConsumerDataSet().add(consumerData);
            }
        }


        // Producer
        // �������б�����
        for (Map.Entry<String/* group */, MQProducerInner> entry : this.producerTable.entrySet()) {
            MQProducerInner impl = entry.getValue();
            if (impl != null) {
                ProducerData producerData = new ProducerData();
                producerData.setGroupName(entry.getKey());

                heartbeatData.getProducerDataSet().add(producerData);
            }
        }

        return heartbeatData;
    }

    /**
     * @����: �ж�broker�Ƿ���namesrv��
     * @����: zgzhang@txbds.com
     * @����:2016��11��23��
     * @�޸�����
     * @������@param brokerAddr  broker��ַ
     * @������@return     
     * @throws
     */
    private boolean isBrokerInNameServer(final String brokerAddr) {
        Iterator<Entry<String, TopicRouteData>> it = this.topicRouteTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, TopicRouteData> itNext = it.next();
            List<BrokerData> brokerDatas = itNext.getValue().getBrokerDatas();
            for (BrokerData bd : brokerDatas) {
                boolean contain = bd.getBrokerAddrs().containsValue(brokerAddr);
                if (contain)
                    return true;
            }
        }

        return false;
    }

    /**
     * @����: �ϴ������ļ��������� 
     * @����: zgzhang@txbds.com
     * @����:2016��11��23��
     * @�޸�����
     * @������@param consumerGroup
     * @������@param fullClassName
     * @������@param topic
     * @������@param filterClassSource
     * @������@throws UnsupportedEncodingException     
     * @throws
     */
    private void uploadFilterClassToAllFilterServer(final String consumerGroup,  //������
    												final String fullClassName,  //�������ļ�ȫ��
    												final String topic,          //��ԵĶ�����������
                                                    final String filterClassSource //�����ļ�����
                                                    ) 
        throws UnsupportedEncodingException 
    {
        byte[] classBody = null;
        int classCRC = 0;
        try {
        	/**
        	 * utf-8����
        	 */
            classBody = filterClassSource.getBytes(MixAll.DEFAULT_CHARSET);
            /**
             * �����ļ�crc32
             */
            classCRC = UtilAll.crc32(classBody);
        } catch (Exception e1) {
            log.warn("uploadFilterClassToAllFilterServer Exception, ClassName: {} {}", //
                    fullClassName, //
                    RemotingHelper.exceptionSimpleDesc(e1));
        }

        /**
         * ��ȡ·����Ϣ
         */
        TopicRouteData topicRouteData = this.topicRouteTable.get(topic);
        if (topicRouteData != null //
                && topicRouteData.getFilterServerTable() != null && !topicRouteData.getFilterServerTable().isEmpty()) {
            Iterator<Entry<String, List<String>>> it = topicRouteData.getFilterServerTable().entrySet().iterator();
            while (it.hasNext()) {
                Entry<String, List<String>> next = it.next();
                List<String> value = next.getValue();
                for (final String fsAddr : value) {
                    try {
                        this.mQClientAPIImpl.registerMessageFilterClass(fsAddr, consumerGroup, topic, fullClassName, classCRC, classBody,
                                5000);

                        log.info("register message class filter to {} OK, ConsumerGroup: {} Topic: {} ClassName: {}", fsAddr, consumerGroup,
                                topic, fullClassName);

                    } catch (Exception e) {
                        log.error("uploadFilterClassToAllFilterServer Exception", e);
                    }
                }
            }
        } else {
            log.warn("register message class filter failed, because no filter server, ConsumerGroup: {} Topic: {} ClassName: {}",
                    consumerGroup, topic, fullClassName);
        }
    }

    /**
     * @����: �Ƚ�·����Ϣ�Ƿ�һֱ 
     * @����: zgzhang@txbds.com
     * @����:2016��11��23��
     * @�޸�����
     * @������@param olddata
     * @������@param nowdata
     * @������@return     
     * @throws
     */
    private boolean topicRouteDataIsChange(TopicRouteData olddata, TopicRouteData nowdata) {
        if (olddata == null || nowdata == null)
            return true;
        TopicRouteData old = olddata.cloneTopicRouteData();
        TopicRouteData now = nowdata.cloneTopicRouteData();
        Collections.sort(old.getQueueDatas());
        Collections.sort(old.getBrokerDatas());
        Collections.sort(now.getQueueDatas());
        Collections.sort(now.getBrokerDatas());
        return !old.equals(now);

    }

    /**
     * @����: �Ƿ���Ҫ���¶��������·����Ϣ 
     * @����: zgzhang@txbds.com
     * @����:2016��11��23��
     * @�޸�����
     * @������@param topic  ����
     * @������@return     
     * @throws
     */
    private boolean isNeedUpdateTopicRouteInfo(final String topic) {
        boolean result = false;
        {
        	/**
        	 * ������
        	 */
            Iterator<Entry<String, MQProducerInner>> it = this.producerTable.entrySet().iterator();
            while (it.hasNext() && !result) {
                Entry<String, MQProducerInner> entry = it.next();
                MQProducerInner impl = entry.getValue();
                if (impl != null) {
                    result = impl.isPublishTopicNeedUpdate(topic);
                }
            }
        }

        {
        	/**
        	 * ������
        	 */
            Iterator<Entry<String, MQConsumerInner>> it = this.consumerTable.entrySet().iterator();
            while (it.hasNext() && !result) {
                Entry<String, MQConsumerInner> entry = it.next();
                MQConsumerInner impl = entry.getValue();
                if (impl != null) {
                    result = impl.isSubscribeTopicNeedUpdate(topic);
                }
            }
        }

        return result;
    }

    /**
     * @����: ���������·����Ϣת������Ϣ
     * @����: zgzhang@txbds.com
     * @����:2016��11��23��
     * @�޸�����
     * @������@param topic
     * @������@param route
     * @������@return     
     * @throws
     */
    public static TopicPublishInfo topicRouteData2TopicPublishInfo(final String topic, final TopicRouteData route) {
        TopicPublishInfo info = new TopicPublishInfo();
        info.setTopicRouteData(route);
        if (route.getOrderTopicConf() != null && route.getOrderTopicConf().length() > 0) {
            String[] brokers = route.getOrderTopicConf().split(";");
            for (String broker : brokers) {
                String[] item = broker.split(":");
                int nums = Integer.parseInt(item[1]);
                for (int i = 0; i < nums; i++) {
                    MessageQueue mq = new MessageQueue(topic, item[0], i);
                    info.getMessageQueueList().add(mq);
                }
            }

            info.setOrderTopic(true);
        } 
        else 
        {
            List<QueueData> qds = route.getQueueDatas();
            Collections.sort(qds);
            for (QueueData qd : qds) {
                if (PermName.isWriteable(qd.getPerm())) {
                    BrokerData brokerData = null;
                    for (BrokerData bd : route.getBrokerDatas()) {
                        if (bd.getBrokerName().equals(qd.getBrokerName())) {
                            brokerData = bd;
                            break;
                        }
                    }

                    if (null == brokerData) {
                        continue;
                    }

                    if (!brokerData.getBrokerAddrs().containsKey(MixAll.MASTER_ID)) {
                        continue;
                    }

                    for (int i = 0; i < qd.getWriteQueueNums(); i++) {
                        MessageQueue mq = new MessageQueue(topic, qd.getBrokerName(), i);
                        info.getMessageQueueList().add(mq);
                    }
                }
            }

            info.setOrderTopic(false);
        }

        return info;
    }

    /**
     * @����: ·����Ϣת��������Ϣ 
     * @����: zgzhang@txbds.com
     * @����:2016��11��23��
     * @�޸�����
     * @������@param topic
     * @������@param route
     * @������@return     
     * @throws
     */
    public static Set<MessageQueue> topicRouteData2TopicSubscribeInfo(final String topic, final TopicRouteData route) {
        Set<MessageQueue> mqList = new HashSet<MessageQueue>();
        List<QueueData> qds = route.getQueueDatas();
        for (QueueData qd : qds) {
            if (PermName.isReadable(qd.getPerm())) {
                for (int i = 0; i < qd.getReadQueueNums(); i++) {
                    MessageQueue mq = new MessageQueue(topic, qd.getBrokerName(), i);
                    mqList.add(mq);
                }
            }
        }

        return mqList;
    }

    public void shutdown() {
        // Consumer
        if (!this.consumerTable.isEmpty())
            return;

        // AdminExt
        if (!this.adminExtTable.isEmpty())
            return;

        // Producer
        if (this.producerTable.size() > 1)
            return;

        synchronized (this) {
            switch (this.serviceState) {
                case CREATE_JUST:
                    break;
                case RUNNING:
                    this.defaultMQProducer.getDefaultMQProducerImpl().shutdown(false);

                    this.serviceState = ServiceState.SHUTDOWN_ALREADY;
                    this.pullMessageService.shutdown(true);
                    this.scheduledExecutorService.shutdown();
                    this.mQClientAPIImpl.shutdown();
                    this.rebalanceService.shutdown();

                    if (this.datagramSocket != null) {
                        this.datagramSocket.close();
                        this.datagramSocket = null;
                    }
                    MQClientManager.getInstance().removeClientFactory(this.clientId);
                    log.info("the client factory [{}] shutdown OK", this.clientId);
                    break;
                case SHUTDOWN_ALREADY:
                    break;
                default:
                    break;
            }
        }
    }

    /**
     * @����: ע�������� 
     * @����: zgzhang@txbds.com
     * @����:2016��11��23��
     * @�޸�����
     * @������@param group
     * @������@param consumer
     * @������@return     
     * @throws
     */
    public boolean registerConsumer(final String group, final MQConsumerInner consumer) {
        if (null == group || null == consumer) {
            return false;
        }

        MQConsumerInner prev = this.consumerTable.putIfAbsent(group, consumer);
        if (prev != null) {
            log.warn("the consumer group[" + group + "] exist already.");
            return false;
        }

        return true;
    }

    public void unregisterConsumer(final String group) {
        this.consumerTable.remove(group);
        this.unregisterClientWithLock(null, group);
    }

    private void unregisterClientWithLock(final String producerGroup, final String consumerGroup) {
        try {
            if (this.lockHeartbeat.tryLock(LockTimeoutMillis, TimeUnit.MILLISECONDS)) {
                try {
                    this.unregisterClient(producerGroup, consumerGroup);
                } catch (Exception e) {
                    log.error("unregisterClient exception", e);
                } finally {
                    this.lockHeartbeat.unlock();
                }
            } else {
                log.warn("lock heartBeat, but failed.");
            }
        } catch (InterruptedException e) {
            log.warn("unregisterClientWithLock exception", e);
        }
    }

    private void unregisterClient(final String producerGroup, final String consumerGroup) {
        Iterator<Entry<String, HashMap<Long, String>>> it = this.brokerAddrTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, HashMap<Long, String>> entry = it.next();
            String brokerName = entry.getKey();
            HashMap<Long, String> oneTable = entry.getValue();

            if (oneTable != null) {
                for (Map.Entry<Long, String> entry1 : oneTable.entrySet()) {
                    String addr = entry1.getValue();
                    if (addr != null) {
                        try {
                            this.mQClientAPIImpl.unregisterClient(addr, this.clientId, producerGroup, consumerGroup, 3000);
                            log.info("unregister client[Producer: {} Consumer: {}] from broker[{} {} {}] success", producerGroup,
                                    consumerGroup, brokerName, entry1.getKey(), addr);
                        } catch (RemotingException e) {
                            log.error("unregister client exception from broker: " + addr, e);
                        } catch (MQBrokerException e) {
                            log.error("unregister client exception from broker: " + addr, e);
                        } catch (InterruptedException e) {
                            log.error("unregister client exception from broker: " + addr, e);
                        }
                    }
                }
            }
        }
    }

    public boolean registerProducer(final String group, final DefaultMQProducerImpl producer) {
        if (null == group || null == producer) {
            return false;
        }

        MQProducerInner prev = this.producerTable.putIfAbsent(group, producer);
        if (prev != null) {
            log.warn("the producer group[{}] exist already.", group);
            return false;
        }

        return true;
    }

    public void unregisterProducer(final String group) {
        this.producerTable.remove(group);
        this.unregisterClientWithLock(group, null);
    }

    public boolean registerAdminExt(final String group, final MQAdminExtInner admin) {
        if (null == group || null == admin) {
            return false;
        }

        MQAdminExtInner prev = this.adminExtTable.putIfAbsent(group, admin);
        if (prev != null) {
            log.warn("the admin group[{}] exist already.", group);
            return false;
        }

        return true;
    }

    public void unregisterAdminExt(final String group) {
        this.adminExtTable.remove(group);
    }

    public void rebalanceImmediately() {
        this.rebalanceService.wakeup();
    }

    public void doRebalance() {
        for (Map.Entry<String, MQConsumerInner> entry : this.consumerTable.entrySet()) {
            MQConsumerInner impl = entry.getValue();
            if (impl != null) {
                try {
                    impl.doRebalance();
                } catch (Exception e) {
                    log.error("doRebalance exception", e);
                }
            }
        }
    }

    public MQProducerInner selectProducer(final String group) {
        return this.producerTable.get(group);
    }

    public MQConsumerInner selectConsumer(final String group) {
        return this.consumerTable.get(group);
    }

    /**
     * @����: ����broker 
     * @����: zgzhang@txbds.com
     * @����:2016��11��23��
     * @�޸�����
     * @������@param brokerName
     * @������@return     
     * @throws
     */
    public FindBrokerResult findBrokerAddressInAdmin(final String brokerName) {
        String brokerAddr = null;
        boolean slave = false;
        boolean found = false;

        HashMap<Long/* brokerId */, String/* address */> map = this.brokerAddrTable.get(brokerName);
        if (map != null && !map.isEmpty()) {
            FOR_SEG:
            for (Map.Entry<Long, String> entry : map.entrySet()) {
                Long id = entry.getKey();
                brokerAddr = entry.getValue();
                if (brokerAddr != null) {
                    found = true;
                    /**
                     * �Ƿ�Ϊ��broker
                     */
                    if (MixAll.MASTER_ID == id) {
                        slave = false;
                        break FOR_SEG;
                    } else {
                        slave = true;
                    }
                    break;

                }
            } // end of for
        }

        if (found) {
            return new FindBrokerResult(brokerAddr, slave);
        }

        return null;
    }

    /**
     * @����: ��ȡbroker��Ϣ 
     * @����: zgzhang@txbds.com
     * @����:2016��11��23��
     * @�޸�����
     * @������@param brokerName
     * @������@return     
     * @return String   
     * @throws
     */
    public String findBrokerAddressInPublish(final String brokerName) {
        HashMap<Long/* brokerId */, String/* address */> map = this.brokerAddrTable.get(brokerName);
        if (map != null && !map.isEmpty()) {
            return map.get(MixAll.MASTER_ID);
        }

        return null;
    }

    public FindBrokerResult findBrokerAddressInSubscribe(//
                                                         final String brokerName, //
                                                         final long brokerId, //
                                                         final boolean onlyThisBroker//
    ) {
        String brokerAddr = null;
        boolean slave = false;
        boolean found = false;

        HashMap<Long/* brokerId */, String/* address */> map = this.brokerAddrTable.get(brokerName);
        if (map != null && !map.isEmpty()) {
            brokerAddr = map.get(brokerId);
            slave = (brokerId != MixAll.MASTER_ID);
            found = (brokerAddr != null);

            if (!found && !onlyThisBroker) {
                Entry<Long, String> entry = map.entrySet().iterator().next();
                brokerAddr = entry.getValue();
                slave = (entry.getKey() != MixAll.MASTER_ID);
                found = true;
            }
        }

        if (found) {
            return new FindBrokerResult(brokerAddr, slave);
        }

        return null;
    }

    public List<String> findConsumerIdList(final String topic, final String group) {
        String brokerAddr = this.findBrokerAddrByTopic(topic);
        if (null == brokerAddr) {
            this.updateTopicRouteInfoFromNameServer(topic);
            brokerAddr = this.findBrokerAddrByTopic(topic);
        }

        if (null != brokerAddr) {
            try {
                return this.mQClientAPIImpl.getConsumerIdListByGroup(brokerAddr, group, 3000);
            } catch (Exception e) {
                log.warn("getConsumerIdListByGroup exception, " + brokerAddr + " " + group, e);
            }
        }

        return null;
    }

    public String findBrokerAddrByTopic(final String topic) {
        TopicRouteData topicRouteData = this.topicRouteTable.get(topic);
        if (topicRouteData != null) {
            List<BrokerData> brokers = topicRouteData.getBrokerDatas();
            if (!brokers.isEmpty()) {
                BrokerData bd = brokers.get(0);
                return bd.selectBrokerAddr();
            }
        }

        return null;
    }

    public void resetOffset(String topic, String group, Map<MessageQueue, Long> offsetTable) {
        DefaultMQPushConsumerImpl consumer = null;
        try {
            MQConsumerInner impl = this.consumerTable.get(group);
            if (impl != null && impl instanceof DefaultMQPushConsumerImpl) {
                consumer = (DefaultMQPushConsumerImpl) impl;
            } else {
                log.info("[reset-offset] consumer dose not exist. group={}", group);
                return;
            }

            consumer.suspend();

            ConcurrentHashMap<MessageQueue, ProcessQueue> processQueueTable = consumer.getRebalanceImpl().getProcessQueueTable();

            for(Map.Entry<MessageQueue,ProcessQueue> entry : processQueueTable.entrySet()){
                MessageQueue mq = entry.getKey();
                if(topic.equals(mq.getTopic())){
                    ProcessQueue pq = entry.getValue();
                    pq.setDropped(true);
                    pq.clear();
                }
            }


            try {
                TimeUnit.SECONDS.sleep(30);
            } catch (InterruptedException e) {
                //
            }

            processQueueTable = consumer.getRebalanceImpl().getProcessQueueTable();

            for (Map.Entry<MessageQueue, ProcessQueue> entry : processQueueTable.entrySet()) {
                MessageQueue mq = entry.getKey();
                if (topic.equals(mq.getTopic())) {
                    consumer.updateConsumeOffset(mq, offsetTable.get(mq));
                    consumer.getRebalanceImpl().removeUnnecessaryMessageQueue(mq, entry.getValue());
                    processQueueTable.remove(mq);
                }
            }
        } finally {
            consumer.resume();
        }
    }

    public Map<MessageQueue, Long> getConsumerStatus(String topic, String group) {
        MQConsumerInner impl = this.consumerTable.get(group);
        if (impl != null && impl instanceof DefaultMQPushConsumerImpl) {
            DefaultMQPushConsumerImpl consumer = (DefaultMQPushConsumerImpl) impl;
            return consumer.getOffsetStore().cloneOffsetTable(topic);
        } else if (impl != null && impl instanceof DefaultMQPullConsumerImpl) {
            DefaultMQPullConsumerImpl consumer = (DefaultMQPullConsumerImpl) impl;
            return consumer.getOffsetStore().cloneOffsetTable(topic);
        } else {
            return Collections.emptyMap();
        }
    }

    public TopicRouteData getAnExistTopicRouteData(final String topic) {
        return this.topicRouteTable.get(topic);
    }

    public MQClientAPIImpl getMQClientAPIImpl() {
        return mQClientAPIImpl;
    }

    public MQAdminImpl getMQAdminImpl() {
        return mQAdminImpl;
    }

    public long getBootTimestamp() {
        return bootTimestamp;
    }

    public ScheduledExecutorService getScheduledExecutorService() {
        return scheduledExecutorService;
    }

    public PullMessageService getPullMessageService() {
        return pullMessageService;
    }

    public DefaultMQProducer getDefaultMQProducer() {
        return defaultMQProducer;
    }

    public ConcurrentHashMap<String, TopicRouteData> getTopicRouteTable() {
        return topicRouteTable;
    }

    public ConsumeMessageDirectlyResult consumeMessageDirectly(final MessageExt msg, //
                                                               final String consumerGroup, //
                                                               final String brokerName) {
        MQConsumerInner mqConsumerInner = this.consumerTable.get(consumerGroup);
        if (null != mqConsumerInner) {
            DefaultMQPushConsumerImpl consumer = (DefaultMQPushConsumerImpl) mqConsumerInner;

            ConsumeMessageDirectlyResult result = consumer.getConsumeMessageService().consumeMessageDirectly(msg, brokerName);
            return result;
        }

        return null;
    }


    public ConsumerRunningInfo consumerRunningInfo(final String consumerGroup) {
        MQConsumerInner mqConsumerInner = this.consumerTable.get(consumerGroup);

        ConsumerRunningInfo consumerRunningInfo = mqConsumerInner.consumerRunningInfo();

        List<String> nsList = this.mQClientAPIImpl.getRemotingClient().getNameServerAddressList();

        StringBuffer strBuffer = new StringBuffer();
        if (nsList != null) {
            for (String addr : nsList) {
                strBuffer.append(addr + ";");
            }
        }

        String nsAddr = strBuffer.toString();
        consumerRunningInfo.getProperties().put(ConsumerRunningInfo.PROP_NAMESERVER_ADDR, nsAddr);
        consumerRunningInfo.getProperties().put(ConsumerRunningInfo.PROP_CONSUME_TYPE, mqConsumerInner.consumeType().name());
        consumerRunningInfo.getProperties().put(ConsumerRunningInfo.PROP_CLIENT_VERSION,
                MQVersion.getVersionDesc(MQVersion.CurrentVersion));

        return consumerRunningInfo;
    }


    public ConsumerStatsManager getConsumerStatsManager() {
        return consumerStatsManager;
    }

    public NettyClientConfig getNettyClientConfig() {
        return nettyClientConfig;
    }
}
