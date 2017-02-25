/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.rocketmq.broker;

import com.alibaba.rocketmq.broker.client.*;
import com.alibaba.rocketmq.broker.client.net.Broker2Client;
import com.alibaba.rocketmq.broker.client.rebalance.RebalanceLockManager;
import com.alibaba.rocketmq.broker.filtersrv.FilterServerManager;
import com.alibaba.rocketmq.broker.latency.BrokerFastFailure;
import com.alibaba.rocketmq.broker.latency.BrokerFixedThreadPoolExecutor;
import com.alibaba.rocketmq.broker.longpolling.NotifyMessageArrivingListener;
import com.alibaba.rocketmq.broker.longpolling.PullRequestHoldService;
import com.alibaba.rocketmq.broker.mqtrace.ConsumeMessageHook;
import com.alibaba.rocketmq.broker.mqtrace.SendMessageHook;
import com.alibaba.rocketmq.broker.offset.ConsumerOffsetManager;
import com.alibaba.rocketmq.broker.out.BrokerOuterAPI;
import com.alibaba.rocketmq.broker.plugin.MessageStoreFactory;
import com.alibaba.rocketmq.broker.plugin.MessageStorePluginContext;
import com.alibaba.rocketmq.broker.processor.*;
import com.alibaba.rocketmq.broker.slave.SlaveSynchronize;
import com.alibaba.rocketmq.broker.subscription.SubscriptionGroupManager;
import com.alibaba.rocketmq.broker.topic.TopicConfigManager;
import com.alibaba.rocketmq.common.*;
import com.alibaba.rocketmq.common.constant.LoggerName;
import com.alibaba.rocketmq.common.constant.PermName;
import com.alibaba.rocketmq.common.namesrv.RegisterBrokerResult;
import com.alibaba.rocketmq.common.protocol.RequestCode;
import com.alibaba.rocketmq.common.protocol.body.TopicConfigSerializeWrapper;
import com.alibaba.rocketmq.common.stats.MomentStatsItem;
import com.alibaba.rocketmq.remoting.RPCHook;
import com.alibaba.rocketmq.remoting.RemotingServer;
import com.alibaba.rocketmq.remoting.netty.*;
import com.alibaba.rocketmq.store.DefaultMessageStore;
import com.alibaba.rocketmq.store.MessageArrivingListener;
import com.alibaba.rocketmq.store.MessageStore;
import com.alibaba.rocketmq.store.config.BrokerRole;
import com.alibaba.rocketmq.store.config.MessageStoreConfig;
import com.alibaba.rocketmq.store.stats.BrokerStats;
import com.alibaba.rocketmq.store.stats.BrokerStatsManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.*;


/**
 * broker������
 * @author shijia.wxr
 */
public class BrokerController {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.BrokerLoggerName);
    private static final Logger logProtection = LoggerFactory.getLogger(LoggerName.ProtectionLoggerName);
    private static final Logger logWaterMark = LoggerFactory.getLogger(LoggerName.WaterMarkLoggerName);
    /**
     * �����ļ�
     */
    private final BrokerConfig brokerConfig;
    /**
     * broker server�����ļ�
     */
    private final NettyServerConfig nettyServerConfig;
    /**
     * �ͻ��������ļ�
     */
    private final NettyClientConfig nettyClientConfig;
    /**
     * ��Ϣ�洢�����ļ�
     */
    private final MessageStoreConfig messageStoreConfig;
    /**
     * ���ݰ汾
     */
    private final DataVersion configDataVersion = new DataVersion();
    /**
     * ����ƫ����������
     */
    private final ConsumerOffsetManager consumerOffsetManager;
    /**
     * �����߹���
     */
    private final ConsumerManager consumerManager;
    /**
     * �����߹���
     */
    private final ProducerManager producerManager;
    /**
     * �ͻ������ӱ��ֹ������
     */
    private final ClientHousekeepingService clientHousekeepingService;
    
    /**
     * 
     */
    private final PullMessageProcessor pullMessageProcessor;
    
    /**
     * ��ȡ������������
     */
    private final PullRequestHoldService pullRequestHoldService;

    /**
     * ��Ϣ���������
     */
    private final MessageArrivingListener messageArrivingListener;
    
    /**
     * 
     */
    private final Broker2Client broker2Client;
    
    /**
     * ��������� 
     */
    private final SubscriptionGroupManager subscriptionGroupManager;
    
    /**
     * ����id���¼�����
     * ���������߼��롢�˳���ʱ��֪ͨ����������
     * �Ա������Ѷ˽��и��ؾ���Ĵ���
     */
    private final ConsumerIdsChangeListener consumerIdsChangeListener;
    
    /**
     * ���ؾ�����й���
     */
    private final RebalanceLockManager rebalanceLockManager = new RebalanceLockManager();
    
    /**
     * broker���������
     * �������ķ������
     */
    private final BrokerOuterAPI brokerOuterAPI;
    
    /**
     * ������ȷ���
     */
    private final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryImpl(
            "BrokerControllerScheduledThread"));
    
    /**
     * slaveͬ������
     */
    private final SlaveSynchronize slaveSynchronize;
    
    /**
     * �����̶߳���
     */
    private final BlockingQueue<Runnable> sendThreadPoolQueue;
    
    /**
     * ��ȡ�̶߳���
     */
    private final BlockingQueue<Runnable> pullThreadPoolQueue;
    
    /**
     * ���˷������
     */
    private final FilterServerManager filterServerManager;
    
    /**
     * broker��ͳ��״̬����
     */
    private final BrokerStatsManager brokerStatsManager;
    
    /**
     * ��Ϣ����hook�б�
     */
    private final List<SendMessageHook> sendMessageHookList = new ArrayList<SendMessageHook>();
    
    /**
     * ������Ϣhook�б�
     */
    private final List<ConsumeMessageHook> consumeMessageHookList = new ArrayList<ConsumeMessageHook>();
    
    /**
     * ��Ϣ�洢������
     */
    private MessageStore messageStore;
    
    /**
     * ��������socketServer
     * ��Ҫ���� producer ��consumer
     */
    private RemotingServer remotingServer;
    
    /**
     * ha server 
     * ��Ҫ��slaveͬ��
     */
    private RemotingServer fastRemotingServer;
    
    /**
     * topic���ù������
     * ��Ҫ�����ʼ��ϵͳ��һЩtopic
     * ά��brokerϵͳ��ǰ��topic�б��������Ϣ
     */
    private TopicConfigManager topicConfigManager;
    
    /**
     * �����̳߳�
     */
    private ExecutorService sendMessageExecutor;
    
    /**
     * ��ȡ��Ϣ�̳߳�
     */
    private ExecutorService pullMessageExecutor;
    
    /**
     * broker�����̳߳�
     */
    private ExecutorService adminBrokerExecutor;
    
    /**
     * �ͻ��˹����̳߳�
     */
    private ExecutorService clientManageExecutor;
    
    /**
     * ���ڸ���master��ha�����ַ
     */
    private boolean updateMasterHAServerAddrPeriodically = false;
    
    /**
     * broker��Ϣ����ͳ��
     * ��Ҫ����2��ָ��
     * ���졢����Ľ��ա���������ͳ��
     */
    private BrokerStats brokerStats;
    
    /**
     * ��Ϣ�洢������ַ
     */
    private InetSocketAddress storeHost;
    
    /**
     * ��������
     */
    private BrokerFastFailure brokerFastFailure;

    public BrokerController(//
                            final BrokerConfig brokerConfig, //
                            final NettyServerConfig nettyServerConfig, //
                            final NettyClientConfig nettyClientConfig, //
                            final MessageStoreConfig messageStoreConfig //
    ) {
        this.brokerConfig = brokerConfig;
        this.nettyServerConfig = nettyServerConfig;
        this.nettyClientConfig = nettyClientConfig;
        this.messageStoreConfig = messageStoreConfig;
        this.consumerOffsetManager = new ConsumerOffsetManager(this);
        this.topicConfigManager = new TopicConfigManager(this);
        this.pullMessageProcessor = new PullMessageProcessor(this);
        this.pullRequestHoldService = new PullRequestHoldService(this);
        this.messageArrivingListener = new NotifyMessageArrivingListener(this.pullRequestHoldService);
        this.consumerIdsChangeListener = new DefaultConsumerIdsChangeListener(this);
        this.consumerManager = new ConsumerManager(this.consumerIdsChangeListener);
        this.producerManager = new ProducerManager();
        this.clientHousekeepingService = new ClientHousekeepingService(this);
        this.broker2Client = new Broker2Client(this);
        this.subscriptionGroupManager = new SubscriptionGroupManager(this);
        this.brokerOuterAPI = new BrokerOuterAPI(nettyClientConfig);
        this.filterServerManager = new FilterServerManager(this);

        /**
         * ����namesrv��ַ�Ļ� ����brokeroutapi��namesrv��ַ
         */
        if (this.brokerConfig.getNamesrvAddr() != null) {
            this.brokerOuterAPI.updateNameServerAddressList(this.brokerConfig.getNamesrvAddr());
            log.info("user specfied name server address: {}", this.brokerConfig.getNamesrvAddr());
        }

        /**
         * broker��slaveͬ��
         */
        this.slaveSynchronize = new SlaveSynchronize(this);

        /**
         * �����̳߳ض���
         * Ĭ������ 65535
         */
        this.sendThreadPoolQueue = new LinkedBlockingQueue<Runnable>(this.brokerConfig.getSendThreadPoolQueueCapacity());

        /**
         * ��ȡ�̳߳ض���
         * Ĭ������ 65535
         */
        this.pullThreadPoolQueue = new LinkedBlockingQueue<Runnable>(this.brokerConfig.getPullThreadPoolQueueCapacity());

        /**
         * broker״̬����
         * ��Ҫ���� broker�ĸ���ͳ��״̬����ά��
         */
        this.brokerStatsManager = new BrokerStatsManager(this.brokerConfig.getBrokerClusterName());
        
        /**
         * ���ô洢����ip�˿ں�
         * Ĭ�ϵľ��� broker����Ķ˿ں�   ip:listenPort
         */
        this.setStoreHost(new InetSocketAddress(this.getBrokerConfig().getBrokerIP1(), this.getNettyServerConfig().getListenPort()));

        /**
         * broker���ض���
         */
        this.brokerFastFailure = new BrokerFastFailure(this);
    }

    public BrokerConfig getBrokerConfig() {
        return brokerConfig;
    }

    public NettyServerConfig getNettyServerConfig() {
        return nettyServerConfig;
    }

    public BlockingQueue<Runnable> getPullThreadPoolQueue() {
        return pullThreadPoolQueue;
    }

    /**
     * @����: broker��ʼ�� 
     * @����: zgzhang@txbds.com
     * @����:2016��11��23��
     * @�޸�����
     * @������@return
     * @������@throws CloneNotSupportedException     
     * @return boolean   
     * @throws
     */
    public boolean initialize() throws CloneNotSupportedException 
    {
        boolean result = true;

        /**
         * ����topic����
         * �������ļ��м���
         */
        result = result && this.topicConfigManager.load();

        /**
         * �������ѽ���
         */
        result = result && this.consumerOffsetManager.load();
        
        /**
         * ���ض�������Ϣ
         */
        result = result && this.subscriptionGroupManager.load();

        if (result) {
            try {
            	/**
            	 * ʹ��DefaultMessageStore��ʼ����Ϣ�洢����
            	 */
                this.messageStore =
                        new DefaultMessageStore(this.messageStoreConfig, this.brokerStatsManager, this.messageArrivingListener,
                                this.brokerConfig);
                this.brokerStats = new BrokerStats((DefaultMessageStore) this.messageStore);
                //load plugin
                MessageStorePluginContext context = new MessageStorePluginContext(messageStoreConfig, brokerStatsManager, messageArrivingListener, brokerConfig);
                this.messageStore = MessageStoreFactory.build(context, this.messageStore);
            } catch (IOException e) {
                result = false;
                e.printStackTrace();
            }
        }

        /**
         * ��Ϣ�洢��Ϣ����
         */
        result = result && this.messageStore.load();

        if (result) 
        {
        	/**
        	 * ��ʼ��10911 Ĭ�ϵ�broker��ַ�ķ���
        	 */
            this.remotingServer = new NettyRemotingServer(this.nettyServerConfig, this.clientHousekeepingService);
            
            /**
             * fast������
             * �˿�Ϊbroker - 2
             */
            NettyServerConfig fastConfig = (NettyServerConfig) this.nettyServerConfig.clone();
            /**
             * listenPort - 2 Ĭ��10909
             */
            fastConfig.setListenPort(nettyServerConfig.getListenPort() - 2);
            this.fastRemotingServer = new NettyRemotingServer(fastConfig, this.clientHousekeepingService);
            
            this.sendMessageExecutor = new BrokerFixedThreadPoolExecutor(//
                    this.brokerConfig.getSendMessageThreadPoolNums(),//
                    this.brokerConfig.getSendMessageThreadPoolNums(),//
                    1000 * 60,//
                    TimeUnit.MILLISECONDS,//
                    this.sendThreadPoolQueue,//
                    new ThreadFactoryImpl("SendMessageThread_"));

            this.pullMessageExecutor = new BrokerFixedThreadPoolExecutor(//
                    this.brokerConfig.getPullMessageThreadPoolNums(),//
                    this.brokerConfig.getPullMessageThreadPoolNums(),//
                    1000 * 60,//
                    TimeUnit.MILLISECONDS,//
                    this.pullThreadPoolQueue,//
                    new ThreadFactoryImpl("PullMessageThread_"));

            this.adminBrokerExecutor =
                    Executors.newFixedThreadPool(this.brokerConfig.getAdminBrokerThreadPoolNums(), new ThreadFactoryImpl(
                            "AdminBrokerThread_"));

            this.clientManageExecutor =
                    Executors.newFixedThreadPool(this.brokerConfig.getClientManageThreadPoolNums(), new ThreadFactoryImpl(
                            "ClientManageThread_"));

            this.registerProcessor();


            // TODO remove in future
            final long initialDelay = UtilAll.computNextMorningTimeMillis() - System.currentTimeMillis();
            final long period = 1000 * 60 * 60 * 24;
            this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
                @Override
                public void run() {
                    try {
                        BrokerController.this.getBrokerStats().record();
                    } catch (Throwable e) {
                        log.error("schedule record error.", e);
                    }
                }
            }, initialDelay, period, TimeUnit.MILLISECONDS);

            this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
                @Override
                public void run() {
                    try {
                        BrokerController.this.consumerOffsetManager.persist();
                    } catch (Throwable e) {
                        log.error("schedule persist consumerOffset error.", e);
                    }
                }
            }, 1000 * 10, this.brokerConfig.getFlushConsumerOffsetInterval(), TimeUnit.MILLISECONDS);

            /**
             * broker����
             * 3��һ�ε���
             * ͨ�������������3��������Ƿ�������16M��Ϣ��ȷ�����ѽ����Ƿ��ӳ�
             * ���������    �����ӳ� �رյĻ�������������disable��, ������broker���ȶ���
             * ֻ����ȷ����Ҫ�����ܵ�����²����øñ�־
             * �ο���com.alibaba.rocketmq.common.BrokerConfig �� disableConsumeIfConsumerReadSlowly��consumerFallbehindThreshold
             */
            this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
                @Override
                public void run() {
                    try {
                        BrokerController.this.protectBroker();
                    } catch (Exception e) {
                        log.error("protectBroker error.", e);
                    }
                }
            }, 3, 3, TimeUnit.MINUTES);

            /**
             * ��ʱ��ӡ��ǰbroker�ĸ���
             */
            this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
                @Override
                public void run() {
                    try {
                        BrokerController.this.printWaterMark();
                    } catch (Exception e) {
                        log.error("printWaterMark error.", e);
                    }
                }
            }, 10, 1, TimeUnit.SECONDS);

            this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {

                @Override
                public void run() {
                    try {
                        log.info("dispatch behind commit log {} bytes", BrokerController.this.getMessageStore().dispatchBehindBytes());
                    } catch (Throwable e) {
                        log.error("schedule dispatchBehindBytes error.", e);
                    }
                }
            }, 1000 * 10, 1000 * 60, TimeUnit.MILLISECONDS);

            /**
             * ����������ļ����Ѿ��������Ʒ����ַnamesrvaddr
             */
            if (this.brokerConfig.getNamesrvAddr() != null) 
            {
                this.brokerOuterAPI.updateNameServerAddressList(this.brokerConfig.getNamesrvAddr());
            }
            /**
             * �����ļ���û���������Ʒ����ַnamesrvaddr
             */
            else if (this.brokerConfig.isFetchNamesrvAddrByAddressServer()) 
            {
                this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            BrokerController.this.brokerOuterAPI.fetchNameServerAddr();
                        } catch (Throwable e) {
                            log.error("ScheduledTask fetchNameServerAddr exception", e);
                        }
                    }
                }, 1000 * 10, 1000 * 60 * 2, TimeUnit.MILLISECONDS);
            }

            /**
             * ���broker��slave�Ļ�
             */
            if (BrokerRole.SLAVE == this.messageStoreConfig.getBrokerRole()) {
            	
            	/**
            	 * ���ha��ַ���� ������Ϣ�洢��ha��ַ
            	 */
                if (this.messageStoreConfig.getHaMasterAddress() != null && this.messageStoreConfig.getHaMasterAddress().length() >= 6) 
                {
                    this.messageStore.updateHaMasterAddress(this.messageStoreConfig.getHaMasterAddress());
                    this.updateMasterHAServerAddrPeriodically = false;
                } 
                else 
                {
                    this.updateMasterHAServerAddrPeriodically = true;
                }

                this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {

                    @Override
                    public void run() {
                        try {
                            BrokerController.this.slaveSynchronize.syncAll();
                        } catch (Throwable e) {
                            log.error("ScheduledTask syncAll slave exception", e);
                        }
                    }
                }, 1000 * 10, 1000 * 60, TimeUnit.MILLISECONDS);
            } 
            else 
            {
            	/**
            	 * 
            	 */
                this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {

                    @Override
                    public void run() {
                        try {
                            BrokerController.this.printMasterAndSlaveDiff();
                        } catch (Throwable e) {
                            log.error("schedule printMasterAndSlaveDiff error.", e);
                        }
                    }
                }, 1000 * 10, 1000 * 60, TimeUnit.MILLISECONDS);
            }
        }

        return result;
    }

    public void registerProcessor() {
        /**
         * SendMessageProcessor
         */
        SendMessageProcessor sendProcessor = new SendMessageProcessor(this);
        sendProcessor.registerSendMessageHook(sendMessageHookList);
        sendProcessor.registerConsumeMessageHook(consumeMessageHookList);

        this.remotingServer.registerProcessor(RequestCode.SEND_MESSAGE, sendProcessor, this.sendMessageExecutor);
        this.remotingServer.registerProcessor(RequestCode.SEND_MESSAGE_V2, sendProcessor, this.sendMessageExecutor);
        this.remotingServer.registerProcessor(RequestCode.CONSUMER_SEND_MSG_BACK, sendProcessor, this.sendMessageExecutor);
        this.fastRemotingServer.registerProcessor(RequestCode.SEND_MESSAGE, sendProcessor, this.sendMessageExecutor);
        this.fastRemotingServer.registerProcessor(RequestCode.SEND_MESSAGE_V2, sendProcessor, this.sendMessageExecutor);
        this.fastRemotingServer.registerProcessor(RequestCode.CONSUMER_SEND_MSG_BACK, sendProcessor, this.sendMessageExecutor);

        /**
         * PullMessageProcessor
         */
        this.remotingServer.registerProcessor(RequestCode.PULL_MESSAGE, this.pullMessageProcessor, this.pullMessageExecutor);
        this.pullMessageProcessor.registerConsumeMessageHook(consumeMessageHookList);

        /**
         * ��ѯ��Ϣ
         * QueryMessageProcessor
         */
        NettyRequestProcessor queryProcessor = new QueryMessageProcessor(this);
        this.remotingServer.registerProcessor(RequestCode.QUERY_MESSAGE, queryProcessor, this.pullMessageExecutor);
        this.remotingServer.registerProcessor(RequestCode.VIEW_MESSAGE_BY_ID, queryProcessor, this.pullMessageExecutor);

        this.fastRemotingServer.registerProcessor(RequestCode.QUERY_MESSAGE, queryProcessor, this.pullMessageExecutor);
        this.fastRemotingServer.registerProcessor(RequestCode.VIEW_MESSAGE_BY_ID, queryProcessor, this.pullMessageExecutor);

        /**
         * ClientManageProcessor
         */
        ClientManageProcessor clientProcessor = new ClientManageProcessor(this);
        
        /**
         * ������ע���ͻ��ˡ��������ȡ�ͻ����б�
         */
        this.remotingServer.registerProcessor(RequestCode.HEART_BEAT, clientProcessor, this.clientManageExecutor);
        this.remotingServer.registerProcessor(RequestCode.UNREGISTER_CLIENT, clientProcessor, this.clientManageExecutor);
        this.remotingServer.registerProcessor(RequestCode.GET_CONSUMER_LIST_BY_GROUP, clientProcessor, this.clientManageExecutor);


        this.fastRemotingServer.registerProcessor(RequestCode.HEART_BEAT, clientProcessor, this.clientManageExecutor);
        this.fastRemotingServer.registerProcessor(RequestCode.UNREGISTER_CLIENT, clientProcessor, this.clientManageExecutor);
        this.fastRemotingServer.registerProcessor(RequestCode.GET_CONSUMER_LIST_BY_GROUP, clientProcessor, this.clientManageExecutor);
        
        /**
         * ��ѯ���������ѽ���
         */
        this.remotingServer.registerProcessor(RequestCode.UPDATE_CONSUMER_OFFSET, clientProcessor, this.clientManageExecutor);
        this.remotingServer.registerProcessor(RequestCode.QUERY_CONSUMER_OFFSET, clientProcessor, this.clientManageExecutor);

        this.fastRemotingServer.registerProcessor(RequestCode.UPDATE_CONSUMER_OFFSET, clientProcessor, this.clientManageExecutor);
        this.fastRemotingServer.registerProcessor(RequestCode.QUERY_CONSUMER_OFFSET, clientProcessor, this.clientManageExecutor);

        /**
         * EndTransactionProcessor
         */
        this.remotingServer.registerProcessor(RequestCode.END_TRANSACTION, new EndTransactionProcessor(this), this.sendMessageExecutor);
        this.fastRemotingServer.registerProcessor(RequestCode.END_TRANSACTION, new EndTransactionProcessor(this), this.sendMessageExecutor);

        /**
         * Default
         */
        AdminBrokerProcessor adminProcessor = new AdminBrokerProcessor(this);
        this.remotingServer.registerDefaultProcessor(adminProcessor, this.adminBrokerExecutor);
        this.fastRemotingServer.registerDefaultProcessor(adminProcessor, this.adminBrokerExecutor);
    }

    public BrokerStats getBrokerStats() {
        return brokerStats;
    }

    public void setBrokerStats(BrokerStats brokerStats) {
        this.brokerStats = brokerStats;
    }

    /**
     * @����: broker����
     * 		 
     * @����: zgzhang@txbds.com
     * @����:2016��11��24��
     * @�޸�����
     * @������     
     * @throws
     */
    public void protectBroker() {
    	/**
    	 * disableConsumeIfConsumerReadSlowly default is false
    	 */
        if (this.brokerConfig.isDisableConsumeIfConsumerReadSlowly()) {
            final Iterator<Map.Entry<String, MomentStatsItem>> it = this.brokerStatsManager.getMomentStatsItemSetFallSize().getStatsItemTable().entrySet().iterator();
            while (it.hasNext()) {
                final Map.Entry<String, MomentStatsItem> next = it.next();
                /**
                 * �����˶����ֽ���Ϣ
                 */
                final long fallBehindBytes = next.getValue().getValue().get();
                if (fallBehindBytes > this.brokerConfig.getConsumerFallbehindThreshold()) {
                    final String[] split = next.getValue().getStatsKey().split("@");
                    final String group = split[2];
                    logProtection.info("[PROTECT_BROKER] the consumer[{}] consume slowly, {} bytes, disable it", group, fallBehindBytes);
                    this.subscriptionGroupManager.disableConsume(group);
                }
            }
        }
    }

    /**
     * @����:  ��ȡ����ͷԪ�ص�������ȴ��¼�
     * 			���㷽ʽ����Ϣ�洢ʱ��  - ͷԪ�����񴴽��¼�
     * @����: zgzhang@txbds.com
     * @����:2016��11��24��
     * @�޸�����
     * @������@param q
     * @������@return     
     * @throws
     */
    public long headSlowTimeMills(BlockingQueue<Runnable> q) {
        long slowTimeMills = 0;
        /**
         * ��ȡͷԪ�أ����ǲ��Ƴ�ͷԪ��
         */
        final Runnable peek = q.peek();
        if (peek != null) {
            RequestTask rt = BrokerFastFailure.castRunnable(peek);
            slowTimeMills = this.messageStore.now() - rt.getCreateTimestamp();
        }

        if (slowTimeMills < 0) slowTimeMills = 0;

        return slowTimeMills;
    }

    /**
     * @����:  �����̳߳�����������ӳ��¼�
     * @����: zgzhang@txbds.com
     * @����:2016��11��24��
     * @�޸�����
     * @������@return     
     * @throws
     */
    public long headSlowTimeMills4SendThreadPoolQueue() {
        return this.headSlowTimeMills(this.sendThreadPoolQueue);
    }

    /**
     * @����: ��ȡ�̳߳�����������ӳ�ʱ�� 
     * @����: zgzhang@txbds.com
     * @����:2016��11��24��
     * @�޸�����
     * @������@return     
     * @throws
     */
    public long headSlowTimeMills4PullThreadPoolQueue() {
        return this.headSlowTimeMills(this.pullThreadPoolQueue);
    }

    /**
     * @����: ��ӡҵ��Ķ��д�С�����紴���¼�
     * 			�Դ�����Ӧ��ǰ�����ٶȵ�ˮƽ��
     * @����: zgzhang@txbds.com
     * @����:2016��11��24��
     * @�޸�����
     * @������     
     * @throws
     */
    public void printWaterMark() {
        logWaterMark.info("[WATERMARK] Send Queue Size: {} SlowTimeMills: {}", this.sendThreadPoolQueue.size(), headSlowTimeMills4SendThreadPoolQueue());
        logWaterMark.info("[WATERMARK] Pull Queue Size: {} SlowTimeMills: {}", this.pullThreadPoolQueue.size(), headSlowTimeMills4PullThreadPoolQueue());
    }

    public MessageStore getMessageStore() {
        return messageStore;
    }

    public void setMessageStore(MessageStore messageStore) {
        this.messageStore = messageStore;
    }

    private void printMasterAndSlaveDiff() {
        long diff = this.messageStore.slaveFallBehindMuch();

        // XXX: warn and notify me
        log.info("slave fall behind master, how much, {} bytes", diff);
    }

    public Broker2Client getBroker2Client() {
        return broker2Client;
    }

    public String getConfigDataVersion() {
        return this.configDataVersion.toJson();
    }

    public ConsumerManager getConsumerManager() {
        return consumerManager;
    }

    public ConsumerOffsetManager getConsumerOffsetManager() {
        return consumerOffsetManager;
    }

    public MessageStoreConfig getMessageStoreConfig() {
        return messageStoreConfig;
    }

    public ProducerManager getProducerManager() {
        return producerManager;
    }

    public void setFastRemotingServer(RemotingServer fastRemotingServer) {
        this.fastRemotingServer = fastRemotingServer;
    }

    public PullMessageProcessor getPullMessageProcessor() {
        return pullMessageProcessor;
    }

    public PullRequestHoldService getPullRequestHoldService() {
        return pullRequestHoldService;
    }

    public SubscriptionGroupManager getSubscriptionGroupManager() {
        return subscriptionGroupManager;
    }

    public void shutdown() {
        if (this.brokerStatsManager != null) {
            this.brokerStatsManager.shutdown();
        }

        if (this.clientHousekeepingService != null) {
            this.clientHousekeepingService.shutdown();
        }

        if (this.pullRequestHoldService != null) {
            this.pullRequestHoldService.shutdown();
        }

        if (this.remotingServer != null) {
            this.remotingServer.shutdown();
        }

        if (this.fastRemotingServer != null) {
            this.fastRemotingServer.shutdown();
        }

        if (this.messageStore != null) {
            this.messageStore.shutdown();
        }

        this.scheduledExecutorService.shutdown();
        try {
            this.scheduledExecutorService.awaitTermination(5000, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
        }

        this.unregisterBrokerAll();

        if (this.sendMessageExecutor != null) {
            this.sendMessageExecutor.shutdown();
        }

        if (this.pullMessageExecutor != null) {
            this.pullMessageExecutor.shutdown();
        }

        if (this.adminBrokerExecutor != null) {
            this.adminBrokerExecutor.shutdown();
        }

        if (this.brokerOuterAPI != null) {
            this.brokerOuterAPI.shutdown();
        }

        this.consumerOffsetManager.persist();

        if (this.filterServerManager != null) {
            this.filterServerManager.shutdown();
        }

        if (this.brokerFastFailure != null) {
            this.brokerFastFailure.shutdown();
        }
    }

    private void unregisterBrokerAll() {
        this.brokerOuterAPI.unregisterBrokerAll(//
                this.brokerConfig.getBrokerClusterName(), //
                this.getBrokerAddr(), //
                this.brokerConfig.getBrokerName(), //
                this.brokerConfig.getBrokerId());
    }

    public String getBrokerAddr() {
        String addr = this.brokerConfig.getBrokerIP1() + ":" + this.nettyServerConfig.getListenPort();
        return addr;
    }

    /**
     * @����: ����broker���� 
     * @����: zgzhang@txbds.com
     * @����:2016��11��24��
     * @�޸�����
     * @������@throws Exception     
     * @throws
     */
    public void start() throws Exception {
        if (this.messageStore != null) {
            this.messageStore.start();
        }

        if (this.remotingServer != null) {
            this.remotingServer.start();
        }

        if (this.fastRemotingServer != null) {
            this.fastRemotingServer.start();
        }

        if (this.brokerOuterAPI != null) {
            this.brokerOuterAPI.start();
        }

        if (this.pullRequestHoldService != null) {
            this.pullRequestHoldService.start();
        }

        if (this.clientHousekeepingService != null) {
            this.clientHousekeepingService.start();
        }

        if (this.filterServerManager != null) {
            this.filterServerManager.start();
        }

        /**
         * ע��broker��namesrv��ȥ
         */
        this.registerBrokerAll(true, false);

        /**
         * ��ʱע��broker��namesrv��ȥ
         * ����10�������
         * ÿ��30�붨ʱע��broker��namesrv��ȥ
         */
        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {

            @Override
            public void run() {
                try {
                    BrokerController.this.registerBrokerAll(true, false);
                } catch (Throwable e) {
                    log.error("registerBrokerAll Exception", e);
                }
            }
        }, 1000 * 10, 1000 * 30, TimeUnit.MILLISECONDS);

        /**
         * broker״̬����
         */
        if (this.brokerStatsManager != null) {
            this.brokerStatsManager.start();
        }

        /**
         * ����
         */
        if (this.brokerFastFailure != null) {
            this.brokerFastFailure.start();
        }
    }

    /**
     * @����: ע��broker��namesrv��ȥ 
     * @����: zgzhang@txbds.com
     * @����:2016��11��24��
     * @�޸�����
     * @������@param checkOrderConfig  
     * @������@param oneway     
     * @throws
     */
    public synchronized void registerBrokerAll(final boolean checkOrderConfig, boolean oneway) 
    {
    	/**
    	 * topic�������л���װ����
    	 */
        TopicConfigSerializeWrapper topicConfigWrapper = this.getTopicConfigManager().buildTopicConfigSerializeWrapper();

        /**
         * ���brokerû�ж�дȨ�� ��Ҫ����broker�������ļ�����Ȩ��
         */
        if (!PermName.isWriteable(this.getBrokerConfig().getBrokerPermission())
                || !PermName.isReadable(this.getBrokerConfig().getBrokerPermission())) 
        {
            ConcurrentHashMap<String, TopicConfig> topicConfigTable = new ConcurrentHashMap<String, TopicConfig>();
            for (TopicConfig topicConfig : topicConfigWrapper.getTopicConfigTable().values()) {
                TopicConfig tmp =
                        new TopicConfig(topicConfig.getTopicName(), 		//topic����
                        		topicConfig.getReadQueueNums(), 			//�����д�С
                        		topicConfig.getWriteQueueNums(),			//д���д�С
                                this.brokerConfig.getBrokerPermission());	//Ȩ��
                topicConfigTable.put(topicConfig.getTopicName(), tmp);
            }
            topicConfigWrapper.setTopicConfigTable(topicConfigTable);
        }

        /**
         * ע��broker��namesrv��ȥ
         */
        RegisterBrokerResult registerBrokerResult = this.brokerOuterAPI.registerBrokerAll(
                this.brokerConfig.getBrokerClusterName(), //broker���õļ�Ⱥ����
                this.getBrokerAddr(), //   broker��ַ
                this.brokerConfig.getBrokerName(), // broker����
                this.brokerConfig.getBrokerId(), // brokerid
                this.getHAServerAddr(), // ha�ķ����ַ
                topicConfigWrapper,// topic�����ļ�
                this.filterServerManager.buildNewFilterServerList(),//���˷����б�
                oneway,//�Ƿ�ѡ��oneway��ʽ  false
                this.brokerConfig.getRegisterBrokerTimeoutMills()); //��ʱʱ��

        if (registerBrokerResult != null) {
        	/**
        	 * ����messageStore��hamaster��ַ
        	 */
            if (this.updateMasterHAServerAddrPeriodically && registerBrokerResult.getHaServerAddr() != null) 
            {
                this.messageStore.updateHaMasterAddress(registerBrokerResult.getHaServerAddr());
            }

            /**
             * ����slave ͬ�����ݵ�master brokerԴ��ַ
             */
            this.slaveSynchronize.setMasterAddr(registerBrokerResult.getMasterAddr());

            /**
             * �Ƿ���ݷ��ؽ������topic�������ļ��е�order��־
             * 
             */
            if (checkOrderConfig) {
                this.getTopicConfigManager().updateOrderTopicConfig(registerBrokerResult.getKvTable());
            }
        }
    }

    public TopicConfigManager getTopicConfigManager() {
        return topicConfigManager;
    }

    public void setTopicConfigManager(TopicConfigManager topicConfigManager) {
        this.topicConfigManager = topicConfigManager;
    }

    /**
     * @����: message ha listen port 
     * @����: zgzhang@txbds.com
     * @����: 2016��11��25��
     * @�޸�����
     * @������@return     
     * @throws
     */
    public String getHAServerAddr() {
        String addr = this.brokerConfig.getBrokerIP2() + ":" + this.messageStoreConfig.getHaListenPort();
        return addr;
    }

    public void updateAllConfig(Properties properties) {
        MixAll.properties2Object(properties, brokerConfig);
        MixAll.properties2Object(properties, nettyServerConfig);
        MixAll.properties2Object(properties, nettyClientConfig);
        MixAll.properties2Object(properties, messageStoreConfig);
        this.configDataVersion.nextVersion();
        this.flushAllConfig();
    }

    private void flushAllConfig() {
        String allConfig = this.encodeAllConfig();
        try {
            MixAll.string2File(allConfig, BrokerPathConfigHelper.getBrokerConfigPath());
            log.info("flush broker config, {} OK", BrokerPathConfigHelper.getBrokerConfigPath());
        } catch (IOException e) {
            log.info("flush broker config Exception, " + BrokerPathConfigHelper.getBrokerConfigPath(), e);
        }
    }

    public String encodeAllConfig() {
        StringBuilder sb = new StringBuilder();
        {
            Properties properties = MixAll.object2Properties(this.brokerConfig);
            if (properties != null) {
                sb.append(MixAll.properties2String(properties));
            } else {
                log.error("encodeAllConfig object2Properties error");
            }
        }

        {
            Properties properties = MixAll.object2Properties(this.messageStoreConfig);
            if (properties != null) {
                sb.append(MixAll.properties2String(properties));
            } else {
                log.error("encodeAllConfig object2Properties error");
            }
        }

        {
            Properties properties = MixAll.object2Properties(this.nettyServerConfig);
            if (properties != null) {
                sb.append(MixAll.properties2String(properties));
            } else {
                log.error("encodeAllConfig object2Properties error");
            }
        }

        {
            Properties properties = MixAll.object2Properties(this.nettyClientConfig);
            if (properties != null) {
                sb.append(MixAll.properties2String(properties));
            } else {
                log.error("encodeAllConfig object2Properties error");
            }
        }
        return sb.toString();
    }

    public RebalanceLockManager getRebalanceLockManager() {
        return rebalanceLockManager;
    }

    public SlaveSynchronize getSlaveSynchronize() {
        return slaveSynchronize;
    }

    public ExecutorService getPullMessageExecutor() {
        return pullMessageExecutor;
    }

    public void setPullMessageExecutor(ExecutorService pullMessageExecutor) {
        this.pullMessageExecutor = pullMessageExecutor;
    }

    public BlockingQueue<Runnable> getSendThreadPoolQueue() {
        return sendThreadPoolQueue;
    }

    public FilterServerManager getFilterServerManager() {
        return filterServerManager;
    }

    public BrokerStatsManager getBrokerStatsManager() {
        return brokerStatsManager;
    }

    public List<SendMessageHook> getSendMessageHookList() {
        return sendMessageHookList;
    }

    public void registerSendMessageHook(final SendMessageHook hook) {
        this.sendMessageHookList.add(hook);
        log.info("register SendMessageHook Hook, {}", hook.hookName());
    }

    public List<ConsumeMessageHook> getConsumeMessageHookList() {
        return consumeMessageHookList;
    }

    public void registerConsumeMessageHook(final ConsumeMessageHook hook) {
        this.consumeMessageHookList.add(hook);
        log.info("register ConsumeMessageHook Hook, {}", hook.hookName());
    }

    public void registerServerRPCHook(RPCHook rpcHook) {
        getRemotingServer().registerRPCHook(rpcHook);
    }

    public RemotingServer getRemotingServer() {
        return remotingServer;
    }

    public void setRemotingServer(RemotingServer remotingServer) {
        this.remotingServer = remotingServer;
    }

    public void registerClientRPCHook(RPCHook rpcHook) {
        this.getBrokerOuterAPI().registerRPCHook(rpcHook);
    }

    public BrokerOuterAPI getBrokerOuterAPI() {
        return brokerOuterAPI;
    }

    public InetSocketAddress getStoreHost() {
        return storeHost;
    }

    public void setStoreHost(InetSocketAddress storeHost) {
        this.storeHost = storeHost;
    }
}
