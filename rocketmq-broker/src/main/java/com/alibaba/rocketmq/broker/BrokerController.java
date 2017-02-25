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
 * broker控制器
 * @author shijia.wxr
 */
public class BrokerController {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.BrokerLoggerName);
    private static final Logger logProtection = LoggerFactory.getLogger(LoggerName.ProtectionLoggerName);
    private static final Logger logWaterMark = LoggerFactory.getLogger(LoggerName.WaterMarkLoggerName);
    /**
     * 配置文件
     */
    private final BrokerConfig brokerConfig;
    /**
     * broker server配置文件
     */
    private final NettyServerConfig nettyServerConfig;
    /**
     * 客户端配置文件
     */
    private final NettyClientConfig nettyClientConfig;
    /**
     * 消息存储配置文件
     */
    private final MessageStoreConfig messageStoreConfig;
    /**
     * 数据版本
     */
    private final DataVersion configDataVersion = new DataVersion();
    /**
     * 消费偏移量管理器
     */
    private final ConsumerOffsetManager consumerOffsetManager;
    /**
     * 消费者管理
     */
    private final ConsumerManager consumerManager;
    /**
     * 生产者管理
     */
    private final ProducerManager producerManager;
    /**
     * 客户端连接保持管理服务
     */
    private final ClientHousekeepingService clientHousekeepingService;
    
    /**
     * 
     */
    private final PullMessageProcessor pullMessageProcessor;
    
    /**
     * 拉取请求批量处理
     */
    private final PullRequestHoldService pullRequestHoldService;

    /**
     * 消息到达监听器
     */
    private final MessageArrivingListener messageArrivingListener;
    
    /**
     * 
     */
    private final Broker2Client broker2Client;
    
    /**
     * 订阅组管理 
     */
    private final SubscriptionGroupManager subscriptionGroupManager;
    
    /**
     * 消费id更新监听器
     * 在有消费者加入、退出的时候通知其他消费者
     * 以便在消费端进行负载均衡的处理
     */
    private final ConsumerIdsChangeListener consumerIdsChangeListener;
    
    /**
     * 负载均衡队列管理
     */
    private final RebalanceLockManager rebalanceLockManager = new RebalanceLockManager();
    
    /**
     * broker外层服务对象
     * 负责具体的服务调用
     */
    private final BrokerOuterAPI brokerOuterAPI;
    
    /**
     * 任务调度服务
     */
    private final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryImpl(
            "BrokerControllerScheduledThread"));
    
    /**
     * slave同步服务
     */
    private final SlaveSynchronize slaveSynchronize;
    
    /**
     * 发送线程队列
     */
    private final BlockingQueue<Runnable> sendThreadPoolQueue;
    
    /**
     * 拉取线程队列
     */
    private final BlockingQueue<Runnable> pullThreadPoolQueue;
    
    /**
     * 过滤服务管理
     */
    private final FilterServerManager filterServerManager;
    
    /**
     * broker的统计状态管理
     */
    private final BrokerStatsManager brokerStatsManager;
    
    /**
     * 消息发送hook列表
     */
    private final List<SendMessageHook> sendMessageHookList = new ArrayList<SendMessageHook>();
    
    /**
     * 消费信息hook列表
     */
    private final List<ConsumeMessageHook> consumeMessageHookList = new ArrayList<ConsumeMessageHook>();
    
    /**
     * 消息存储管理器
     */
    private MessageStore messageStore;
    
    /**
     * 对外服务的socketServer
     * 主要面向 producer 和consumer
     */
    private RemotingServer remotingServer;
    
    /**
     * ha server 
     * 主要做slave同步
     */
    private RemotingServer fastRemotingServer;
    
    /**
     * topic配置管理对象
     * 主要负责初始化系统的一些topic
     * 维护broker系统当前的topic列表和配置信息
     */
    private TopicConfigManager topicConfigManager;
    
    /**
     * 发送线程池
     */
    private ExecutorService sendMessageExecutor;
    
    /**
     * 拉取消息线程池
     */
    private ExecutorService pullMessageExecutor;
    
    /**
     * broker管理线程池
     */
    private ExecutorService adminBrokerExecutor;
    
    /**
     * 客户端管理线程池
     */
    private ExecutorService clientManageExecutor;
    
    /**
     * 定期更新master的ha服务地址
     */
    private boolean updateMasterHAServerAddrPeriodically = false;
    
    /**
     * broker消息数据统计
     * 主要包括2个指标
     * 昨天、今天的接收、发送数据统计
     */
    private BrokerStats brokerStats;
    
    /**
     * 消息存储主机地址
     */
    private InetSocketAddress storeHost;
    
    /**
     * 流量控制
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
         * 配置namesrv地址的话 更新brokeroutapi的namesrv地址
         */
        if (this.brokerConfig.getNamesrvAddr() != null) {
            this.brokerOuterAPI.updateNameServerAddressList(this.brokerConfig.getNamesrvAddr());
            log.info("user specfied name server address: {}", this.brokerConfig.getNamesrvAddr());
        }

        /**
         * broker的slave同步
         */
        this.slaveSynchronize = new SlaveSynchronize(this);

        /**
         * 发送线程池队列
         * 默认容量 65535
         */
        this.sendThreadPoolQueue = new LinkedBlockingQueue<Runnable>(this.brokerConfig.getSendThreadPoolQueueCapacity());

        /**
         * 拉取线程池队列
         * 默认容量 65535
         */
        this.pullThreadPoolQueue = new LinkedBlockingQueue<Runnable>(this.brokerConfig.getPullThreadPoolQueueCapacity());

        /**
         * broker状态管理
         * 主要负责 broker的各种统计状态管理维护
         */
        this.brokerStatsManager = new BrokerStatsManager(this.brokerConfig.getBrokerClusterName());
        
        /**
         * 设置存储主机ip端口号
         * 默认的就是 broker对外的端口号   ip:listenPort
         */
        this.setStoreHost(new InetSocketAddress(this.getBrokerConfig().getBrokerIP1(), this.getNettyServerConfig().getListenPort()));

        /**
         * broker流控对象
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
     * @描述: broker初始化 
     * @作者: zgzhang@txbds.com
     * @日期:2016年11月23日
     * @修改内容
     * @参数：@return
     * @参数：@throws CloneNotSupportedException     
     * @return boolean   
     * @throws
     */
    public boolean initialize() throws CloneNotSupportedException 
    {
        boolean result = true;

        /**
         * 加载topic配置
         * 从配置文件中加载
         */
        result = result && this.topicConfigManager.load();

        /**
         * 加载消费进度
         */
        result = result && this.consumerOffsetManager.load();
        
        /**
         * 加载订阅组信息
         */
        result = result && this.subscriptionGroupManager.load();

        if (result) {
            try {
            	/**
            	 * 使用DefaultMessageStore初始化消息存储对象
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
         * 消息存储信息加载
         */
        result = result && this.messageStore.load();

        if (result) 
        {
        	/**
        	 * 初始化10911 默认的broker地址的服务
        	 */
            this.remotingServer = new NettyRemotingServer(this.nettyServerConfig, this.clientHousekeepingService);
            
            /**
             * fast服务器
             * 端口为broker - 2
             */
            NettyServerConfig fastConfig = (NettyServerConfig) this.nettyServerConfig.clone();
            /**
             * listenPort - 2 默认10909
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
             * broker保护
             * 3秒一次调用
             * 通过检查消费者在3秒过程中是否消费了16M消息来确定消费进度是否延迟
             * 如果启用了    消费延迟 关闭的话，则会把消费者disable掉, 来保护broker的稳定性
             * 只有在确定需要高性能的情况下才启用该标志
             * 参考：com.alibaba.rocketmq.common.BrokerConfig 中 disableConsumeIfConsumerReadSlowly、consumerFallbehindThreshold
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
             * 定时打印当前broker的负载
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
             * 如果在配置文件中已经配置名称服务地址namesrvaddr
             */
            if (this.brokerConfig.getNamesrvAddr() != null) 
            {
                this.brokerOuterAPI.updateNameServerAddressList(this.brokerConfig.getNamesrvAddr());
            }
            /**
             * 配置文件中没有配置名称服务地址namesrvaddr
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
             * 如果broker是slave的话
             */
            if (BrokerRole.SLAVE == this.messageStoreConfig.getBrokerRole()) {
            	
            	/**
            	 * 如果ha地址存在 更新消息存储的ha地址
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
         * 查询消息
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
         * 心跳、注销客户端、根据组获取客户端列表
         */
        this.remotingServer.registerProcessor(RequestCode.HEART_BEAT, clientProcessor, this.clientManageExecutor);
        this.remotingServer.registerProcessor(RequestCode.UNREGISTER_CLIENT, clientProcessor, this.clientManageExecutor);
        this.remotingServer.registerProcessor(RequestCode.GET_CONSUMER_LIST_BY_GROUP, clientProcessor, this.clientManageExecutor);


        this.fastRemotingServer.registerProcessor(RequestCode.HEART_BEAT, clientProcessor, this.clientManageExecutor);
        this.fastRemotingServer.registerProcessor(RequestCode.UNREGISTER_CLIENT, clientProcessor, this.clientManageExecutor);
        this.fastRemotingServer.registerProcessor(RequestCode.GET_CONSUMER_LIST_BY_GROUP, clientProcessor, this.clientManageExecutor);
        
        /**
         * 查询、更新消费进度
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
     * @描述: broker保护
     * 		 
     * @作者: zgzhang@txbds.com
     * @日期:2016年11月24日
     * @修改内容
     * @参数：     
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
                 * 消费了多少字节消息
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
     * @描述:  获取队列头元素的慢处理等待事件
     * 			计算方式：消息存储时间  - 头元素任务创建事件
     * @作者: zgzhang@txbds.com
     * @日期:2016年11月24日
     * @修改内容
     * @参数：@param q
     * @参数：@return     
     * @throws
     */
    public long headSlowTimeMills(BlockingQueue<Runnable> q) {
        long slowTimeMills = 0;
        /**
         * 获取头元素，但是不移除头元素
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
     * @描述:  发送线程池最早任务的延迟事件
     * @作者: zgzhang@txbds.com
     * @日期:2016年11月24日
     * @修改内容
     * @参数：@return     
     * @throws
     */
    public long headSlowTimeMills4SendThreadPoolQueue() {
        return this.headSlowTimeMills(this.sendThreadPoolQueue);
    }

    /**
     * @描述: 拉取线程池最早任务的延迟时间 
     * @作者: zgzhang@txbds.com
     * @日期:2016年11月24日
     * @修改内容
     * @参数：@return     
     * @throws
     */
    public long headSlowTimeMills4PullThreadPoolQueue() {
        return this.headSlowTimeMills(this.pullThreadPoolQueue);
    }

    /**
     * @描述: 打印业务的队列大小和最早创建事件
     * 			以此来反应当前处理速度的水平线
     * @作者: zgzhang@txbds.com
     * @日期:2016年11月24日
     * @修改内容
     * @参数：     
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
     * @描述: 启动broker服务 
     * @作者: zgzhang@txbds.com
     * @日期:2016年11月24日
     * @修改内容
     * @参数：@throws Exception     
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
         * 注册broker到namesrv上去
         */
        this.registerBrokerAll(true, false);

        /**
         * 定时注册broker到namesrv上去
         * 启动10秒后运行
         * 每隔30秒定时注册broker到namesrv上去
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
         * broker状态管理
         */
        if (this.brokerStatsManager != null) {
            this.brokerStatsManager.start();
        }

        /**
         * 流控
         */
        if (this.brokerFastFailure != null) {
            this.brokerFastFailure.start();
        }
    }

    /**
     * @描述: 注册broker到namesrv上去 
     * @作者: zgzhang@txbds.com
     * @日期:2016年11月24日
     * @修改内容
     * @参数：@param checkOrderConfig  
     * @参数：@param oneway     
     * @throws
     */
    public synchronized void registerBrokerAll(final boolean checkOrderConfig, boolean oneway) 
    {
    	/**
    	 * topic配置序列化包装对象
    	 */
        TopicConfigSerializeWrapper topicConfigWrapper = this.getTopicConfigManager().buildTopicConfigSerializeWrapper();

        /**
         * 如果broker没有读写权限 需要根据broker的配置文件更新权限
         */
        if (!PermName.isWriteable(this.getBrokerConfig().getBrokerPermission())
                || !PermName.isReadable(this.getBrokerConfig().getBrokerPermission())) 
        {
            ConcurrentHashMap<String, TopicConfig> topicConfigTable = new ConcurrentHashMap<String, TopicConfig>();
            for (TopicConfig topicConfig : topicConfigWrapper.getTopicConfigTable().values()) {
                TopicConfig tmp =
                        new TopicConfig(topicConfig.getTopicName(), 		//topic名称
                        		topicConfig.getReadQueueNums(), 			//读队列大小
                        		topicConfig.getWriteQueueNums(),			//写队列大小
                                this.brokerConfig.getBrokerPermission());	//权限
                topicConfigTable.put(topicConfig.getTopicName(), tmp);
            }
            topicConfigWrapper.setTopicConfigTable(topicConfigTable);
        }

        /**
         * 注册broker到namesrv上去
         */
        RegisterBrokerResult registerBrokerResult = this.brokerOuterAPI.registerBrokerAll(
                this.brokerConfig.getBrokerClusterName(), //broker配置的集群名称
                this.getBrokerAddr(), //   broker地址
                this.brokerConfig.getBrokerName(), // broker名称
                this.brokerConfig.getBrokerId(), // brokerid
                this.getHAServerAddr(), // ha的服务地址
                topicConfigWrapper,// topic配置文件
                this.filterServerManager.buildNewFilterServerList(),//过滤服务列表
                oneway,//是否选择oneway方式  false
                this.brokerConfig.getRegisterBrokerTimeoutMills()); //超时时间

        if (registerBrokerResult != null) {
        	/**
        	 * 更新messageStore的hamaster地址
        	 */
            if (this.updateMasterHAServerAddrPeriodically && registerBrokerResult.getHaServerAddr() != null) 
            {
                this.messageStore.updateHaMasterAddress(registerBrokerResult.getHaServerAddr());
            }

            /**
             * 更新slave 同步数据的master broker源地址
             */
            this.slaveSynchronize.setMasterAddr(registerBrokerResult.getMasterAddr());

            /**
             * 是否根据返回结果更新topic的配置文件中的order标志
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
     * @描述: message ha listen port 
     * @作者: zgzhang@txbds.com
     * @日期: 2016年11月25日
     * @修改内容
     * @参数：@return     
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
