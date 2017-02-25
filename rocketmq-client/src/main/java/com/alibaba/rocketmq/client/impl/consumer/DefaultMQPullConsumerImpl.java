/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.rocketmq.client.impl.consumer;

import com.alibaba.rocketmq.client.QueryResult;
import com.alibaba.rocketmq.client.Validators;
import com.alibaba.rocketmq.client.consumer.DefaultMQPullConsumer;
import com.alibaba.rocketmq.client.consumer.PullCallback;
import com.alibaba.rocketmq.client.consumer.PullResult;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import com.alibaba.rocketmq.client.consumer.store.LocalFileOffsetStore;
import com.alibaba.rocketmq.client.consumer.store.OffsetStore;
import com.alibaba.rocketmq.client.consumer.store.ReadOffsetType;
import com.alibaba.rocketmq.client.consumer.store.RemoteBrokerOffsetStore;
import com.alibaba.rocketmq.client.exception.MQBrokerException;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.client.hook.ConsumeMessageContext;
import com.alibaba.rocketmq.client.hook.ConsumeMessageHook;
import com.alibaba.rocketmq.client.hook.FilterMessageHook;
import com.alibaba.rocketmq.client.impl.CommunicationMode;
import com.alibaba.rocketmq.client.impl.MQClientManager;
import com.alibaba.rocketmq.client.impl.factory.MQClientInstance;
import com.alibaba.rocketmq.client.log.ClientLogger;
import com.alibaba.rocketmq.common.MixAll;
import com.alibaba.rocketmq.common.ServiceState;
import com.alibaba.rocketmq.common.UtilAll;
import com.alibaba.rocketmq.common.consumer.ConsumeFromWhere;
import com.alibaba.rocketmq.common.filter.FilterAPI;
import com.alibaba.rocketmq.common.help.FAQUrl;
import com.alibaba.rocketmq.common.message.*;
import com.alibaba.rocketmq.common.protocol.body.ConsumerRunningInfo;
import com.alibaba.rocketmq.common.protocol.heartbeat.ConsumeType;
import com.alibaba.rocketmq.common.protocol.heartbeat.MessageModel;
import com.alibaba.rocketmq.common.protocol.heartbeat.SubscriptionData;
import com.alibaba.rocketmq.common.sysflag.PullSysFlag;
import com.alibaba.rocketmq.remoting.RPCHook;
import com.alibaba.rocketmq.remoting.common.RemotingHelper;
import com.alibaba.rocketmq.remoting.exception.RemotingException;
import org.slf4j.Logger;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;


/**
 * 拉取消息消费者实现
 * @author shijia.wxr
 */
public class DefaultMQPullConsumerImpl implements MQConsumerInner 
{
	/**
	 * log handler
	 */
    private final Logger log = ClientLogger.getLog();
    /**
     * 拉取对象
     */
    private final DefaultMQPullConsumer defaultMQPullConsumer;
    /**
     * 启动时间
     */
    private final long consumerStartTimestamp = System.currentTimeMillis();
    /**
     * rpc 拦截器
     */
    private final RPCHook rpcHook;
    /**
     * 消息过滤器列表
     */
    private final ArrayList<ConsumeMessageHook> consumeMessageHookList = new ArrayList<ConsumeMessageHook>();
    /**
     * 过滤消息过滤器列表
     */
    private final ArrayList<FilterMessageHook> filterMessageHookList = new ArrayList<FilterMessageHook>();
    /**
     * 服务状态  默认是未启动
     */
    private ServiceState serviceState = ServiceState.CREATE_JUST;
    /**
     * 网络实例对象
     */
    private MQClientInstance mQClientFactory;
    /**
     * 拉起函数封装对象
     */
    private PullAPIWrapper pullAPIWrapper;
    /**
     * 消息消费进度保存对象
     */
    private OffsetStore offsetStore;
    /**
     * 负载均衡器
     */
    private RebalanceImpl rebalanceImpl = new RebalancePullImpl(this);


    public DefaultMQPullConsumerImpl(final DefaultMQPullConsumer defaultMQPullConsumer, final RPCHook rpcHook) {
        this.defaultMQPullConsumer = defaultMQPullConsumer;
        this.rpcHook = rpcHook;
    }

    public void registerConsumeMessageHook(final ConsumeMessageHook hook) {
        this.consumeMessageHookList.add(hook);
        log.info("register consumeMessageHook Hook, {}", hook.hookName());
    }

    /**
     * @描述: 创建topic 
     * @作者: zgzhang@txbds.com
     * @日期:2016年11月24日
     * @修改内容
     * @参数：@param key
     * @参数：@param newTopic
     * @参数：@param queueNum
     * @参数：@throws MQClientException     
     * @throws
     */
    public void createTopic(String key, String newTopic, int queueNum) throws MQClientException {
        createTopic(key, newTopic, queueNum, 0);
    }

    /**
     * @描述: 创建topic 
     * @作者: zgzhang@txbds.com
     * @日期:2016年11月24日
     * @修改内容
     * @参数：@param key
     * @参数：@param newTopic
     * @参数：@param queueNum
     * @参数：@param topicSysFlag
     * @参数：@throws MQClientException     
     * @throws
     */
    public void createTopic(String key, String newTopic, int queueNum, int topicSysFlag) throws MQClientException {
        this.makeSureStateOK();
        this.mQClientFactory.getMQAdminImpl().createTopic(key, newTopic, queueNum, topicSysFlag);
    }

    /**
     * @描述: 检查服务运行状态 
     * @作者: zgzhang@txbds.com
     * @日期:2016年11月23日
     * @修改内容
     * @参数：@throws MQClientException     
     * @throws
     */
    private void makeSureStateOK() throws MQClientException {
        if (this.serviceState != ServiceState.RUNNING) {
            throw new MQClientException("The consumer service state not OK, "//
                    + this.serviceState//
                    + FAQUrl.suggestTodo(FAQUrl.CLIENT_SERVICE_NOT_OK),
                    null);
        }
    }

    /**
     * @描述: 查询消费位置 
     * @作者: zgzhang@txbds.com
     * @日期:2016年11月23日
     * @修改内容
     * @参数：@param mq
     * @参数：@param fromStore
     * @参数：@return
     * @参数：@throws MQClientException     
     * @throws
     */
    public long fetchConsumeOffset(MessageQueue mq, boolean fromStore) throws MQClientException {
        this.makeSureStateOK();
        return this.offsetStore.readOffset(mq, fromStore ? ReadOffsetType.READ_FROM_STORE : ReadOffsetType.MEMORY_FIRST_THEN_STORE);
    }

    /**
     * @描述: 获取订阅关联的消息队列 
     * @作者: zgzhang@txbds.com
     * @日期:2016年11月23日
     * @修改内容
     * @参数：@param topic
     * @参数：@return
     * @参数：@throws MQClientException     
     * @throws
     */
    public Set<MessageQueue> fetchMessageQueuesInBalance(String topic) throws MQClientException {
        this.makeSureStateOK();
        if (null == topic) {
            throw new IllegalArgumentException("topic is null");
        }

        ConcurrentHashMap<MessageQueue, ProcessQueue> mqTable = this.rebalanceImpl.getProcessQueueTable();
        Set<MessageQueue> mqResult = new HashSet<MessageQueue>();
        for (MessageQueue mq : mqTable.keySet()) {
            if (mq.getTopic().equals(topic)) {
                mqResult.add(mq);
            }
        }

        return mqResult;
    }

    /**
     * @描述: 根据topic从namesrv上获取消息队列 
     * @作者: zgzhang@txbds.com
     * @日期:2016年11月24日
     * @修改内容
     * @参数：@param topic
     * @参数：@return
     * @参数：@throws MQClientException     
     * @throws
     */
    public List<MessageQueue> fetchPublishMessageQueues(String topic) throws MQClientException {
        this.makeSureStateOK();
        return this.mQClientFactory.getMQAdminImpl().fetchPublishMessageQueues(topic);
    }

    /**
     * @描述: 获取订阅的消息队列 
     * @作者: zgzhang@txbds.com
     * @日期:2016年11月24日
     * @修改内容
     * @参数：@param topic
     * @参数：@return
     * @参数：@throws MQClientException     
     * @throws
     */
    public Set<MessageQueue> fetchSubscribeMessageQueues(String topic) throws MQClientException {
        this.makeSureStateOK();
        return this.mQClientFactory.getMQAdminImpl().fetchSubscribeMessageQueues(topic);
    }

    /**
     * @描述: 获取指定消息队列存储开始时间 
     * @作者: zgzhang@txbds.com
     * @日期:2016年11月24日
     * @修改内容
     * @参数：@param mq
     * @参数：@return
     * @参数：@throws MQClientException     
     * @throws
     */
    public long earliestMsgStoreTime(MessageQueue mq) throws MQClientException {
        this.makeSureStateOK();
        return this.mQClientFactory.getMQAdminImpl().earliestMsgStoreTime(mq);
    }

    /**
     * @描述: 最大偏移量 
     * @作者: zgzhang@txbds.com
     * @日期:2016年11月24日
     * @修改内容
     * @参数：@param mq
     * @参数：@return
     * @参数：@throws MQClientException     
     * @throws
     */
    public long maxOffset(MessageQueue mq) throws MQClientException {
        this.makeSureStateOK();
        return this.mQClientFactory.getMQAdminImpl().maxOffset(mq);
    }

    /**
     * @描述: 消息队列的最小偏移量 
     * @作者: zgzhang@txbds.com
     * @日期:2016年11月24日
     * @修改内容
     * @参数：@param mq
     * @参数：@return
     * @参数：@throws MQClientException     
     * @throws
     */
    public long minOffset(MessageQueue mq) throws MQClientException {
        this.makeSureStateOK();
        return this.mQClientFactory.getMQAdminImpl().minOffset(mq);
    }

    /**
     * @描述:  从指定的消息队列拉取消息
     * @作者: zgzhang@txbds.com
     * @日期:2016年11月24日
     * @修改内容
     * @参数：@param mq				消息队列
     * @参数：@param subExpression	过滤描述
     * @参数：@param offset			开始偏移量     从该位置查询消息
     * @参数：@param maxNums			最大消息数量
     * @参数：@return					默认从offset开始+maxnums结束的消息
     * @参数：@throws MQClientException
     * @参数：@throws RemotingException
     * @参数：@throws MQBrokerException
     * @参数：@throws InterruptedException     
     * @throws
     */
    public PullResult pull(MessageQueue mq, String subExpression, long offset, int maxNums)
            throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        return pull(mq, subExpression, offset, maxNums, this.defaultMQPullConsumer.getConsumerPullTimeoutMillis());
    }

    /**
     * @描述: 从指定的消息队列拉取消息,指定了最大超时时间
     * @作者: zgzhang@txbds.com
     * @日期:2016年11月24日
     * @修改内容
     * @参数：@param mq
     * @参数：@param subExpression
     * @参数：@param offset
     * @参数：@param maxNums
     * @参数：@param timeout
     * @参数：@return
     * @参数：@throws MQClientException
     * @参数：@throws RemotingException
     * @参数：@throws MQBrokerException
     * @参数：@throws InterruptedException     
     * @throws
     */
    public PullResult pull(MessageQueue mq, String subExpression, long offset, int maxNums, long timeout)
            throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        return this.pullSyncImpl(mq, subExpression, offset, maxNums, false, timeout);
    }

    /**
     * @描述: 同步拉取消息 
     * @作者: zgzhang@txbds.com
     * @日期:2016年11月23日
     * @修改内容
     * @参数：@param mq     队列
     * @参数：@param subExpression  描述信息
     * @参数：@param offset    队列中的偏移量
     * @参数：@param maxNums   最大消息数目
     * @参数：@param block     
     * @参数：@param timeout  超时时间
     * @参数：@throws MQClientException
     * @参数：@throws RemotingException
     * @参数：@throws MQBrokerException
     * @参数：@throws InterruptedException     
     * @return PullResult 拉取结果  
     * @throws
     */
    private PullResult pullSyncImpl(MessageQueue mq, String subExpression, long offset, int maxNums, boolean block, long timeout)
            throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        
    	/**
    	 * 检查运行状态
    	 */
    	this.makeSureStateOK();

        if (null == mq) {
            throw new MQClientException("mq is null", null);

        }

        /**
         * 检查偏移量是否合法
         */
        if (offset < 0) {
            throw new MQClientException("offset < 0", null);
        }

        /**
         * 拉取最大消息量
         */
        if (maxNums <= 0) {
            throw new MQClientException("maxNums <= 0", null);
        }

        /**
         * 维护自动订阅信息
         */
        this.subscriptionAutomatically(mq.getTopic());

        /**
         * 构建消息标志位
         */
        int sysFlag = PullSysFlag.buildSysFlag(false, block, true, false);

        SubscriptionData subscriptionData;
        try {
            subscriptionData = FilterAPI.buildSubscriptionData(this.defaultMQPullConsumer.getConsumerGroup(), //
                    mq.getTopic(), subExpression);
        } catch (Exception e) {
            throw new MQClientException("parse subscription error", e);
        }

        long timeoutMillis = block ? this.defaultMQPullConsumer.getConsumerTimeoutMillisWhenSuspend() : timeout;

        /**
         * 同步拉取消息
         */
        PullResult pullResult = this.pullAPIWrapper.pullKernelImpl(//
                mq, // 1   消息队列
                subscriptionData.getSubString(), // 2  过滤描述字段
                0L, // 3  
                offset, // 4
                maxNums, // 5
                sysFlag, // 6
                0, // 7
                this.defaultMQPullConsumer.getBrokerSuspendMaxTimeMillis(), // 8
                timeoutMillis, // 9
                CommunicationMode.SYNC, // 10
                null// 11
        );

        this.pullAPIWrapper.processPullResult(mq, pullResult, subscriptionData);

        if (!this.consumeMessageHookList.isEmpty()) {
            ConsumeMessageContext consumeMessageContext = null;
            consumeMessageContext = new ConsumeMessageContext();
            consumeMessageContext.setConsumerGroup(this.groupName());
            consumeMessageContext.setMq(mq);
            consumeMessageContext.setMsgList(pullResult.getMsgFoundList());
            consumeMessageContext.setSuccess(false);
            this.executeHookBefore(consumeMessageContext);
            consumeMessageContext.setStatus(ConsumeConcurrentlyStatus.CONSUME_SUCCESS.toString());
            consumeMessageContext.setSuccess(true);
            this.executeHookAfter(consumeMessageContext);
        }

        return pullResult;
    }

    /**
     * @描述:  自动订阅维护
     * @作者: zgzhang@txbds.com
     * @日期:2016年11月23日
     * @修改内容
     * @参数：@param topic 主题名称     
     * @return void   
     * @throws
     */
    private void subscriptionAutomatically(final String topic) 
    {
    	//如果不存在则添加
        if (!this.rebalanceImpl.getSubscriptionInner().containsKey(topic)) {
            try {
                SubscriptionData subscriptionData = FilterAPI.buildSubscriptionData(this.defaultMQPullConsumer.getConsumerGroup(), //
                        topic, 
                        SubscriptionData.SUB_ALL);
                this.rebalanceImpl.subscriptionInner.putIfAbsent(topic, subscriptionData);
            } catch (Exception e) {
            }
        }
    }

    @Override
    public String groupName() {
        return this.defaultMQPullConsumer.getConsumerGroup();
    }

    public void executeHookBefore(final ConsumeMessageContext context) {
        if (!this.consumeMessageHookList.isEmpty()) {
            for (ConsumeMessageHook hook : this.consumeMessageHookList) {
                try {
                    hook.consumeMessageBefore(context);
                } catch (Throwable e) {
                }
            }
        }
    }

    public void executeHookAfter(final ConsumeMessageContext context) {
        if (!this.consumeMessageHookList.isEmpty()) {
            for (ConsumeMessageHook hook : this.consumeMessageHookList) {
                try {
                    hook.consumeMessageAfter(context);
                } catch (Throwable e) {
                }
            }
        }
    }

    @Override
    public MessageModel messageModel() {
        return this.defaultMQPullConsumer.getMessageModel();
    }

    @Override
    public ConsumeType consumeType() {
        return ConsumeType.CONSUME_ACTIVELY;
    }

    @Override
    public ConsumeFromWhere consumeFromWhere() {
        return ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET;
    }

    @Override
    public Set<SubscriptionData> subscriptions() {
        Set<SubscriptionData> result = new HashSet<SubscriptionData>();

        Set<String> topics = this.defaultMQPullConsumer.getRegisterTopics();
        if (topics != null) {
            synchronized (topics) {
                for (String t : topics) {
                    SubscriptionData ms = null;
                    try {
                        ms = FilterAPI.buildSubscriptionData(this.groupName(), t, SubscriptionData.SUB_ALL);
                    } catch (Exception e) {
                        log.error("parse subscription error", e);
                    }
                    ms.setSubVersion(0L);
                    result.add(ms);
                }
            }
        }

        return result;
    }

    @Override
    public void doRebalance() {
        if (this.rebalanceImpl != null) {
            this.rebalanceImpl.doRebalance(false);
        }
    }

    /**
     * 持久化消费者进度
     */
    @Override
    public void persistConsumerOffset() {
        try {
            this.makeSureStateOK();
            Set<MessageQueue> mqs = new HashSet<MessageQueue>();
            Set<MessageQueue> allocateMq = this.rebalanceImpl.getProcessQueueTable().keySet();
            if (allocateMq != null) {
                mqs.addAll(allocateMq);
            }
            this.offsetStore.persistAll(mqs);
        } catch (Exception e) {
            log.error("group: " + this.defaultMQPullConsumer.getConsumerGroup() + " persistConsumerOffset exception", e);
        }
    }

    @Override
    public void updateTopicSubscribeInfo(String topic, Set<MessageQueue> info) {
        Map<String, SubscriptionData> subTable = this.rebalanceImpl.getSubscriptionInner();
        if (subTable != null) {
            if (subTable.containsKey(topic)) {
                this.rebalanceImpl.getTopicSubscribeInfoTable().put(topic, info);
            }
        }
    }

    @Override
    public boolean isSubscribeTopicNeedUpdate(String topic) {
        Map<String, SubscriptionData> subTable = this.rebalanceImpl.getSubscriptionInner();
        if (subTable != null) {
            if (subTable.containsKey(topic)) {
                return !this.rebalanceImpl.topicSubscribeInfoTable.containsKey(topic);
            }
        }

        return false;
    }

    @Override
    public boolean isUnitMode() {
        return this.defaultMQPullConsumer.isUnitMode();
    }

    @Override
    public ConsumerRunningInfo consumerRunningInfo() {
        ConsumerRunningInfo info = new ConsumerRunningInfo();

        Properties prop = MixAll.object2Properties(this.defaultMQPullConsumer);
        prop.put(ConsumerRunningInfo.PROP_CONSUMER_START_TIMESTAMP, String.valueOf(this.consumerStartTimestamp));
        info.setProperties(prop);

        info.getSubscriptionSet().addAll(this.subscriptions());
        return info;
    }

    public void pull(MessageQueue mq, String subExpression, long offset, int maxNums, PullCallback pullCallback)
            throws MQClientException, RemotingException, InterruptedException {
        pull(mq, subExpression, offset, maxNums, pullCallback, this.defaultMQPullConsumer.getConsumerPullTimeoutMillis());
    }

    public void pull(MessageQueue mq, String subExpression, long offset, int maxNums, PullCallback pullCallback, long timeout)
            throws MQClientException, RemotingException, InterruptedException {
        this.pullAsyncImpl(mq, subExpression, offset, maxNums, pullCallback, false, timeout);
    }

    /**
     * @描述: 异步方式拉取消息 
     * @作者: zgzhang@txbds.com
     * @日期:2016年11月23日
     * @修改内容
     * @参数：@param mq
     * @参数：@param subExpression
     * @参数：@param offset
     * @参数：@param maxNums
     * @参数：@param pullCallback
     * @参数：@param block
     * @参数：@param timeout
     * @参数：@throws MQClientException
     * @参数：@throws RemotingException
     * @参数：@throws InterruptedException     
     * @throws
     */
    private void pullAsyncImpl(//
                               final MessageQueue mq, //
                               final String subExpression, //
                               final long offset, //
                               final int maxNums, //
                               final PullCallback pullCallback, //
                               final boolean block, //
                               final long timeout) throws MQClientException, RemotingException, InterruptedException {
        this.makeSureStateOK();

        if (null == mq) {
            throw new MQClientException("mq is null", null);
        }

        if (offset < 0) {
            throw new MQClientException("offset < 0", null);
        }

        if (maxNums <= 0) {
            throw new MQClientException("maxNums <= 0", null);
        }

        if (null == pullCallback) {
            throw new MQClientException("pullCallback is null", null);
        }

        this.subscriptionAutomatically(mq.getTopic());

        try {
            int sysFlag = PullSysFlag.buildSysFlag(false, block, true, false);

            final SubscriptionData subscriptionData;
            try {
                subscriptionData = FilterAPI.buildSubscriptionData(this.defaultMQPullConsumer.getConsumerGroup(), //
                        mq.getTopic(), subExpression);
            } catch (Exception e) {
                throw new MQClientException("parse subscription error", e);
            }

            long timeoutMillis = block ? this.defaultMQPullConsumer.getConsumerTimeoutMillisWhenSuspend() : timeout;

            this.pullAPIWrapper.pullKernelImpl(//
                    mq, // 1
                    subscriptionData.getSubString(), // 2
                    0L, // 3
                    offset, // 4
                    maxNums, // 5
                    sysFlag, // 6
                    0, // 7
                    this.defaultMQPullConsumer.getBrokerSuspendMaxTimeMillis(), // 8
                    timeoutMillis, // 9
                    CommunicationMode.ASYNC, // 10
                    new PullCallback() {

                        @Override
                        public void onSuccess(PullResult pullResult) {
                            pullCallback
                                    .onSuccess(DefaultMQPullConsumerImpl.this.pullAPIWrapper.processPullResult(mq, pullResult, subscriptionData));
                        }

                        @Override
                        public void onException(Throwable e) {
                            pullCallback.onException(e);
                        }
                    });
        } catch (MQBrokerException e) {
            throw new MQClientException("pullAsync unknow exception", e);
        }
    }

    /**
     * @描述:  同步方式 拉取指定消息队列上的消息列表
     * @作者: zgzhang@txbds.com
     * @日期:2016年11月23日
     * @修改内容
     * @参数：@param mq               消息队列
     * @参数：@param subExpression    过滤描述字段
     * @参数：@param offset           偏移量
     * @参数：@param maxNums          一次拉取最大消息数
     * @参数：@return                 PullResult
     * @参数：@throws MQClientException
     * @参数：@throws RemotingException
     * @参数：@throws MQBrokerException
     * @参数：@throws InterruptedException     
     * @throws
     */
    public PullResult pullBlockIfNotFound(MessageQueue mq, String subExpression, long offset, int maxNums)
            throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        return this.pullSyncImpl(mq, subExpression, offset, maxNums, true, this.getDefaultMQPullConsumer().getConsumerPullTimeoutMillis());
    }

    public DefaultMQPullConsumer getDefaultMQPullConsumer() {
        return defaultMQPullConsumer;
    }

    /**
     * @描述: 异步方式拉取消息
     * @作者: zgzhang@txbds.com
     * @日期:2016年11月23日
     * @修改内容
     * @参数：@param mq               消息队列
     * @参数：@param subExpression    过滤描述字段
     * @参数：@param offset           偏移量
     * @参数：@param maxNums          一次拉取最大数
     * @参数：@param pullCallback     回调函数
     * @参数：@throws MQClientException
     * @参数：@throws RemotingException
     * @参数：@throws InterruptedException     
     * @throws
     */
    public void pullBlockIfNotFound(MessageQueue mq, String subExpression, long offset, int maxNums, PullCallback pullCallback)
            throws MQClientException, RemotingException, InterruptedException {
        this.pullAsyncImpl(mq, subExpression, offset, maxNums, pullCallback, true,
                this.getDefaultMQPullConsumer().getConsumerPullTimeoutMillis());
    }

    public QueryResult queryMessage(String topic, String key, int maxNum, long begin, long end)
            throws MQClientException, InterruptedException {
        this.makeSureStateOK();
        return this.mQClientFactory.getMQAdminImpl().queryMessage(topic, key, maxNum, begin, end);
    }

    public MessageExt queryMessageByUniqKey(String topic, String uniqKey)
            throws MQClientException, InterruptedException {
        this.makeSureStateOK();
        return this.mQClientFactory.getMQAdminImpl().queryMessageByUniqKey(topic, uniqKey);
    }

    public long searchOffset(MessageQueue mq, long timestamp) throws MQClientException {
        this.makeSureStateOK();
        return this.mQClientFactory.getMQAdminImpl().searchOffset(mq, timestamp);
    }

    public void sendMessageBack(MessageExt msg, int delayLevel, final String brokerName)
            throws RemotingException, MQBrokerException, InterruptedException, MQClientException {
        sendMessageBack(msg, delayLevel, brokerName, this.defaultMQPullConsumer.getConsumerGroup());
    }

    public void sendMessageBack(MessageExt msg, int delayLevel, final String brokerName, String consumerGroup)
            throws RemotingException, MQBrokerException, InterruptedException, MQClientException {
        try {
            String brokerAddr = (null != brokerName) ? this.mQClientFactory.findBrokerAddressInPublish(brokerName)
                    : RemotingHelper.parseSocketAddressAddr(msg.getStoreHost());

            if (UtilAll.isBlank(consumerGroup)) {
                consumerGroup = this.defaultMQPullConsumer.getConsumerGroup();
            }

            this.mQClientFactory.getMQClientAPIImpl().consumerSendMessageBack(brokerAddr, msg, consumerGroup, delayLevel, 3000,
                    this.defaultMQPullConsumer.getMaxReconsumeTimes());
        } catch (Exception e) {
            log.error("sendMessageBack Exception, " + this.defaultMQPullConsumer.getConsumerGroup(), e);

            Message newMsg = new Message(MixAll.getRetryTopic(this.defaultMQPullConsumer.getConsumerGroup()), msg.getBody());
            String originMsgId = MessageAccessor.getOriginMessageId(msg);
            MessageAccessor.setOriginMessageId(newMsg, UtilAll.isBlank(originMsgId) ? msg.getMsgId() : originMsgId);
            newMsg.setFlag(msg.getFlag());
            MessageAccessor.setProperties(newMsg, msg.getProperties());
            MessageAccessor.putProperty(newMsg, MessageConst.PROPERTY_RETRY_TOPIC, msg.getTopic());
            MessageAccessor.setReconsumeTime(newMsg, String.valueOf(msg.getReconsumeTimes() + 1));
            MessageAccessor.setMaxReconsumeTimes(newMsg, String.valueOf(this.defaultMQPullConsumer.getMaxReconsumeTimes()));
            newMsg.setDelayTimeLevel(3 + msg.getReconsumeTimes());
            this.mQClientFactory.getDefaultMQProducer().send(newMsg);
        }
    }

    public void shutdown() {
        switch (this.serviceState) {
            case CREATE_JUST:
                break;
            case RUNNING:
                this.persistConsumerOffset();
                this.mQClientFactory.unregisterConsumer(this.defaultMQPullConsumer.getConsumerGroup());
                this.mQClientFactory.shutdown();
                log.info("the consumer [{}] shutdown OK", this.defaultMQPullConsumer.getConsumerGroup());
                this.serviceState = ServiceState.SHUTDOWN_ALREADY;
                break;
            case SHUTDOWN_ALREADY:
                break;
            default:
                break;
        }
    }

    /**
     * @描述: 启动拉取消费者服务 
     * @作者: zgzhang@txbds.com
     * @日期:2016年11月23日
     * @修改内容
     * @参数：@throws MQClientException     
     * @throws
     */
    public void start() throws MQClientException {
        switch (this.serviceState) {
            case CREATE_JUST:
                this.serviceState = ServiceState.START_FAILED;

                /**
                 * 检查配置文件
                 */
                this.checkConfig();

                /**
                 * 把订阅主题数据拷贝到负载均衡器里面
                 */
                this.copySubscription();

                /**
                 * 如果是集群消费模式,需要更改实例的名称
                 * 如果在集群模式下,消费者实例名称为default的话，则需要修改为进程id
                 */
                if (this.defaultMQPullConsumer.getMessageModel() == MessageModel.CLUSTERING) {
                    this.defaultMQPullConsumer.changeInstanceNameToPID();
                }

                /**
                 * 创建客户工厂对象
                 * 主要负责连接类
                 */
                this.mQClientFactory = MQClientManager.getInstance().getAndCreateMQClientInstance(this.defaultMQPullConsumer, this.rpcHook);

                /**
                 * 设置消费组名称
                 */
                this.rebalanceImpl.setConsumerGroup(this.defaultMQPullConsumer.getConsumerGroup());
                
                /**
                 * 在负载均衡上设置消费模式
                 */
                this.rebalanceImpl.setMessageModel(this.defaultMQPullConsumer.getMessageModel());
                
                /**
                 * 设置负载均衡的队列分配算法
                 * 默认初始化的是平均分配
                 */
                this.rebalanceImpl.setAllocateMessageQueueStrategy(this.defaultMQPullConsumer.getAllocateMessageQueueStrategy());
                
                /**
                 * 设置客户端交互管理工厂对象
                 */
                this.rebalanceImpl.setmQClientFactory(this.mQClientFactory);

                /**
                 * 构造拉取处理包装类对象
                 */
                this.pullAPIWrapper = new PullAPIWrapper(//
                        mQClientFactory, // 客户端连接管理工厂
                        this.defaultMQPullConsumer.getConsumerGroup(),  //消费组名称 
                        isUnitMode());
                
                /**
                 * 注册消息过滤拦截器列表
                 */
                this.pullAPIWrapper.registerFilterMessageHook(filterMessageHookList);

                /**
                 * 如果本地偏移量存储不为空
                 */
                if (this.defaultMQPullConsumer.getOffsetStore() != null) 
                {
                	/**
                	 * 获取偏移量存储对象
                	 */
                    this.offsetStore = this.defaultMQPullConsumer.getOffsetStore();
                } 
                else 
                {
                	/**
                	 * 根据消费模式获取偏移量存储对象
                	 */
                    switch (this.defaultMQPullConsumer.getMessageModel()) {
                        case BROADCASTING:
                        	/**
                        	 * 广播模式需要从本地文件获取
                        	 */
                            this.offsetStore = new LocalFileOffsetStore(this.mQClientFactory, this.defaultMQPullConsumer.getConsumerGroup());
                            break;
                        case CLUSTERING:
                        	/**
                        	 * 集群模式从远程broker上获取
                        	 */
                            this.offsetStore = new RemoteBrokerOffsetStore(this.mQClientFactory, this.defaultMQPullConsumer.getConsumerGroup());
                            break;
                        default:
                            break;
                    }
                }

                /**
                 * 如果是广播模式从本地文件读取，如果文件不存在则读取不奥
                 * 如果是集群模式，则需要从远程broker上读取，当在集群下load实现为空
                 */
                this.offsetStore.load();

                /**
                 * 远程管理类上注册消费者对象
                 */
                boolean registerOK = mQClientFactory.registerConsumer(this.defaultMQPullConsumer.getConsumerGroup(), //消费组名称 
                												this); //消费者
                
                /**
                 * 如果注册的消费者已经存在，则返回false
                 * 注册失败,停止启动流程
                 * 抛出异常，上层捕获
                 */
                if (!registerOK) {
                    this.serviceState = ServiceState.CREATE_JUST;

                    throw new MQClientException("The consumer group[" + this.defaultMQPullConsumer.getConsumerGroup()
                            + "] has been created before, specify another name please." + FAQUrl.suggestTodo(FAQUrl.GROUP_NAME_DUPLICATE_URL),
                            null);
                }

                /**
                 * 启动消息队列客户端工厂
                 */
                mQClientFactory.start();
                log.info("the consumer [{}] start OK", this.defaultMQPullConsumer.getConsumerGroup());
                this.serviceState = ServiceState.RUNNING;
                break;
            case RUNNING:
            case START_FAILED:
            case SHUTDOWN_ALREADY:
                throw new MQClientException("The PullConsumer service state not OK, maybe started once, "//
                        + this.serviceState//
                        + FAQUrl.suggestTodo(FAQUrl.CLIENT_SERVICE_NOT_OK),
                        null);
            default:
                break;
        }
    }

    private void checkConfig() throws MQClientException {
        // check consumerGroup
        Validators.checkGroup(this.defaultMQPullConsumer.getConsumerGroup());

        // consumerGroup
        if (null == this.defaultMQPullConsumer.getConsumerGroup()) {
            throw new MQClientException(
                    "consumerGroup is null" //
                            + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL), //
                    null);
        }

        // consumerGroup
        if (this.defaultMQPullConsumer.getConsumerGroup().equals(MixAll.DEFAULT_CONSUMER_GROUP)) {
            throw new MQClientException(
                    "consumerGroup can not equal "//
                            + MixAll.DEFAULT_CONSUMER_GROUP //
                            + ", please specify another one."//
                            + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL), //
                    null);
        }

        // messageModel
        if (null == this.defaultMQPullConsumer.getMessageModel()) {
            throw new MQClientException(
                    "messageModel is null" //
                            + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL), //
                    null);
        }

        // allocateMessageQueueStrategy
        if (null == this.defaultMQPullConsumer.getAllocateMessageQueueStrategy()) {
            throw new MQClientException(
                    "allocateMessageQueueStrategy is null" //
                            + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL), //
                    null);
        }

        // allocateMessageQueueStrategy
        if (this.defaultMQPullConsumer.getConsumerTimeoutMillisWhenSuspend() < this.defaultMQPullConsumer.getBrokerSuspendMaxTimeMillis()) {
            throw new MQClientException(
                    "Long polling mode, the consumer consumerTimeoutMillisWhenSuspend must greater than brokerSuspendMaxTimeMillis" //
                            + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL), //
                    null);
        }
    }

    /**
     * @描述: 把订阅主题数据放到负载均衡器里面 
     * @作者: zgzhang@txbds.com
     * @日期:2016年11月23日
     * @修改内容
     * @参数：@throws MQClientException     
     * @throws
     */
    private void copySubscription() throws MQClientException {
        try {
        	/**
        	 * 获取注册的订阅主题集合
        	 */
            Set<String> registerTopics = this.defaultMQPullConsumer.getRegisterTopics();

            /**
             * 如果存在，需要放到负载均衡器里面
             */
            if (registerTopics != null) {
                for (final String topic : registerTopics) {
                    SubscriptionData subscriptionData = FilterAPI.buildSubscriptionData(this.defaultMQPullConsumer.getConsumerGroup(), //
                            topic, SubscriptionData.SUB_ALL);
                    this.rebalanceImpl.getSubscriptionInner().put(topic, subscriptionData);
                }
            }
        } catch (Exception e) {
            throw new MQClientException("subscription exception", e);
        }
    }

    public void updateConsumeOffset(MessageQueue mq, long offset) throws MQClientException {
        this.makeSureStateOK();
        this.offsetStore.updateOffset(mq, offset, false);
    }

    public MessageExt viewMessage(String msgId) throws RemotingException, MQBrokerException, InterruptedException, MQClientException {
        this.makeSureStateOK();
        return this.mQClientFactory.getMQAdminImpl().viewMessage(msgId);
    }

    public void registerFilterMessageHook(final FilterMessageHook hook) {
        this.filterMessageHookList.add(hook);
        log.info("register FilterMessageHook Hook, {}", hook.hookName());
    }

    public OffsetStore getOffsetStore() {
        return offsetStore;
    }

    public void setOffsetStore(OffsetStore offsetStore) {
        this.offsetStore = offsetStore;
    }

    public PullAPIWrapper getPullAPIWrapper() {
        return pullAPIWrapper;
    }

    public void setPullAPIWrapper(PullAPIWrapper pullAPIWrapper) {
        this.pullAPIWrapper = pullAPIWrapper;
    }

    public ServiceState getServiceState() {
        return serviceState;
    }

    public void setServiceState(ServiceState serviceState) {
        this.serviceState = serviceState;
    }

    public long getConsumerStartTimestamp() {
        return consumerStartTimestamp;
    }


    public RebalanceImpl getRebalanceImpl() {
        return rebalanceImpl;
    }
}
