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
package com.alibaba.rocketmq.client.impl.consumer;

import com.alibaba.rocketmq.client.consumer.PullCallback;
import com.alibaba.rocketmq.client.consumer.PullResult;
import com.alibaba.rocketmq.client.consumer.PullStatus;
import com.alibaba.rocketmq.client.exception.MQBrokerException;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.client.hook.FilterMessageContext;
import com.alibaba.rocketmq.client.hook.FilterMessageHook;
import com.alibaba.rocketmq.client.impl.CommunicationMode;
import com.alibaba.rocketmq.client.impl.FindBrokerResult;
import com.alibaba.rocketmq.client.impl.factory.MQClientInstance;
import com.alibaba.rocketmq.client.log.ClientLogger;
import com.alibaba.rocketmq.common.MixAll;
import com.alibaba.rocketmq.common.message.*;
import com.alibaba.rocketmq.common.protocol.header.PullMessageRequestHeader;
import com.alibaba.rocketmq.common.protocol.heartbeat.SubscriptionData;
import com.alibaba.rocketmq.common.protocol.route.TopicRouteData;
import com.alibaba.rocketmq.common.sysflag.PullSysFlag;
import com.alibaba.rocketmq.remoting.exception.RemotingException;
import org.slf4j.Logger;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;


/**
 * 拉取处理逻辑包装类
 * @author shijia.wxr
 */
public class PullAPIWrapper {
    private final Logger log = ClientLogger.getLog();
    
    /**
     * 客户端连接实例
     */
    private final MQClientInstance mQClientFactory;
    
    /**
     * 消费组名称
     */
    private final String consumerGroup;
    
    /**
     * 
     */
    private final boolean unitMode;
    
    /**
     * 队列和broker关系表
     */
    private ConcurrentHashMap<MessageQueue, AtomicLong/* brokerId */> pullFromWhichNodeTable = new ConcurrentHashMap<MessageQueue, AtomicLong>(32);
    
    /**
     * 用户主动连接broker的时候,需要设置默认的brokerid  暂时只在过滤器服务中使用到了
     * 参考:com.alibaba.rocketmq.filtersrv.registerFilterServerToBroker()
     */
    private volatile boolean connectBrokerByUser = false;
    
    /**
     * 默认为主broker
     */
    private volatile long defaultBrokerId = MixAll.MASTER_ID;
    
    /**
     * 随机数生成对象
     */
    private Random random = new Random(System.currentTimeMillis());
    
    /**
     * 消息逻辑拦截器列表
     */
    private ArrayList<FilterMessageHook> filterMessageHookList = new ArrayList<FilterMessageHook>();

    public PullAPIWrapper(MQClientInstance mQClientFactory, String consumerGroup, boolean unitMode) {
        this.mQClientFactory = mQClientFactory;
        this.consumerGroup = consumerGroup;
        this.unitMode = unitMode;
    }

    /**
     * @描述: 处理拉取消息结果 
     * @作者: zgzhang@txbds.com
     * @日期:2016年11月23日
     * @修改内容
     * @参数：@param mq					消息队列
     * @参数：@param pullResult			拉取结果
     * @参数：@param subscriptionData		订阅数据
     * @参数：@return     
     * @throws
     */
    public PullResult processPullResult(final MessageQueue mq, 
    									final PullResult pullResult,
                                        final SubscriptionData subscriptionData) 
    {
    	/**
    	 * 类型转换
    	 * 在拉取到消息的时候已经生成PullResultExt类型了，这里使用父类型进行传递
    	 * 这里只是把类型还原一下
    	 * 参考：MQClientApiImple.java  pullMessageSync方法
    	 */
        PullResultExt pullResultExt = (PullResultExt) pullResult;

        /**
         * 更新拉取信息broker的id
         */
        this.updatePullFromWhichNode(mq, pullResultExt.getSuggestWhichBrokerId());
        
        /**
         * 查询到数据
         */
        if (PullStatus.FOUND == pullResult.getPullStatus()) 
        {
        	/**
        	 * 根据响应的body字段生成缓冲区
        	 */
            ByteBuffer byteBuffer = ByteBuffer.wrap(pullResultExt.getMessageBinary());
            
            /**
             * body解码
             * 解析出MessageExt对象列表
             */
            List<MessageExt> msgList = MessageDecoder.decodes(byteBuffer);

            /**
             * 过滤前对象
             */
            List<MessageExt> msgListFilterAgain = msgList;
            
            /**
             * 订阅描述上有tag 同时 设置过滤模式的情况下才进行过滤
             */
            if (!subscriptionData.getTagsSet().isEmpty() && !subscriptionData.isClassFilterMode()) 
            {
                msgListFilterAgain = new ArrayList<MessageExt>(msgList.size());
                for (MessageExt msg : msgList) {
                    if (msg.getTags() != null) {
                        if (subscriptionData.getTagsSet().contains(msg.getTags())) {
                            msgListFilterAgain.add(msg);
                        }
                    }
                }
            }

            /**
             * 如果设定了拦截器
             */
            if (this.hasHook()) {
                FilterMessageContext filterMessageContext = new FilterMessageContext();
                filterMessageContext.setUnitMode(unitMode);
                filterMessageContext.setMsgList(msgListFilterAgain);
                this.executeHook(filterMessageContext);
            }

            /**
             * 设定消息的MIN_OFFSET和MAX_OFFSET
             * 同一批次拉取的消息中这两个属性都是一样的
             */
            for (MessageExt msg : msgListFilterAgain) {
                MessageAccessor.putProperty(msg, MessageConst.PROPERTY_MIN_OFFSET, Long.toString(pullResult.getMinOffset()));
                MessageAccessor.putProperty(msg, MessageConst.PROPERTY_MAX_OFFSET, Long.toString(pullResult.getMaxOffset()));
            }

            /**
             * 设定消息列表到拉取结果上
             */
            pullResultExt.setMsgFoundList(msgListFilterAgain);
        }

        /**
         * 清理body消息
         */
        pullResultExt.setMessageBinary(null);

        return pullResult;
    }

    /**
     * @描述: 更新拉取信息的broker信息 
     * @作者: zgzhang@txbds.com
     * @日期:2016年11月23日
     * @修改内容
     * @参数：@param mq
     * @参数：@param brokerId     
     * @throws
     */
    public void updatePullFromWhichNode(final MessageQueue mq, final long brokerId) {
        AtomicLong suggest = this.pullFromWhichNodeTable.get(mq);
        if (null == suggest) {
            this.pullFromWhichNodeTable.put(mq, new AtomicLong(brokerId));
        } else {
            suggest.set(brokerId);
        }
    }

    /**
     * @描述: 是否设定了拦截器 
     * @作者: zgzhang@txbds.com
     * @日期:2016年11月23日
     * @修改内容
     * @参数：@return     
     * @throws
     */
    public boolean hasHook() {
        return !this.filterMessageHookList.isEmpty();
    }

    /**
     * @描述: 执行消息拦截器 
     * @作者: zgzhang@txbds.com
     * @日期:2016年11月23日
     * @修改内容
     * @参数：@param context     
     * @throws
     */
    public void executeHook(final FilterMessageContext context) {
        if (!this.filterMessageHookList.isEmpty()) {
            for (FilterMessageHook hook : this.filterMessageHookList) {
                try {
                    hook.filterMessage(context);
                } catch (Throwable e) {
                    log.error("execute hook error. hookName={}", hook.hookName());
                }
            }
        }
    }

    /**
     * @描述: 拉取消息接口 
     * @作者: zgzhang@txbds.com
     * @日期:2016年11月23日
     * @修改内容
     * @参数：@param mq
     * @参数：@param subExpression
     * @参数：@param subVersion
     * @参数：@param offset
     * @参数：@param maxNums
     * @参数：@param sysFlag
     * @参数：@param commitOffset
     * @参数：@param brokerSuspendMaxTimeMillis
     * @参数：@param timeoutMillis
     * @参数：@param communicationMode
     * @参数：@param pullCallback
     * @参数：@return
     * @参数：@throws MQClientException
     * @参数：@throws RemotingException
     * @参数：@throws MQBrokerException
     * @参数：@throws InterruptedException     
     * @throws
     */
    public PullResult pullKernelImpl(//
                                     final MessageQueue mq,// 1
                                     final String subExpression,// 2
                                     final long subVersion,// 3
                                     final long offset,// 4
                                     final int maxNums,// 5
                                     final int sysFlag,// 6
                                     final long commitOffset,// 7
                                     final long brokerSuspendMaxTimeMillis,// 8
                                     final long timeoutMillis,// 9
                                     final CommunicationMode communicationMode,// 10
                                     final PullCallback pullCallback// 11
    ) throws MQClientException, RemotingException, MQBrokerException, InterruptedException 
    {
    	/**
    	 * 查询broker地址
    	 */
        FindBrokerResult findBrokerResult =
                this.mQClientFactory.findBrokerAddressInSubscribe(mq.getBrokerName(),
                        this.recalculatePullFromWhichNode(mq), false);
        
        /**
         * 如果没有找到broker
         */
        if (null == findBrokerResult) 
        {
        	/**
        	 * 从nameserver上更新路由信息,然后再次查找
        	 */
            this.mQClientFactory.updateTopicRouteInfoFromNameServer(mq.getTopic());
            findBrokerResult =
                    this.mQClientFactory.findBrokerAddressInSubscribe(mq.getBrokerName(),
                            this.recalculatePullFromWhichNode(mq), false);
        }

        /**
         * 如果找到broker
         */
        if (findBrokerResult != null) 
        {
            int sysFlagInner = sysFlag;

            /**
             * 如果broker为slave
             */
            if (findBrokerResult.isSlave()) 
            {
            	/**
            	 * 清除提交标志
            	 */
                sysFlagInner = PullSysFlag.clearCommitOffsetFlag(sysFlagInner);
            }

            /**
             * 请求头
             */
            PullMessageRequestHeader requestHeader = new PullMessageRequestHeader();
            requestHeader.setConsumerGroup(this.consumerGroup);
            requestHeader.setTopic(mq.getTopic());
            requestHeader.setQueueId(mq.getQueueId());
            requestHeader.setQueueOffset(offset);
            requestHeader.setMaxMsgNums(maxNums);
            requestHeader.setSysFlag(sysFlagInner);
            requestHeader.setCommitOffset(commitOffset);
            requestHeader.setSuspendTimeoutMillis(brokerSuspendMaxTimeMillis);
            requestHeader.setSubscription(subExpression);
            requestHeader.setSubVersion(subVersion);

            String brokerAddr = findBrokerResult.getBrokerAddr();
            /**
             * 如果配置了需要过滤
             * 暂时不支持过滤选项
             */
            if (PullSysFlag.hasClassFilterFlag(sysFlagInner)) {
                brokerAddr = computPullFromWhichFilterServer(mq.getTopic(), brokerAddr);
            }

            PullResult pullResult = this.mQClientFactory.getMQClientAPIImpl().pullMessage(//
                    brokerAddr,//
                    requestHeader,//
                    timeoutMillis,//
                    communicationMode,//
                    pullCallback);

            return pullResult;
        }

        /**
         * 抛出异常,提示broker不存在
         */
        throw new MQClientException("The broker[" + mq.getBrokerName() + "] not exist", null);
    }

    /**
     * @描述: 重新计算从哪个broker拉取消息 
     * @作者: zgzhang@txbds.com
     * @日期:2016年11月23日
     * @修改内容
     * @参数：@param mq
     * @参数：@return     
     * @throws
     */
    public long recalculatePullFromWhichNode(final MessageQueue mq) {
        if (this.isConnectBrokerByUser()) {
            return this.defaultBrokerId;
        }

        AtomicLong suggest = this.pullFromWhichNodeTable.get(mq);
        if (suggest != null) {
            return suggest.get();
        }

        return MixAll.MASTER_ID;
    }

    /**
     * @描述:  
     * @作者: zgzhang@txbds.com
     * @日期:2016年11月23日
     * @修改内容
     * @参数：@param topic
     * @参数：@param brokerAddr
     * @参数：@return
     * @参数：@throws MQClientException     
     * @throws
     */
    private String computPullFromWhichFilterServer(final String topic, final String brokerAddr)
            throws MQClientException 
    {
        ConcurrentHashMap<String, TopicRouteData> topicRouteTable = this.mQClientFactory.getTopicRouteTable();
        if (topicRouteTable != null) 
        {
            TopicRouteData topicRouteData = topicRouteTable.get(topic);
            List<String> list = topicRouteData.getFilterServerTable().get(brokerAddr);

            if (list != null && !list.isEmpty()) {
                return list.get(randomNum() % list.size());
            }
        }

        throw new MQClientException("Find Filter Server Failed, Broker Addr: " + brokerAddr + " topic: "
                + topic, null);
    }

    public boolean isConnectBrokerByUser() {
        return connectBrokerByUser;
    }

    public int randomNum() {
        int value = random.nextInt();
        if (value < 0) {
            value = Math.abs(value);
            if (value < 0)
                value = 0;
        }
        return value;
    }

    public void setConnectBrokerByUser(boolean connectBrokerByUser) {
        this.connectBrokerByUser = connectBrokerByUser;

    }

    public void registerFilterMessageHook(ArrayList<FilterMessageHook> filterMessageHookList) {
        this.filterMessageHookList = filterMessageHookList;
    }

    public long getDefaultBrokerId() {
        return defaultBrokerId;
    }

    public void setDefaultBrokerId(long defaultBrokerId) {
        this.defaultBrokerId = defaultBrokerId;
    }
}
