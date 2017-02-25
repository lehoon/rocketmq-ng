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

import com.alibaba.rocketmq.common.constant.LoggerName;
import com.alibaba.rocketmq.common.consumer.ConsumeFromWhere;
import com.alibaba.rocketmq.common.protocol.heartbeat.ConsumeType;
import com.alibaba.rocketmq.common.protocol.heartbeat.MessageModel;
import com.alibaba.rocketmq.common.protocol.heartbeat.SubscriptionData;
import com.alibaba.rocketmq.remoting.common.RemotingHelper;
import com.alibaba.rocketmq.remoting.common.RemotingUtil;
import io.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;


/**
 * 消费者管理
 * @author shijia.wxr
 */
public class ConsumerManager {
	
    private static final Logger log = LoggerFactory.getLogger(LoggerName.BrokerLoggerName);
    
    /**
     * 通道的有效事件120秒
     */
    private static final long ChannelExpiredTimeout = 1000 * 120;
    
    /**
     * 消费者组-消费者对应关系
     */
    private final ConcurrentHashMap<String/* Group */, ConsumerGroupInfo> consumerTable =
            new ConcurrentHashMap<String, ConsumerGroupInfo>(1024);
    
    
    /**
     * 消费ids变化通知监听器
     */
    private final ConsumerIdsChangeListener consumerIdsChangeListener;


    public ConsumerManager(final ConsumerIdsChangeListener consumerIdsChangeListener) {
        this.consumerIdsChangeListener = consumerIdsChangeListener;
    }

    /**
     * @描述: 查询指定组、id的消费者 
     * @作者: zgzhang@txbds.com
     * @日期:2016年11月24日
     * @修改内容
     * @参数：@param group
     * @参数：@param clientId
     * @参数：@return     
     * @throws
     */
    public ClientChannelInfo findChannel(final String group, final String clientId) {
        ConsumerGroupInfo consumerGroupInfo = this.consumerTable.get(group);
        if (consumerGroupInfo != null) {
            return consumerGroupInfo.findChannel(clientId);
        }

        return null;
    }

    /**
     * @描述: 查询订阅信息 
     * @作者: zgzhang@txbds.com
     * @日期:2016年11月24日
     * @修改内容
     * @参数：@param group
     * @参数：@param topic
     * @参数：@return     
     * @throws
     */
    public SubscriptionData findSubscriptionData(final String group, final String topic) {
        ConsumerGroupInfo consumerGroupInfo = this.getConsumerGroupInfo(group);
        if (consumerGroupInfo != null) {
            return consumerGroupInfo.findSubscriptionData(topic);
        }

        return null;
    }

    /**
     * @描述: 查询消费组信息 
     * @作者: zgzhang@txbds.com
     * @日期:2016年11月24日
     * @修改内容
     * @参数：@param group
     * @参数：@return     
     * @throws
     */
    public ConsumerGroupInfo getConsumerGroupInfo(final String group) {
        return this.consumerTable.get(group);
    }

    /**
     * @描述: 查询消费组信息大小 
     * @作者: zgzhang@txbds.com
     * @日期:2016年11月24日
     * @修改内容
     * @参数：@param group
     * @参数：@return     
     * @throws
     */
    public int findSubscriptionDataCount(final String group) {
        ConsumerGroupInfo consumerGroupInfo = this.getConsumerGroupInfo(group);
        if (consumerGroupInfo != null) {
            return consumerGroupInfo.getSubscriptionTable().size();
        }

        return 0;
    }

    /**
     * @描述: socket通道关闭事件通知 
     * @作者: zgzhang@txbds.com
     * @日期:2016年11月24日
     * @修改内容
     * @参数：@param remoteAddr
     * @参数：@param channel     
     * @throws
     */
    public void doChannelCloseEvent(final String remoteAddr, final Channel channel) {
        Iterator<Entry<String, ConsumerGroupInfo>> it = this.consumerTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, ConsumerGroupInfo> next = it.next();
            ConsumerGroupInfo info = next.getValue();
            boolean removed = info.doChannelCloseEvent(remoteAddr, channel);
            if (removed) {
                if (info.getChannelInfoTable().isEmpty()) {
                    ConsumerGroupInfo remove = this.consumerTable.remove(next.getKey());
                    if (remove != null) {
                        log.info("ungister consumer ok, no any connection, and remove consumer group, {}",
                                next.getKey());
                    }
                }

                this.consumerIdsChangeListener.consumerIdsChanged(next.getKey(), info.getAllChannel());
            }
        }
    }

    /**
     * @描述: 注册一个消费者 
     * @作者: zgzhang@txbds.com
     * @日期:2016年11月24日
     * @修改内容
     * @参数：@param group				组名称
     * @参数：@param clientChannelInfo	消费端信息
     * @参数：@param consumeType			消费类型     pull or push
     * @参数：@param messageModel			消息类型  集群 or 广播
     * @参数：@param consumeFromWhere		消费位置
     * @参数：@param subList				sub list
     * @参数：@return     
     * @throws
     */
    public boolean registerConsumer(final String group, final ClientChannelInfo clientChannelInfo,
                                    ConsumeType consumeType, MessageModel messageModel, ConsumeFromWhere consumeFromWhere,
                                    final Set<SubscriptionData> subList) {

    	/**
    	 * 查询该组的信息
    	 */
        ConsumerGroupInfo consumerGroupInfo = this.consumerTable.get(group);
        if (null == consumerGroupInfo) {
            ConsumerGroupInfo tmp = new ConsumerGroupInfo(group, consumeType, messageModel, consumeFromWhere);
            ConsumerGroupInfo prev = this.consumerTable.putIfAbsent(group, tmp);
            consumerGroupInfo = prev != null ? prev : tmp;
        }

        /**
         * 更新or添加消费者信息
         */
        boolean r1 =
                consumerGroupInfo.updateChannel(clientChannelInfo, consumeType, messageModel,
                        consumeFromWhere);
        
        /**
         * 更新订阅信息
         */
        boolean r2 = consumerGroupInfo.updateSubscription(subList);

        /**
         * 通知指定消费组有新的消费者加入
         */
        if (r1 || r2) {
            this.consumerIdsChangeListener.consumerIdsChanged(group, consumerGroupInfo.getAllChannel());
        }

        return r1 || r2;
    }

    /**
     * @描述: 删除指定的消费者 并通知其他消费者 
     * @作者: zgzhang@txbds.com
     * @日期:2016年11月24日
     * @修改内容
     * @参数：@param group
     * @参数：@param clientChannelInfo     
     * @throws
     */
    public void unregisterConsumer(final String group, final ClientChannelInfo clientChannelInfo) {
        ConsumerGroupInfo consumerGroupInfo = this.consumerTable.get(group);
        if (null != consumerGroupInfo) {
            consumerGroupInfo.unregisterChannel(clientChannelInfo);
            if (consumerGroupInfo.getChannelInfoTable().isEmpty()) {
                ConsumerGroupInfo remove = this.consumerTable.remove(group);
                if (remove != null) {
                    log.info("ungister consumer ok, no any connection, and remove consumer group, {}", group);
                }
            }
            this.consumerIdsChangeListener.consumerIdsChanged(group, consumerGroupInfo.getAllChannel());
        }
    }


    public void scanNotActiveChannel() {
        Iterator<Entry<String, ConsumerGroupInfo>> it = this.consumerTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, ConsumerGroupInfo> next = it.next();
            String group = next.getKey();
            ConsumerGroupInfo consumerGroupInfo = next.getValue();
            ConcurrentHashMap<Channel, ClientChannelInfo> channelInfoTable =
                    consumerGroupInfo.getChannelInfoTable();

            Iterator<Entry<Channel, ClientChannelInfo>> itChannel = channelInfoTable.entrySet().iterator();
            while (itChannel.hasNext()) {
                Entry<Channel, ClientChannelInfo> nextChannel = itChannel.next();
                ClientChannelInfo clientChannelInfo = nextChannel.getValue();
                long diff = System.currentTimeMillis() - clientChannelInfo.getLastUpdateTimestamp();
                if (diff > ChannelExpiredTimeout) {
                    log.warn(
                            "SCAN: remove expired channel from ConsumerManager consumerTable. channel={}, consumerGroup={}",
                            RemotingHelper.parseChannelRemoteAddr(clientChannelInfo.getChannel()), group);
                    RemotingUtil.closeChannel(clientChannelInfo.getChannel());
                    itChannel.remove();
                }
            }

            if (channelInfoTable.isEmpty()) {
                log.warn(
                        "SCAN: remove expired channel from ConsumerManager consumerTable, all clear, consumerGroup={}",
                        group);
                it.remove();
            }
        }
    }


    public HashSet<String> queryTopicConsumeByWho(final String topic) {
        HashSet<String> groups = new HashSet<String>();
        Iterator<Entry<String, ConsumerGroupInfo>> it = this.consumerTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, ConsumerGroupInfo> entry = it.next();
            ConcurrentHashMap<String, SubscriptionData> subscriptionTable =
                    entry.getValue().getSubscriptionTable();
            if (subscriptionTable.containsKey(topic)) {
                groups.add(entry.getKey());
            }
        }

        return groups;
    }
}
