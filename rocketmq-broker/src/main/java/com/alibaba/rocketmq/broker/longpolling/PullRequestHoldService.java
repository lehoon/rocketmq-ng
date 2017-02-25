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
package com.alibaba.rocketmq.broker.longpolling;

import com.alibaba.rocketmq.broker.BrokerController;
import com.alibaba.rocketmq.common.ServiceThread;
import com.alibaba.rocketmq.common.constant.LoggerName;
import com.alibaba.rocketmq.remoting.exception.RemotingCommandException;
import com.alibaba.rocketmq.store.DefaultMessageFilter;
import com.alibaba.rocketmq.store.MessageFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;


/**
 * @author shijia.wxr
 */
public class PullRequestHoldService extends ServiceThread {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.BrokerLoggerName);
    
    /**
     * 分割符
     */
    private static final String TOPIC_QUEUEID_SEPARATOR = "@";
    
    /**
     * broker控制器对象
     */
    private final BrokerController brokerController;

    /**
     * 消息过滤对象
     */
    private final MessageFilter messageFilter = new DefaultMessageFilter();
    
    /**
     * 请求表
     */
    private ConcurrentHashMap<String/* topic@queueId */, ManyPullRequest> pullRequestTable =
            new ConcurrentHashMap<String, ManyPullRequest>(1024);


    public PullRequestHoldService(final BrokerController brokerController) {
        this.brokerController = brokerController;
    }

    /**
     * @描述: 暂停拉取请求 
     * @作者: zgzhang@txbds.com
     * @日期:2016年11月24日
     * @修改内容
     * @参数：@param topic
     * @参数：@param queueId
     * @参数：@param pullRequest     
     * @throws
     */
    public void suspendPullRequest(final String topic, final int queueId, final PullRequest pullRequest) {
    	/**
    	 * 构造key值
    	 */
        String key = this.buildKey(topic, queueId);
        
        /**
         * 查看是否已经存在该请求
         */
        ManyPullRequest mpr = this.pullRequestTable.get(key);
        
        /**
         * 如果没有对应topic的队列请求容器
         * 则应该创建
         */
        if (null == mpr) {
            mpr = new ManyPullRequest();
            ManyPullRequest prev = this.pullRequestTable.putIfAbsent(key, mpr);
            if (prev != null) {
                mpr = prev;
            }
        }

        /**
         * 把请求放入容器
         */
        mpr.addPullRequest(pullRequest);
    }

    /**
     * @描述: 构建key值 
     * 			规则：topic+ '@' + queueid
     * @作者: zgzhang@txbds.com
     * @日期:2016年11月24日
     * @修改内容
     * @参数：@param topic    topic name
     * @参数：@param queueId  队列编号
     * @参数：@return     
     * @throws
     */
    private String buildKey(final String topic, final int queueId) {
        StringBuilder sb = new StringBuilder();
        sb.append(topic);
        sb.append(TOPIC_QUEUEID_SEPARATOR);
        sb.append(queueId);
        return sb.toString();
    }

    @Override
    public void run() {
        log.info(this.getServiceName() + " service started");
        while (!this.isStoped()) {
            try {
            	/**
            	 * 如果broker配置了支持批量拉取
            	 * 则会等待10秒中处理一次
            	 */
                if (this.brokerController.getBrokerConfig().isLongPollingEnable()) {
                    this.waitForRunning(10 * 1000);
                } 
                else 
                {
                	/**
                	 * 否则会等待默认的1秒
                	 */
                    this.waitForRunning(this.brokerController.getBrokerConfig().getShortPollingTimeMills());
                }
                this.checkHoldRequest();
            } catch (Exception e) {
                log.warn(this.getServiceName() + " service has exception. ", e);
            }
        }

        log.info(this.getServiceName() + " service end");
    }

    @Override
    public String getServiceName() {
        return PullRequestHoldService.class.getSimpleName();
    }

    /**
     * @描述: 检查暂停的请求 
     * @作者: zgzhang@txbds.com
     * @日期:2016年11月24日
     * @修改内容
     * @参数：     
     * @throws
     */
    private void checkHoldRequest() {
        for (String key : this.pullRequestTable.keySet()) {
            String[] kArray = key.split(TOPIC_QUEUEID_SEPARATOR);
            if (kArray != null && 2 == kArray.length) {
                String topic = kArray[0];
                int queueId = Integer.parseInt(kArray[1]);
                final long offset = this.brokerController.getMessageStore().getMaxOffsetInQuque(topic, queueId);
                this.notifyMessageArriving(topic, queueId, offset);
            }
        }
    }

    /**
     * @描述: 消息到达通知 
     * @作者: zgzhang@txbds.com
     * @日期:2016年11月24日
     * @修改内容
     * @参数：@param topic
     * @参数：@param queueId
     * @参数：@param maxOffset     
     * @throws
     */
    public void notifyMessageArriving(final String topic, final int queueId, final long maxOffset) {
        notifyMessageArriving(topic, queueId, maxOffset, null);
    }

    /**
     * @描述: 消息到达通知 
     * @作者: zgzhang@txbds.com
     * @日期:2016年11月24日
     * @修改内容
     * @参数：@param topic
     * @参数：@param queueId
     * @参数：@param maxOffset
     * @参数：@param tagsCode     
     * @throws
     */
    public void notifyMessageArriving(final String topic, final int queueId, final long maxOffset, final Long tagsCode) {
        String key = this.buildKey(topic, queueId);
        ManyPullRequest mpr = this.pullRequestTable.get(key);
        if (mpr != null) {
            List<PullRequest> requestList = mpr.cloneListAndClear();
            if (requestList != null) {
                List<PullRequest> replayList = new ArrayList<PullRequest>();

                for (PullRequest request : requestList) {
                    long newestOffset = maxOffset;
                    if (newestOffset <= request.getPullFromThisOffset()) {
                        newestOffset = this.brokerController.getMessageStore().getMaxOffsetInQuque(topic, queueId);
                    }

                    Long tmp = tagsCode;
                    if (newestOffset > request.getPullFromThisOffset()) {
                        if (tagsCode == null) {
                            // tmp = getLatestMessageTagsCode(topic, queueId,
                            // maxOffset);
                        }
                        
                        /**
                         * 使用默认的过滤器匹配消息是否匹配
                         */
                        if (this.messageFilter.isMessageMatched(request.getSubscriptionData(), tmp)) {
                            try {
                                this.brokerController.getPullMessageProcessor().excuteRequestWhenWakeup(request.getClientChannel(),
                                        request.getRequestCommand());
                            } catch (RemotingCommandException e) {
                                log.error("", e);
                            }
                            continue;
                        }
                    }

                    /**
                     * 
                     */
                    if (System.currentTimeMillis() >= (request.getSuspendTimestamp() + request.getTimeoutMillis())) {
                        try {
                            this.brokerController.getPullMessageProcessor().excuteRequestWhenWakeup(request.getClientChannel(),
                                    request.getRequestCommand());
                        } catch (RemotingCommandException e) {
                            log.error("", e);
                        }
                        continue;
                    }


                    replayList.add(request);
                }

                if (!replayList.isEmpty()) {
                    mpr.addPullRequest(replayList);
                }
            }
        }
    }
}
