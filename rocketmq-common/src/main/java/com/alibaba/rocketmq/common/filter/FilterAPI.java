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
package com.alibaba.rocketmq.common.filter;

import com.alibaba.rocketmq.common.protocol.heartbeat.SubscriptionData;

import java.net.URL;


/**
 * 过滤api
 * @author shijia.wxr
 *
 */
public class FilterAPI {
	/**
	 * @描述: 生成class文件url 
	 * @作者: zgzhang@txbds.com
	 * @日期:2016年11月23日
	 * @修改内容
	 * @参数：@param className
	 * @参数：@return     
	 * @return URL   
	 * @throws
	 */
    public static URL classFile(final String className) {
        final String javaSource = simpleClassName(className) + ".java";
        URL url = FilterAPI.class.getClassLoader().getResource(javaSource);
        return url;
    }

    public static String simpleClassName(final String className) {
        String simple = className;
        int index = className.lastIndexOf(".");
        if (index >= 0) {
            simple = className.substring(index + 1);
        }

        return simple;
    }

    /**
     * @描述: 构造订阅数据 
     * @作者: zgzhang@txbds.com
     * @日期:2016年11月23日
     * @修改内容
     * @参数：@param consumerGroup  消费组
     * @参数：@param topic    订阅主题
     * @参数：@param subString   描述字段
     * @参数：@return
     * @参数：@throws Exception     
     * @return SubscriptionData  订阅数据 
     * @throws
     */
    public static SubscriptionData buildSubscriptionData(final String consumerGroup, String topic,
                                                         String subString) throws Exception {
        SubscriptionData subscriptionData = new SubscriptionData();
        subscriptionData.setTopic(topic);
        subscriptionData.setSubString(subString);

        if (null == subString || subString.equals(SubscriptionData.SUB_ALL) || subString.length() == 0) {
            subscriptionData.setSubString(SubscriptionData.SUB_ALL);
        } else {
            String[] tags = subString.split("\\|\\|");
            if (tags != null && tags.length > 0) {
                for (String tag : tags) {
                    if (tag.length() > 0) {
                        String trimString = tag.trim();
                        if (trimString.length() > 0) {
                            subscriptionData.getTagsSet().add(trimString);
                            subscriptionData.getCodeSet().add(trimString.hashCode());
                        }
                    }
                }
            } else {
                throw new Exception("subString split error");
            }
        }

        return subscriptionData;
    }
}
