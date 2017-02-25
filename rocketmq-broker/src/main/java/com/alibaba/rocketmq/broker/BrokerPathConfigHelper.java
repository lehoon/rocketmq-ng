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

package com.alibaba.rocketmq.broker;

import java.io.File;

/**
 * @类功能说明： broker的路径配置脚手架
 * @改者：zgzhang@txbds.com
 * @修改日期：2016年11月24日
 * @修改说明：
 * @公司名称：www.txbds.com
 * @创建时间：2016年11月24日
 */
public class BrokerPathConfigHelper 
{
	/**
	 * 默认的配置文件在用户主目录下
	 * 构造路径为
	 * user.home/store/config/broker.properties
	 */
    private static String brokerConfigPath = System.getProperty("user.home") + File.separator + "store"
            + File.separator + "config" + File.separator + "broker.properties";


    public static String getBrokerConfigPath() {
        return brokerConfigPath;
    }


    public static void setBrokerConfigPath(String path) {
        brokerConfigPath = path;
    }


    public static String getTopicConfigPath(final String rootDir) {
        return rootDir + File.separator + "config" + File.separator + "topics.json";
    }


    public static String getConsumerOffsetPath(final String rootDir) {
        return rootDir + File.separator + "config" + File.separator + "consumerOffset.json";
    }


    public static String getSubscriptionGroupPath(final String rootDir) {
        return rootDir + File.separator + "config" + File.separator + "subscriptionGroup.json";
    }

}
