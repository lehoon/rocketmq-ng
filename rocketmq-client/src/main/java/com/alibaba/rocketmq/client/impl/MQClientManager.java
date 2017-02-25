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
package com.alibaba.rocketmq.client.impl;

import com.alibaba.rocketmq.client.ClientConfig;
import com.alibaba.rocketmq.client.impl.factory.MQClientInstance;
import com.alibaba.rocketmq.remoting.RPCHook;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;


/**
 * 客户端连接管理器
 * @author shijia.wxr
 */
public class MQClientManager {
    /**
     * 单利模式下的对象
     */
	private static MQClientManager instance = new MQClientManager();

	/**
	 * 数据计数器
	 */
    private AtomicInteger factoryIndexGenerator = new AtomicInteger();

    /**
     * 维护客户端连接数组
     * 客户端id  <----> 连接对象
     */
    private ConcurrentHashMap<String/* clientId */, MQClientInstance> factoryTable =
            new ConcurrentHashMap<String, MQClientInstance>();


    private MQClientManager() {

    }


    public static MQClientManager getInstance() {
        return instance;
    }

    /**
     * @描述: 根据客户端配置文件创建连接对象 
     * @作者: zgzhang@txbds.com
     * @日期:2016年11月23日
     * @修改内容
     * @参数：@param clientConfig
     * @参数：@return     
     * @throws
     */
    public MQClientInstance getAndCreateMQClientInstance(final ClientConfig clientConfig) {
        return getAndCreateMQClientInstance(clientConfig, null);
    }

    /**
     * @描述: 根据客户端配置文件创建连接对象 
     * @作者: zgzhang@txbds.com
     * @日期:2016年11月23日
     * @修改内容
     * @参数：@param clientConfig    消费者对象
     * @参数：@param rpcHook         拦截器
     * @参数：@return     
     * @throws
     */
    public MQClientInstance getAndCreateMQClientInstance(final ClientConfig clientConfig, RPCHook rpcHook) 
    {
    	/**
    	 * 生成客户端编号
    	 */
        String clientId = clientConfig.buildMQClientId();
        /**
         * 先从工厂里面查找，如果有直接返回，没有则需要创建
         */
        MQClientInstance instance = this.factoryTable.get(clientId);
        
        
        if (null == instance) 
        {
            instance =
                    new MQClientInstance(clientConfig.cloneClientConfig(),	//配置文件
                            this.factoryIndexGenerator.getAndIncrement(), //索引id 
                            clientId, //客户端id
                            rpcHook); //拦截器
            MQClientInstance prev = this.factoryTable.putIfAbsent(clientId, instance);
            if (prev != null) {
                instance = prev;
            } else {
                // TODO log
            }
        }

        return instance;
    }

    public void removeClientFactory(final String clientId) {
        this.factoryTable.remove(clientId);
    }
}
