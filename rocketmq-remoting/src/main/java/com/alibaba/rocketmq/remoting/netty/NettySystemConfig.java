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

package com.alibaba.rocketmq.remoting.netty;

/**
 * @类功能说明： netty配置文件
 * @改者：zgzhang@txbds.com
 * @修改日期：2016年11月24日
 * @修改说明：
 * @公司名称：www.txbds.com
 * @创建时间：2016年11月24日
 */
public class NettySystemConfig {
	/**
	 * 是否启用内存池
	 */
    public static final String SystemPropertyNettyPooledByteBufAllocatorEnable = "com.rocketmq.remoting.nettyPooledByteBufAllocatorEnable";
    /**
     * 发送缓冲区大小
     */
    public static final String SystemPropertySocketSndbufSize = "com.rocketmq.remoting.socket.sndbuf.size";
    /**
     * 接收缓冲区大小
     */
    public static final String SystemPropertySocketRcvbufSize = "com.rocketmq.remoting.socket.rcvbuf.size";
    /**
     * 异步发送等待信号量值,同时允许最大执行数
     */
    public static final String SystemPropertyClientAsyncSemaphoreValue = "com.rocketmq.remoting.clientAsyncSemaphoreValue";
    /**
     * 一次性发送信号量值
     */
    public static final String SystemPropertyClientOnewaySemaphoreValue = "com.rocketmq.remoting.clientOnewaySemaphoreValue";
    /**
     * 根据环境变量获取是否启用netty的内存池分配内存
     * 提高性能 
     * 默认false
     */
    public static final boolean NettyPooledByteBufAllocatorEnable = Boolean.parseBoolean(System.getProperty(SystemPropertyNettyPooledByteBufAllocatorEnable, "false"));
    /**
     * 环境变量配置发送缓冲区大小
     */
    public static int socketSndbufSize = Integer.parseInt(System.getProperty(SystemPropertySocketSndbufSize, "65535"));
    /**
     * 环境变量配置接收缓冲区大小
     */
    public static int socketRcvbufSize = Integer.parseInt(System.getProperty(SystemPropertySocketRcvbufSize, "65535"));
    /**
     * 环境变量配置的异步发送最大信号量值  默认65535
     */
    public static final int ClientAsyncSemaphoreValue = Integer.parseInt(System.getProperty(SystemPropertyClientAsyncSemaphoreValue, "65535"));
    /**
     * 环境变量配置的最大一次发送信号量的值 默认65535
     */
    public static final int ClientOnewaySemaphoreValue = Integer.parseInt(System.getProperty(SystemPropertyClientOnewaySemaphoreValue, "65535"));
}
