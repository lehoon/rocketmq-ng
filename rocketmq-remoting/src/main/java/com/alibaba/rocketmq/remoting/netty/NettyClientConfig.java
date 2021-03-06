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
 * netty 客户端配置
 * @author shijia.wxr
 *
 */
public class NettyClientConfig {
    /**
     * work线程池
     * Worker thread number
     */
    private int clientWorkerThreads = 4;
    /**
     * 客户端回调线程池大小  
     * 取cpu数目
     */
    private int clientCallbackExecutorThreads = Runtime.getRuntime().availableProcessors();
    /**
     * 一次调用信号量值
     * 默认65535
     * 可以通过环境变量或者配置文件修改
     */
    private int clientOnewaySemaphoreValue = NettySystemConfig.ClientOnewaySemaphoreValue;
    /**
     * 异步发送信号量值
     * 默认65535
     * 可以通过环境变量或者配置文件修改
     */
    private int clientAsyncSemaphoreValue = NettySystemConfig.ClientAsyncSemaphoreValue;
    /**
     * 连接超时时间
     * 默认3秒
     */
    private int connectTimeoutMillis = 3000;
    /**
     * 连接超时时间
     * 60秒
     */
    private long channelNotActiveInterval = 1000 * 60;

    /**
     * 如果120秒没有事件（读写）发送, 则会产生IdleStateEvent事件
     * 设置为0 则进制产生IdleStateEvent事件
     * IdleStateEvent will be triggered when neither read nor write was performed for
     * the specified period of this time. Specify {@code 0} to disable
     */
    private int clientChannelMaxIdleTimeSeconds = 120;
    /**
     * 客户端发送缓冲区大小
     */
    private int clientSocketSndBufSize = NettySystemConfig.socketSndbufSize;
    /**
     * 接收端缓冲大小
     */
    private int clientSocketRcvBufSize = NettySystemConfig.socketRcvbufSize;
    /**
     * 是否启用内存池分配内存
     */
    private boolean clientPooledByteBufAllocatorEnable = false;
    /**
     * 超时是否关闭socket
     */
    private boolean clientCloseSocketIfTimeout = false;

    public boolean isClientCloseSocketIfTimeout() {
        return clientCloseSocketIfTimeout;
    }

    public void setClientCloseSocketIfTimeout(final boolean clientCloseSocketIfTimeout) {
        this.clientCloseSocketIfTimeout = clientCloseSocketIfTimeout;
    }

    public int getClientWorkerThreads() {
        return clientWorkerThreads;
    }


    public void setClientWorkerThreads(int clientWorkerThreads) {
        this.clientWorkerThreads = clientWorkerThreads;
    }


    public int getClientOnewaySemaphoreValue() {
        return clientOnewaySemaphoreValue;
    }


    public void setClientOnewaySemaphoreValue(int clientOnewaySemaphoreValue) {
        this.clientOnewaySemaphoreValue = clientOnewaySemaphoreValue;
    }


    public int getConnectTimeoutMillis() {
        return connectTimeoutMillis;
    }


    public void setConnectTimeoutMillis(int connectTimeoutMillis) {
        this.connectTimeoutMillis = connectTimeoutMillis;
    }


    public int getClientCallbackExecutorThreads() {
        return clientCallbackExecutorThreads;
    }


    public void setClientCallbackExecutorThreads(int clientCallbackExecutorThreads) {
        this.clientCallbackExecutorThreads = clientCallbackExecutorThreads;
    }


    public long getChannelNotActiveInterval() {
        return channelNotActiveInterval;
    }


    public void setChannelNotActiveInterval(long channelNotActiveInterval) {
        this.channelNotActiveInterval = channelNotActiveInterval;
    }


    public int getClientAsyncSemaphoreValue() {
        return clientAsyncSemaphoreValue;
    }


    public void setClientAsyncSemaphoreValue(int clientAsyncSemaphoreValue) {
        this.clientAsyncSemaphoreValue = clientAsyncSemaphoreValue;
    }


    public int getClientChannelMaxIdleTimeSeconds() {
        return clientChannelMaxIdleTimeSeconds;
    }


    public void setClientChannelMaxIdleTimeSeconds(int clientChannelMaxIdleTimeSeconds) {
        this.clientChannelMaxIdleTimeSeconds = clientChannelMaxIdleTimeSeconds;
    }


    public int getClientSocketSndBufSize() {
        return clientSocketSndBufSize;
    }


    public void setClientSocketSndBufSize(int clientSocketSndBufSize) {
        this.clientSocketSndBufSize = clientSocketSndBufSize;
    }


    public int getClientSocketRcvBufSize() {
        return clientSocketRcvBufSize;
    }


    public void setClientSocketRcvBufSize(int clientSocketRcvBufSize) {
        this.clientSocketRcvBufSize = clientSocketRcvBufSize;
    }


    public boolean isClientPooledByteBufAllocatorEnable() {
        return clientPooledByteBufAllocatorEnable;
    }


    public void setClientPooledByteBufAllocatorEnable(boolean clientPooledByteBufAllocatorEnable) {
        this.clientPooledByteBufAllocatorEnable = clientPooledByteBufAllocatorEnable;
    }
}
