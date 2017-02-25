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

import com.alibaba.rocketmq.remoting.InvokeCallback;
import com.alibaba.rocketmq.remoting.common.SemaphoreReleaseOnlyOnce;
import com.alibaba.rocketmq.remoting.protocol.RemotingCommand;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;


/**
 * 消息响应
 * @author shijia.wxr
 */
public class ResponseFuture {
	/**
	 * 流水号
	 */
    private final int opaque;
    /**
     * 超时时间
     */
    private final long timeoutMillis;
    /**
     * 回调函数
     */
    private final InvokeCallback invokeCallback;
    /**
     * 开始时间
     */
    private final long beginTimestamp = System.currentTimeMillis();
    /**
     * 只允许一个线程等待,同步助手   实现锁的功能
     */
    private final CountDownLatch countDownLatch = new CountDownLatch(1);
    /**
     * 信号量
     */
    private final SemaphoreReleaseOnlyOnce once;
    /**
     * 是否回调
     */
    private final AtomicBoolean executeCallbackOnlyOnce = new AtomicBoolean(false);
    /**
     * 响应命令控制器
     */
    private volatile RemotingCommand responseCommand;
    /**
     * 消息发送成功失败标志
     */
    private volatile boolean sendRequestOK = true;
    /**
     * 抛出异常
     */
    private volatile Throwable cause;


    public ResponseFuture(int opaque, long timeoutMillis, InvokeCallback invokeCallback,
                          SemaphoreReleaseOnlyOnce once) {
        this.opaque = opaque;
        this.timeoutMillis = timeoutMillis;
        this.invokeCallback = invokeCallback;
        this.once = once;
    }

    /**
     * @描述: 响应回调函数 
     * @作者: zgzhang@txbds.com
     * @日期:2016年11月24日
     * @修改内容
     * @参数：     
     * @throws
     */
    public void executeInvokeCallback() {
        if (invokeCallback != null) {
        	/**
        	 * 如果没有回调,则回调响应结果
        	 */
            if (this.executeCallbackOnlyOnce.compareAndSet(false, true)) {
                invokeCallback.operationComplete(this);
            }
        }
    }

    /**
     * @描述: 释放锁 
     * @作者: zgzhang@txbds.com
     * @日期:2016年11月24日
     * @修改内容
     * @参数：     
     * @throws
     */
    public void release() {
        if (this.once != null) {
            this.once.release();
        }
    }

    /**
     * @描述: 是否超时 
     * @作者: zgzhang@txbds.com
     * @日期:2016年11月24日
     * @修改内容
     * @参数：@return     
     * @throws
     */
    public boolean isTimeout() {
        long diff = System.currentTimeMillis() - this.beginTimestamp;
        return diff > this.timeoutMillis;
    }

    /**
     * @描述: 等待响应 
     * @作者: zgzhang@txbds.com
     * @日期:2016年11月24日
     * @修改内容
     * @参数：@param timeoutMillis
     * @参数：@return
     * @参数：@throws InterruptedException     
     * @throws
     */
    public RemotingCommand waitResponse(final long timeoutMillis) throws InterruptedException {
        this.countDownLatch.await(timeoutMillis, TimeUnit.MILLISECONDS);
        return this.responseCommand;
    }


    public void putResponse(final RemotingCommand responseCommand) {
        this.responseCommand = responseCommand;
        this.countDownLatch.countDown();
    }


    public long getBeginTimestamp() {
        return beginTimestamp;
    }


    public boolean isSendRequestOK() {
        return sendRequestOK;
    }


    public void setSendRequestOK(boolean sendRequestOK) {
        this.sendRequestOK = sendRequestOK;
    }


    public long getTimeoutMillis() {
        return timeoutMillis;
    }


    public InvokeCallback getInvokeCallback() {
        return invokeCallback;
    }


    public Throwable getCause() {
        return cause;
    }


    public void setCause(Throwable cause) {
        this.cause = cause;
    }


    public RemotingCommand getResponseCommand() {
        return responseCommand;
    }


    public void setResponseCommand(RemotingCommand responseCommand) {
        this.responseCommand = responseCommand;
    }


    public int getOpaque() {
        return opaque;
    }


    @Override
    public String toString() {
        return "ResponseFuture [responseCommand=" + responseCommand + ", sendRequestOK=" + sendRequestOK
                + ", cause=" + cause + ", opaque=" + opaque + ", timeoutMillis=" + timeoutMillis
                + ", invokeCallback=" + invokeCallback + ", beginTimestamp=" + beginTimestamp
                + ", countDownLatch=" + countDownLatch + "]";
    }
}
