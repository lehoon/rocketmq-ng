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
 * ��Ϣ��Ӧ
 * @author shijia.wxr
 */
public class ResponseFuture {
	/**
	 * ��ˮ��
	 */
    private final int opaque;
    /**
     * ��ʱʱ��
     */
    private final long timeoutMillis;
    /**
     * �ص�����
     */
    private final InvokeCallback invokeCallback;
    /**
     * ��ʼʱ��
     */
    private final long beginTimestamp = System.currentTimeMillis();
    /**
     * ֻ����һ���̵߳ȴ�,ͬ������   ʵ�����Ĺ���
     */
    private final CountDownLatch countDownLatch = new CountDownLatch(1);
    /**
     * �ź���
     */
    private final SemaphoreReleaseOnlyOnce once;
    /**
     * �Ƿ�ص�
     */
    private final AtomicBoolean executeCallbackOnlyOnce = new AtomicBoolean(false);
    /**
     * ��Ӧ���������
     */
    private volatile RemotingCommand responseCommand;
    /**
     * ��Ϣ���ͳɹ�ʧ�ܱ�־
     */
    private volatile boolean sendRequestOK = true;
    /**
     * �׳��쳣
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
     * @����: ��Ӧ�ص����� 
     * @����: zgzhang@txbds.com
     * @����:2016��11��24��
     * @�޸�����
     * @������     
     * @throws
     */
    public void executeInvokeCallback() {
        if (invokeCallback != null) {
        	/**
        	 * ���û�лص�,��ص���Ӧ���
        	 */
            if (this.executeCallbackOnlyOnce.compareAndSet(false, true)) {
                invokeCallback.operationComplete(this);
            }
        }
    }

    /**
     * @����: �ͷ��� 
     * @����: zgzhang@txbds.com
     * @����:2016��11��24��
     * @�޸�����
     * @������     
     * @throws
     */
    public void release() {
        if (this.once != null) {
            this.once.release();
        }
    }

    /**
     * @����: �Ƿ�ʱ 
     * @����: zgzhang@txbds.com
     * @����:2016��11��24��
     * @�޸�����
     * @������@return     
     * @throws
     */
    public boolean isTimeout() {
        long diff = System.currentTimeMillis() - this.beginTimestamp;
        return diff > this.timeoutMillis;
    }

    /**
     * @����: �ȴ���Ӧ 
     * @����: zgzhang@txbds.com
     * @����:2016��11��24��
     * @�޸�����
     * @������@param timeoutMillis
     * @������@return
     * @������@throws InterruptedException     
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
