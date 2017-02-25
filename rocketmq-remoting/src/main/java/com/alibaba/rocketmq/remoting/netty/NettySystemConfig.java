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
 * @�๦��˵���� netty�����ļ�
 * @���ߣ�zgzhang@txbds.com
 * @�޸����ڣ�2016��11��24��
 * @�޸�˵����
 * @��˾���ƣ�www.txbds.com
 * @����ʱ�䣺2016��11��24��
 */
public class NettySystemConfig {
	/**
	 * �Ƿ������ڴ��
	 */
    public static final String SystemPropertyNettyPooledByteBufAllocatorEnable = "com.rocketmq.remoting.nettyPooledByteBufAllocatorEnable";
    /**
     * ���ͻ�������С
     */
    public static final String SystemPropertySocketSndbufSize = "com.rocketmq.remoting.socket.sndbuf.size";
    /**
     * ���ջ�������С
     */
    public static final String SystemPropertySocketRcvbufSize = "com.rocketmq.remoting.socket.rcvbuf.size";
    /**
     * �첽���͵ȴ��ź���ֵ,ͬʱ�������ִ����
     */
    public static final String SystemPropertyClientAsyncSemaphoreValue = "com.rocketmq.remoting.clientAsyncSemaphoreValue";
    /**
     * һ���Է����ź���ֵ
     */
    public static final String SystemPropertyClientOnewaySemaphoreValue = "com.rocketmq.remoting.clientOnewaySemaphoreValue";
    /**
     * ���ݻ���������ȡ�Ƿ�����netty���ڴ�ط����ڴ�
     * ������� 
     * Ĭ��false
     */
    public static final boolean NettyPooledByteBufAllocatorEnable = Boolean.parseBoolean(System.getProperty(SystemPropertyNettyPooledByteBufAllocatorEnable, "false"));
    /**
     * �����������÷��ͻ�������С
     */
    public static int socketSndbufSize = Integer.parseInt(System.getProperty(SystemPropertySocketSndbufSize, "65535"));
    /**
     * �����������ý��ջ�������С
     */
    public static int socketRcvbufSize = Integer.parseInt(System.getProperty(SystemPropertySocketRcvbufSize, "65535"));
    /**
     * �����������õ��첽��������ź���ֵ  Ĭ��65535
     */
    public static final int ClientAsyncSemaphoreValue = Integer.parseInt(System.getProperty(SystemPropertyClientAsyncSemaphoreValue, "65535"));
    /**
     * �����������õ����һ�η����ź�����ֵ Ĭ��65535
     */
    public static final int ClientOnewaySemaphoreValue = Integer.parseInt(System.getProperty(SystemPropertyClientOnewaySemaphoreValue, "65535"));
}
