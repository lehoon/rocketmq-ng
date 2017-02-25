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

package com.alibaba.rocketmq.common.stats;

import com.alibaba.rocketmq.common.UtilAll;
import org.slf4j.Logger;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @�๦��˵����ͳ��״̬����
 * @���ߣ�zgzhang@txbds.com
 * @�޸����ڣ�2016��11��24��
 * @�޸�˵����
 * @��˾���ƣ�www.txbds.com
 * @����ʱ�䣺2016��11��24��
 */
public class MomentStatsItem {
	/**
	 * ͳ��ָ������
	 */
    private final AtomicLong value = new AtomicLong(0);

    /**
     * ����
     */
    private final String statsName;
    
    /**
     * keyֵ
     */
    private final String statsKey;
    
    /**
     * ���ȷ���
     */
    private final ScheduledExecutorService scheduledExecutorService;
    
    /**
     * log handler
     */
    private final Logger log;


    public MomentStatsItem(String statsName, String statsKey,
                           ScheduledExecutorService scheduledExecutorService, Logger log) {
        this.statsName = statsName;
        this.statsKey = statsKey;
        this.scheduledExecutorService = scheduledExecutorService;
        this.log = log;
    }


    public void init() {

        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
                                                              @Override
                                                              public void run() {
                                                                  try {
                                                                	  /**
                                                                	   * ��ӡ����
                                                                	   */
                                                                      printAtMinutes();
                                                                      /**
                                                                       * ͳ��ָ����������
                                                                       */
                                                                      MomentStatsItem.this.value.set(0);
                                                                  } catch (Throwable e) {
                                                                  }
                                                              }
                                                          }, Math.abs(UtilAll.computNextMinutesTimeMillis() - System.currentTimeMillis()), //1�������
                1000 * 60 * 5, //5��������һ�� 
                TimeUnit.MILLISECONDS);
    }

    /**
     * @����: ��ӡ���� 
     * @����: zgzhang@txbds.com
     * @����:2016��11��24��
     * @�޸�����
     * @������     
     * @throws
     */
    public void printAtMinutes() {
        log.info(String.format("[%s] [%s] Stats Every 5 Minutes, Value: %d", //
                this.statsName,//
                this.statsKey,//
                this.value.get()));
    }

    public AtomicLong getValue() {
        return value;
    }


    public String getStatsKey() {
        return statsKey;
    }


    public String getStatsName() {
        return statsName;
    }
}