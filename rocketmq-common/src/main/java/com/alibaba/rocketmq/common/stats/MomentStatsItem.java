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
 * @类功能说明：统计状态定义
 * @改者：zgzhang@txbds.com
 * @修改日期：2016年11月24日
 * @修改说明：
 * @公司名称：www.txbds.com
 * @创建时间：2016年11月24日
 */
public class MomentStatsItem {
	/**
	 * 统计指标数据
	 */
    private final AtomicLong value = new AtomicLong(0);

    /**
     * 名称
     */
    private final String statsName;
    
    /**
     * key值
     */
    private final String statsKey;
    
    /**
     * 调度服务
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
                                                                	   * 打印数据
                                                                	   */
                                                                      printAtMinutes();
                                                                      /**
                                                                       * 统计指标数据清理
                                                                       */
                                                                      MomentStatsItem.this.value.set(0);
                                                                  } catch (Throwable e) {
                                                                  }
                                                              }
                                                          }, Math.abs(UtilAll.computNextMinutesTimeMillis() - System.currentTimeMillis()), //1秒后运行
                1000 * 60 * 5, //5分钟运行一次 
                TimeUnit.MILLISECONDS);
    }

    /**
     * @描述: 打印数据 
     * @作者: zgzhang@txbds.com
     * @日期:2016年11月24日
     * @修改内容
     * @参数：     
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
