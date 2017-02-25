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
package com.alibaba.rocketmq.common;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;


/**
 * 系统时钟
 * @author vintage.wang
 */
public class SystemClock {

    private final long precision;
    private final AtomicLong now;


    public SystemClock(long precision) {
        this.precision = precision;//init value is 1
        now = new AtomicLong(System.currentTimeMillis());
        scheduleClockUpdating();
    }

    /**
     * @描述: 更新时钟调度 
     * @作者: zgzhang@txbds.com
     * @日期:2016年11月22日
     * @修改内容
     * @参数：     
     * @return void   
     * @throws
     */
    private void scheduleClockUpdating() {
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
            @Override
            public Thread newThread(Runnable runnable) {
                Thread thread = new Thread(runnable, "System Clock");
                thread.setDaemon(true);
                return thread;
            }
        });
        
        //一毫秒更新 一次时间 precision=1
        scheduler.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                now.set(System.currentTimeMillis());
            }
        }, precision, precision, TimeUnit.MILLISECONDS);
    }


    public long now() {
        return now.get();
    }


    public long precision() {
        return precision;
    }
}
