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
package com.alibaba.rocketmq.broker.longpolling;

import java.util.ArrayList;
import java.util.List;


/**
 * 拉取请求容器
 * @author shijia.wxr
 */
public class ManyPullRequest 
{
	/**
	 * 请求容器
	 */
    private final ArrayList<PullRequest> pullRequestList = new ArrayList<PullRequest>();


    public synchronized void addPullRequest(final PullRequest pullRequest) {
        this.pullRequestList.add(pullRequest);
    }


    /**
     * @描述: 批量添加请求 
     * @作者: zgzhang@txbds.com
     * @日期:2016年11月24日
     * @修改内容
     * @参数：@param many     
     * @throws
     */
    public synchronized void addPullRequest(final List<PullRequest> many) {
        this.pullRequestList.addAll(many);
    }

    /**
     * @描述: 复制并且清空请求容器 
     * @作者: zgzhang@txbds.com
     * @日期:2016年11月24日
     * @修改内容
     * @参数：@return     
     * @throws
     */
    public synchronized List<PullRequest> cloneListAndClear() {
        if (!this.pullRequestList.isEmpty()) {
            List<PullRequest> result = (ArrayList<PullRequest>) this.pullRequestList.clone();
            this.pullRequestList.clear();
            return result;
        }

        return null;
    }
}
