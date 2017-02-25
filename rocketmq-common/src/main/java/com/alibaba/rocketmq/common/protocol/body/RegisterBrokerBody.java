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

package com.alibaba.rocketmq.common.protocol.body;

import com.alibaba.rocketmq.remoting.protocol.RemotingSerializable;

import java.util.ArrayList;
import java.util.List;

/**
 * @类功能说明： broker注册请求消息体
 * @改者：zgzhang@txbds.com
 * @修改日期：2016年11月24日
 * @修改说明：
 * @公司名称：www.txbds.com
 * @创建时间：2016年11月24日
 */
public class RegisterBrokerBody extends RemotingSerializable {
    private TopicConfigSerializeWrapper topicConfigSerializeWrapper = new TopicConfigSerializeWrapper();
    private List<String> filterServerList = new ArrayList<String>();


    public TopicConfigSerializeWrapper getTopicConfigSerializeWrapper() {
        return topicConfigSerializeWrapper;
    }


    public void setTopicConfigSerializeWrapper(TopicConfigSerializeWrapper topicConfigSerializeWrapper) {
        this.topicConfigSerializeWrapper = topicConfigSerializeWrapper;
    }


    public List<String> getFilterServerList() {
        return filterServerList;
    }


    public void setFilterServerList(List<String> filterServerList) {
        this.filterServerList = filterServerList;
    }
}
