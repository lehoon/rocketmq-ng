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
package com.alibaba.rocketmq.client.consumer.store;

import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.client.impl.factory.MQClientInstance;
import com.alibaba.rocketmq.client.log.ClientLogger;
import com.alibaba.rocketmq.common.MixAll;
import com.alibaba.rocketmq.common.UtilAll;
import com.alibaba.rocketmq.common.help.FAQUrl;
import com.alibaba.rocketmq.common.message.MessageQueue;
import org.slf4j.Logger;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;


/**
 * ���ش洢ʵ��
 * @author shijia.wxr
 */
public class LocalFileOffsetStore implements OffsetStore 
{
    public final static String LocalOffsetStoreDir = System.getProperty("rocketmq.client.localOffsetStoreDir", System.getProperty("user.home") + File.separator + ".rocketmq_offsets");
    private final static Logger log = ClientLogger.getLog();
    /**
     * �ͻ���ʵ������
     * ��װ��netty��socket����
     */
    private final MQClientInstance mQClientFactory;
    /**
     * ������
     */
    private final String groupName;
    /**
     * �洢·��
     */
    private final String storePath;

    /**
     * ��Ϣ������Ϣƫ������
     * ��Ϣ����-----------��Ϣƫ����
     */
    private ConcurrentHashMap<MessageQueue, AtomicLong> offsetTable = new ConcurrentHashMap<MessageQueue, AtomicLong>();

    /**
     * ���캯��
     * @param mQClientFactory
     * @param groupName
     */
    public LocalFileOffsetStore(MQClientInstance mQClientFactory, String groupName) {
        this.mQClientFactory = mQClientFactory;
        this.groupName = groupName;
        this.storePath = LocalOffsetStoreDir + File.separator + //
                this.mQClientFactory.getClientId() + File.separator + //
                this.groupName + File.separator + //
                "offsets.json";
    }

    /**
     * ������Ϣ����ƫ��������
     */
    @Override
    public void load() throws MQClientException {
        OffsetSerializeWrapper offsetSerializeWrapper = this.readLocalOffset();
        if (offsetSerializeWrapper != null && offsetSerializeWrapper.getOffsetTable() != null) {
            offsetTable.putAll(offsetSerializeWrapper.getOffsetTable());

            /**
             * ��ӡÿ����Ϣ���е�ƫ����
             */
            for (MessageQueue mq : offsetSerializeWrapper.getOffsetTable().keySet()) {
                AtomicLong offset = offsetSerializeWrapper.getOffsetTable().get(mq);
                log.info("load consumer's offset, {} {} {}",//
                        this.groupName,//
                        mq,//
                        offset.get());
            }
        }
    }


    /**
     * ���±��ص��ڴ�����Ϣ������Ϣƫ����
     * mq ��Ϣ����
     * offset ƫ����
     * increaseOnly
     */
    @Override
    public void updateOffset(MessageQueue mq, long offset, boolean increaseOnly) {
        if (mq != null) {
            AtomicLong offsetOld = this.offsetTable.get(mq);
            if (null == offsetOld) {
                offsetOld = this.offsetTable.putIfAbsent(mq, new AtomicLong(offset));
            }

            if (null != offsetOld) {
                if (increaseOnly) {
                    MixAll.compareAndIncreaseOnly(offsetOld, offset);
                } else {
                    offsetOld.set(offset);
                }
            }
        }
    }

    /**
     * ��ȡ��Ϣ����ƫ����
     * mq ��Ϣ����
     * type ��ȡ��ʽ
     * 
     */
    @Override
    public long readOffset(final MessageQueue mq, final ReadOffsetType type) {
        if (mq != null) {
            switch (type) {
                case MEMORY_FIRST_THEN_STORE:
                case READ_FROM_MEMORY: {
                    AtomicLong offset = this.offsetTable.get(mq);
                    if (offset != null) {
                        return offset.get();
                    } else if (ReadOffsetType.READ_FROM_MEMORY == type) {
                        return -1;
                    }
                }
                case READ_FROM_STORE: {
                    OffsetSerializeWrapper offsetSerializeWrapper;
                    try {
                        offsetSerializeWrapper = this.readLocalOffset();
                    } catch (MQClientException e) {
                        return -1;
                    }
                    if (offsetSerializeWrapper != null && offsetSerializeWrapper.getOffsetTable() != null) {
                        AtomicLong offset = offsetSerializeWrapper.getOffsetTable().get(mq);
                        if (offset != null) {
                            this.updateOffset(mq, offset.get(), false);
                            return offset.get();
                        }
                    }
                }
                default:
                    break;
            }
        }

        return -1;
    }


    /**
     * ���±��ش洢�ļ����ڴ���к���Ϣƫ������ӳ���ϵ
     */
    @Override
    public void persistAll(Set<MessageQueue> mqs) {
        if (null == mqs || mqs.isEmpty())
            return;

        OffsetSerializeWrapper offsetSerializeWrapper = new OffsetSerializeWrapper();
        for(Map.Entry<MessageQueue, AtomicLong> entry: this.offsetTable.entrySet()){
            if (mqs.contains(entry.getKey())) {
                AtomicLong offset = entry.getValue();
                offsetSerializeWrapper.getOffsetTable().put(entry.getKey(), offset);
            }
        }

        String jsonString = offsetSerializeWrapper.toJson(true);
        if (jsonString != null) {
            try {
                MixAll.string2File(jsonString, this.storePath);
            } catch (IOException e) {
                log.error("persistAll consumer offset Exception, " + this.storePath, e);
            }
        }
    }


    @Override
    public void persist(MessageQueue mq) {
    }

    @Override
    public void removeOffset(MessageQueue mq) {

    }

    @Override
    public Map<MessageQueue, Long> cloneOffsetTable(String topic) {
        Map<MessageQueue, Long> cloneOffsetTable = new HashMap<MessageQueue, Long>();
        for (Map.Entry<MessageQueue, AtomicLong> entry : this.offsetTable.entrySet()) {
            MessageQueue mq = entry.getKey();
            if (!UtilAll.isBlank(topic) && !topic.equals(mq.getTopic())) {
                continue;
            }
            cloneOffsetTable.put(mq,entry.getValue().get());

        }
        return cloneOffsetTable;
    }

    /**
     * ���ļ��ж�ȡ������Ϣ����ƫ����
     */
    private OffsetSerializeWrapper readLocalOffset() throws MQClientException 
    {
        String content = MixAll.file2String(this.storePath);
        if (null == content || content.length() == 0) {
            return this.readLocalOffsetBak();
        } else {
            OffsetSerializeWrapper offsetSerializeWrapper = null;
            try {
                offsetSerializeWrapper =
                        OffsetSerializeWrapper.fromJson(content, OffsetSerializeWrapper.class);
            } catch (Exception e) {
                log.warn("readLocalOffset Exception, and try to correct", e);
                return this.readLocalOffsetBak();
            }

            return offsetSerializeWrapper;
        }
    }

    /**
     * @����: �ӱ����ļ��ж�ȡ��Ϣ����ƫ���� 
     * @����: zgzhang@txbds.com
     * @����:2016��11��22��
     * @�޸�����
     * @������@return
     * @������@throws MQClientException     
     * @return OffsetSerializeWrapper   
     * @throws
     */
    private OffsetSerializeWrapper readLocalOffsetBak() throws MQClientException {
        String content = MixAll.file2String(this.storePath + ".bak");
        if (content != null && content.length() > 0) {
            OffsetSerializeWrapper offsetSerializeWrapper = null;
            try {
                offsetSerializeWrapper =
                        OffsetSerializeWrapper.fromJson(content, OffsetSerializeWrapper.class);
            } catch (Exception e) {
                log.warn("readLocalOffset Exception", e);
                throw new MQClientException("readLocalOffset Exception, maybe fastjson version too low" //
                        + FAQUrl.suggestTodo(FAQUrl.LOAD_JSON_EXCEPTION), //
                        e);
            }
            return offsetSerializeWrapper;
        }

        return null;
    }
}