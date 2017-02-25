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
package com.alibaba.rocketmq.broker;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.joran.JoranConfigurator;
import com.alibaba.rocketmq.common.BrokerConfig;
import com.alibaba.rocketmq.common.MQVersion;
import com.alibaba.rocketmq.common.MixAll;
import com.alibaba.rocketmq.common.constant.LoggerName;
import com.alibaba.rocketmq.remoting.common.RemotingUtil;
import com.alibaba.rocketmq.remoting.netty.NettyClientConfig;
import com.alibaba.rocketmq.remoting.netty.NettyServerConfig;
import com.alibaba.rocketmq.remoting.netty.NettySystemConfig;
import com.alibaba.rocketmq.remoting.protocol.RemotingCommand;
import com.alibaba.rocketmq.srvutil.ServerUtil;
import com.alibaba.rocketmq.store.config.BrokerRole;
import com.alibaba.rocketmq.store.config.MessageStoreConfig;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;


/**
 * broker�������
 * @author shijia.wxr
 */
public class BrokerStartup {
    public static Properties properties = null;
    public static CommandLine commandLine = null;
    public static String configFile = null;
    public static Logger log;

    public static void main(String[] args) {
        start(createBrokerController(args));
    }

    public static BrokerController start(BrokerController controller) {
        try {

            controller.start();
            String tip =
                    "The broker[" + controller.getBrokerConfig().getBrokerName() + ", "
                            + controller.getBrokerAddr() + "] boot success. serializeType=" + RemotingCommand.getSerializeTypeConfigInThisServer();

            if (null != controller.getBrokerConfig().getNamesrvAddr()) {
                tip += " and name server is " + controller.getBrokerConfig().getNamesrvAddr();
            }

            log.info(tip);
            System.out.println(tip);

            return controller;
        } catch (Throwable e) {
            e.printStackTrace();
            System.exit(-1);
        }

        return null;
    }

    /**
     * @����: ����brokerController���� 
     * @����: zgzhang@txbds.com
     * @����:2016��11��24��
     * @�޸�����
     * @������@param args
     * @������@return     
     * @throws
     */
    public static BrokerController createBrokerController(String[] args) 
    {
    	/**
    	 * ����rocketmq�İ汾��
    	 */
        System.setProperty(RemotingCommand.RemotingVersionKey, Integer.toString(MQVersion.CurrentVersion));

        /**
         * û������netty����˵ķ��ͻ�������С, �趨Ĭ�ϵĴ�С Ϊ131072byte   128K
         */
        if (null == System.getProperty(NettySystemConfig.SystemPropertySocketSndbufSize)) {
            NettySystemConfig.socketSndbufSize = 131072;
        }

        /**
         * û������netty����˵Ľ��ܻ�������С, �趨Ĭ�ϵĴ�С Ϊ131072byte   128K
         */
        if (null == System.getProperty(NettySystemConfig.SystemPropertySocketRcvbufSize)) {
            NettySystemConfig.socketRcvbufSize = 131072;
        }

        try 
        {
            //PackageConflictDetect.detectFastjson();
        	/**
        	 * ���������н�������
        	 */
            Options options = ServerUtil.buildCommandlineOptions(new Options());
            /**
             * ���������в���
             */
            commandLine =
                    ServerUtil.parseCmdLine("mqbroker", args, buildCommandlineOptions(options), new PosixParser());
            
            /**
             * ����ʧ�� ϵͳ�˳�
             */
            if (null == commandLine) {
                System.exit(-1);
                return null;
            }

            /**
             * broker�������ļ�
             */
            final BrokerConfig brokerConfig = new BrokerConfig();
            
            /**
             * netty�ķ���������ļ�
             */
            final NettyServerConfig nettyServerConfig = new NettyServerConfig();
            
            /**
             * netty�ͻ��������ļ�
             */
            final NettyClientConfig nettyClientConfig = new NettyClientConfig();
            
            /**
             * broker����˷���˿�
             * Ĭ��Ϊ10911
             * �����������ļ�������listenPort�޸�
             */
            nettyServerConfig.setListenPort(10911);
            
            /**
             * broker����Ϣ�洢�����ļ�
             * ��Ҫ�����ݴ洢Ŀ¼���ļ���С���־��¼����������̷�ʽ�Ƚ�������
             */
            final MessageStoreConfig messageStoreConfig = new MessageStoreConfig();

            /**
             * �����ǰ������brokerΪslave�Ļ�
             * �趨��Ϣռ���ڴ�ı���  Ĭ��master�Ļ�Ϊ40%
             * salveΪ30%
             * 
             * ����ͨ�������ļ��޸�ռ����
             */
            if (BrokerRole.SLAVE == messageStoreConfig.getBrokerRole()) {
                int ratio = messageStoreConfig.getAccessMessageInMemoryMaxRatio() - 10;
                messageStoreConfig.setAccessMessageInMemoryMaxRatio(ratio);
            }

            /**
             * �����pѡ��
             * ��Ҫ��ӡ�����ļ�ѡ��
             */
            if (commandLine.hasOption('p')) {
                MixAll.printObjectProperties(null, brokerConfig);
                MixAll.printObjectProperties(null, nettyServerConfig);
                MixAll.printObjectProperties(null, nettyClientConfig);
                MixAll.printObjectProperties(null, messageStoreConfig);
                System.exit(0);
            }
            /**
             * ��ӡ����ImportantFieldע�������
             */
            else if (commandLine.hasOption('m')) {
                MixAll.printObjectProperties(null, brokerConfig, true);
                MixAll.printObjectProperties(null, nettyServerConfig, true);
                MixAll.printObjectProperties(null, nettyClientConfig, true);
                MixAll.printObjectProperties(null, messageStoreConfig, true);
                System.exit(0);
            }

            /**
             * ���ָ����broker�������ļ�
             * ��Ҫ��ȡ�����ļ����趨broker����Ϊ��ʽ
             */
            if (commandLine.hasOption('c')) {
                String file = commandLine.getOptionValue('c');
                if (file != null) {
                    configFile = file;
                    InputStream in = new BufferedInputStream(new FileInputStream(file));
                    properties = new Properties();
                    properties.load(in);

                    /**
                     * �����������������õĸ���namesrv��ַ�������ͷ�����
                     */
                    parsePropertie2SystemEnv(properties);
                    /**
                     * ����broker������
                     */
                    MixAll.properties2Object(properties, brokerConfig);
                    /**
                     * ����netty���������
                     */
                    MixAll.properties2Object(properties, nettyServerConfig);
                    /**
                     * ����netty�ͻ��˵�����
                     */
                    MixAll.properties2Object(properties, nettyClientConfig);
                    /**
                     * ������Ϣ�洢������
                     */
                    MixAll.properties2Object(properties, messageStoreConfig);

                    /**
                     * ���������ļ�·��
                     */
                    BrokerPathConfigHelper.setBrokerConfigPath(file);

                    System.out.println("load config properties file OK, " + file);
                    in.close();
                }
            }

            /**
             * �����в������������ļ�
             */
            MixAll.properties2Object(ServerUtil.commandLine2Properties(commandLine), brokerConfig);

            /**
             * ���rocketmq����Ŀ¼û���趨
             * ϵͳ�˳�
             */
            if (null == brokerConfig.getRocketmqHome()) 
            {
                System.out.println("Please set the " + MixAll.ROCKETMQ_HOME_ENV
                        + " variable in your environment to match the location of the RocketMQ installation");
                System.exit(-2);
            }

            /**
             * ��������ļ�������������趨namesrv��ַ
             * �����ļ���ͨ��nameservAddr����
             * �����в���ͨ��-n����
             * У��namesrv��ַ
             */
            String namesrvAddr = brokerConfig.getNamesrvAddr();
            if (null != namesrvAddr) {
                try {
                    String[] addrArray = namesrvAddr.split(";");
                    if (addrArray != null) {
                        for (String addr : addrArray) {
                            RemotingUtil.string2SocketAddress(addr);
                        }
                    }
                } catch (Exception e) {
                    System.out
                            .printf(
                                    "The Name Server Address[%s] illegal, please set it as follows, \"127.0.0.1:9876;192.168.0.1:9876\"%n",
                                    namesrvAddr);
                    System.exit(-3);
                }
            }

            /**
             * �������õ�broker��ɫ�趨brokeridֵ
             */
            switch (messageStoreConfig.getBrokerRole()) {
                case ASYNC_MASTER:
                case SYNC_MASTER:
                    brokerConfig.setBrokerId(MixAll.MASTER_ID);
                    break;
                case SLAVE:
                    if (brokerConfig.getBrokerId() <= 0) {
                        System.out.println("Slave's brokerId must be > 0");
                        System.exit(-3);
                    }

                    break;
                default:
                    break;
            }

            //����ha�����˿� listenPort + 1
            /**
             * Ӱ��haservice��AcceptSocketService����Ķ˿ں�
             * �ο���
             * com.alibaba.rocketmq.store.ha.HAService ���캯����
             *     this.acceptSocketService =
                   new AcceptSocketService(defaultMessageStore.getMessageStoreConfig().getHaListenPort());
             */
            messageStoreConfig.setHaListenPort(nettyServerConfig.getListenPort() + 1);

            LoggerContext lc = (LoggerContext) LoggerFactory.getILoggerFactory();
            JoranConfigurator configurator = new JoranConfigurator();
            configurator.setContext(lc);
            lc.reset();
            configurator.doConfigure(brokerConfig.getRocketmqHome() + "/conf/logback_broker.xml");
            log = LoggerFactory.getLogger(LoggerName.BrokerLoggerName);


            MixAll.printObjectProperties(log, brokerConfig);
            MixAll.printObjectProperties(log, nettyServerConfig);
            MixAll.printObjectProperties(log, nettyClientConfig);
            MixAll.printObjectProperties(log, messageStoreConfig);


            final BrokerController controller = new BrokerController(//
                    brokerConfig, //
                    nettyServerConfig, //
                    nettyClientConfig, //
                    messageStoreConfig);
            boolean initResult = controller.initialize();
            if (!initResult) {
                controller.shutdown();
                System.exit(-3);
            }

            Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
                private volatile boolean hasShutdown = false;
                private AtomicInteger shutdownTimes = new AtomicInteger(0);


                @Override
                public void run() {
                    synchronized (this) {
                        log.info("shutdown hook was invoked, " + this.shutdownTimes.incrementAndGet());
                        if (!this.hasShutdown) {
                            this.hasShutdown = true;
                            long begineTime = System.currentTimeMillis();
                            controller.shutdown();
                            long consumingTimeTotal = System.currentTimeMillis() - begineTime;
                            log.info("shutdown hook over, consuming time total(ms): " + consumingTimeTotal);
                        }
                    }
                }
            }, "ShutdownHook"));

            return controller;
        } catch (Throwable e) {
            e.printStackTrace();
            System.exit(-1);
        }

        return null;
    }

    /**
     * @����: ���������ļ����趨����������ֵ 
     * @����: zgzhang@txbds.com
     * @����:2016��11��24��
     * @�޸�����
     * @������@param properties     
     * @throws
     */
    private static void parsePropertie2SystemEnv(Properties properties){
        if(properties ==null){
            return;
        }
        
        /**
         * ����rocketmq��namesrv��ַ������
         * Ĭ��Ϊjmenv.tbsite.net
         */
        String rmqAddressServerDomain = properties.getProperty("rmqAddressServerDomain","jmenv.tbsite.net");
        
        /**
         * rocketmq namesrv ������
         * ����������һ��rmqAddressServerDomain��֧�ֶ������Ⱥ
         * ��Ϊ��ȡnamesrv��ͨ�� rmqAddressServerDomain + "-" + this.unitName + "?nofix=1"
         * ����ȡ��
         */
        String rmqAddressServerSubGroup = properties.getProperty("rmqAddressServerSubGroup", "nsaddr");
        
        /**
         * �趨����������ֵ
         */
        System.setProperty("rocketmq.namesrv.domain",rmqAddressServerDomain);
        
        /**
         * �趨����������ֵ
         */
        System.setProperty("rocketmq.namesrv.domain.subgroup",rmqAddressServerSubGroup);
    }

    public static Options buildCommandlineOptions(final Options options) {
        Option opt = new Option("c", "configFile", true, "Broker config properties file");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("p", "printConfigItem", false, "Print all config item");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("m", "printImportantConfig", false, "Print important config item");
        opt.setRequired(false);
        options.addOption(opt);

        return options;
    }
}
