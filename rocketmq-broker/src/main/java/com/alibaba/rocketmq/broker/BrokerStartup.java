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
 * broker启动入库
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
     * @描述: 创建brokerController对象 
     * @作者: zgzhang@txbds.com
     * @日期:2016年11月24日
     * @修改内容
     * @参数：@param args
     * @参数：@return     
     * @throws
     */
    public static BrokerController createBrokerController(String[] args) 
    {
    	/**
    	 * 设置rocketmq的版本号
    	 */
        System.setProperty(RemotingCommand.RemotingVersionKey, Integer.toString(MQVersion.CurrentVersion));

        /**
         * 没有配置netty服务端的发送缓冲区大小, 设定默认的大小 为131072byte   128K
         */
        if (null == System.getProperty(NettySystemConfig.SystemPropertySocketSndbufSize)) {
            NettySystemConfig.socketSndbufSize = 131072;
        }

        /**
         * 没有配置netty服务端的接受缓冲区大小, 设定默认的大小 为131072byte   128K
         */
        if (null == System.getProperty(NettySystemConfig.SystemPropertySocketRcvbufSize)) {
            NettySystemConfig.socketRcvbufSize = 131072;
        }

        try 
        {
            //PackageConflictDetect.detectFastjson();
        	/**
        	 * 构建命令行解析对象
        	 */
            Options options = ServerUtil.buildCommandlineOptions(new Options());
            /**
             * 解析命令行参数
             */
            commandLine =
                    ServerUtil.parseCmdLine("mqbroker", args, buildCommandlineOptions(options), new PosixParser());
            
            /**
             * 解析失败 系统退出
             */
            if (null == commandLine) {
                System.exit(-1);
                return null;
            }

            /**
             * broker的配置文件
             */
            final BrokerConfig brokerConfig = new BrokerConfig();
            
            /**
             * netty的服务端配置文件
             */
            final NettyServerConfig nettyServerConfig = new NettyServerConfig();
            
            /**
             * netty客户端配置文件
             */
            final NettyClientConfig nettyClientConfig = new NettyClientConfig();
            
            /**
             * broker服务端服务端口
             * 默认为10911
             * 可以在配置文件中设置listenPort修改
             */
            nettyServerConfig.setListenPort(10911);
            
            /**
             * broker端消息存储配置文件
             * 主要对数据存储目录、文件大小、持久事件、数据落盘方式等进行配置
             */
            final MessageStoreConfig messageStoreConfig = new MessageStoreConfig();

            /**
             * 如果当前启动的broker为slave的话
             * 设定消息占用内存的比率  默认master的话为40%
             * salve为30%
             * 
             * 可以通过配置文件修改占用率
             */
            if (BrokerRole.SLAVE == messageStoreConfig.getBrokerRole()) {
                int ratio = messageStoreConfig.getAccessMessageInMemoryMaxRatio() - 10;
                messageStoreConfig.setAccessMessageInMemoryMaxRatio(ratio);
            }

            /**
             * 如果有p选项
             * 需要打印配置文件选项
             */
            if (commandLine.hasOption('p')) {
                MixAll.printObjectProperties(null, brokerConfig);
                MixAll.printObjectProperties(null, nettyServerConfig);
                MixAll.printObjectProperties(null, nettyClientConfig);
                MixAll.printObjectProperties(null, messageStoreConfig);
                System.exit(0);
            }
            /**
             * 打印带有ImportantField注解的配置
             */
            else if (commandLine.hasOption('m')) {
                MixAll.printObjectProperties(null, brokerConfig, true);
                MixAll.printObjectProperties(null, nettyServerConfig, true);
                MixAll.printObjectProperties(null, nettyClientConfig, true);
                MixAll.printObjectProperties(null, messageStoreConfig, true);
                System.exit(0);
            }

            /**
             * 如果指定了broker的配置文件
             * 需要读取配置文件来设定broker的行为方式
             */
            if (commandLine.hasOption('c')) {
                String file = commandLine.getOptionValue('c');
                if (file != null) {
                    configFile = file;
                    InputStream in = new BufferedInputStream(new FileInputStream(file));
                    properties = new Properties();
                    properties.load(in);

                    /**
                     * 解析环境变量中配置的更新namesrv地址的域名和服务名
                     */
                    parsePropertie2SystemEnv(properties);
                    /**
                     * 更新broker的配置
                     */
                    MixAll.properties2Object(properties, brokerConfig);
                    /**
                     * 更新netty服务端配置
                     */
                    MixAll.properties2Object(properties, nettyServerConfig);
                    /**
                     * 更新netty客户端的配置
                     */
                    MixAll.properties2Object(properties, nettyClientConfig);
                    /**
                     * 更新消息存储的配置
                     */
                    MixAll.properties2Object(properties, messageStoreConfig);

                    /**
                     * 更新配置文件路径
                     */
                    BrokerPathConfigHelper.setBrokerConfigPath(file);

                    System.out.println("load config properties file OK, " + file);
                    in.close();
                }
            }

            /**
             * 命令行参数更新配置文件
             */
            MixAll.properties2Object(ServerUtil.commandLine2Properties(commandLine), brokerConfig);

            /**
             * 如果rocketmq的主目录没有设定
             * 系统退出
             */
            if (null == brokerConfig.getRocketmqHome()) 
            {
                System.out.println("Please set the " + MixAll.ROCKETMQ_HOME_ENV
                        + " variable in your environment to match the location of the RocketMQ installation");
                System.exit(-2);
            }

            /**
             * 如果配置文件或者命令参数设定namesrv地址
             * 配置文件中通过nameservAddr配置
             * 命令行参数通过-n配置
             * 校验namesrv地址
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
             * 根据配置的broker角色设定brokerid值
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

            //设置ha监听端口 listenPort + 1
            /**
             * 影响haservice中AcceptSocketService服务的端口号
             * 参考：
             * com.alibaba.rocketmq.store.ha.HAService 构造函数中
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
     * @描述: 根据配置文件来设定环境变量的值 
     * @作者: zgzhang@txbds.com
     * @日期:2016年11月24日
     * @修改内容
     * @参数：@param properties     
     * @throws
     */
    private static void parsePropertie2SystemEnv(Properties properties){
        if(properties ==null){
            return;
        }
        
        /**
         * 更新rocketmq的namesrv地址的域名
         * 默认为jmenv.tbsite.net
         */
        String rmqAddressServerDomain = properties.getProperty("rmqAddressServerDomain","jmenv.tbsite.net");
        
        /**
         * rocketmq namesrv 的子组
         * 这样可以在一个rmqAddressServerDomain上支持多个服务集群
         * 因为获取namesrv是通过 rmqAddressServerDomain + "-" + this.unitName + "?nofix=1"
         * 来获取的
         */
        String rmqAddressServerSubGroup = properties.getProperty("rmqAddressServerSubGroup", "nsaddr");
        
        /**
         * 设定环境变量的值
         */
        System.setProperty("rocketmq.namesrv.domain",rmqAddressServerDomain);
        
        /**
         * 设定环境变量的值
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
