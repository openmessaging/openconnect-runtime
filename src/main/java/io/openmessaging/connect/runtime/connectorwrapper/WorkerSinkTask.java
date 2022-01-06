/*
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

package io.openmessaging.connect.runtime.connectorwrapper;

import com.alibaba.fastjson.JSON;
import io.openmessaging.KeyValue;
import io.openmessaging.Message;
import io.openmessaging.connect.runtime.common.ConnectKeyValue;
import io.openmessaging.connect.runtime.common.LoggerName;
import io.openmessaging.connect.runtime.common.QueuePartition;
import io.openmessaging.connector.api.common.QueueMetaData;
import io.openmessaging.connector.api.data.Converter;
import io.openmessaging.connector.api.data.SinkDataEntry;
import io.openmessaging.connector.api.data.SourceDataEntry;
import io.openmessaging.connector.api.sink.SinkTask;
import io.openmessaging.connector.api.sink.SinkTaskContext;
import io.openmessaging.consumer.PullConsumer;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A wrapper of {@link SinkTask} for runtime.
 */
public class WorkerSinkTask implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(LoggerName.OMS_RUNTIME);

    /**
     * The configuration key that provides the list of queueNames that are inputs for this
     * SinkTask.
     */
    public static final String QUEUENAMES_CONFIG = "queueNames";

    /**
     * Connector name of current task.
     */
    private String connectorName;

    /**
     * The implements of the sink task.
     */
    private SinkTask sinkTask;

    /**
     * The configs of current sink task.
     */
    private ConnectKeyValue taskConfig;

    /**
     * A switch for the sink task.
     */
    private AtomicBoolean isStopping;

    /**
     * A OMS consumer to pull message from MQ.
     */
    private PullConsumer consumer;

    /**
     * A converter to parse sink data entry to object.
     */
    private Converter recordConverter;

    public ConcurrentHashMap<QueuePartition, PartitionStatus> partitionStatusMap;

    public ConcurrentHashMap<QueuePartition, Long> partitionOffsetMap;

    public WorkerSinkTask(String connectorName,
                          SinkTask sinkTask,
                          ConnectKeyValue taskConfig,
                          Converter recordConverter,
                          PullConsumer consumer) {
        this.connectorName = connectorName;
        this.sinkTask = sinkTask;
        this.taskConfig = taskConfig;
        this.isStopping = new AtomicBoolean(false);
        this.consumer = consumer;
        this.recordConverter = recordConverter;
    }

    /**
     * Start a sink task, and receive data entry from MQ cyclically.
     */
    @Override
    public void run() {
        try {
            sinkTask.initialize(new SinkTaskContext() {
                @Override
                public void resetOffset(QueueMetaData queueMetaData, Long offset) {
                    QueuePartition queuePartition = new QueuePartition(queueMetaData.getQueueName(), 0);
                    partitionOffsetMap.put(queuePartition, offset);
                }

                @Override
                public void resetOffset(Map<QueueMetaData, Long> offsets) {
                    for (Map.Entry<QueueMetaData, Long> entry : offsets.entrySet()) {
                        QueuePartition queuePartition = new QueuePartition(entry.getKey().getQueueName(), 0);
                        partitionOffsetMap.put(queuePartition, entry.getValue());
                    }
                }

                @Override
                public void pause(List<QueueMetaData> queueMetaDataList) {
                    if (null != queueMetaDataList && queueMetaDataList.size() > 0) {
                        for (QueueMetaData queueMetaData : queueMetaDataList) {
                            QueuePartition queuePartition = new QueuePartition(queueMetaData.getQueueName(), 0);
                            partitionStatusMap.put(queuePartition, PartitionStatus.PAUSE);
                        }
                    }
                }

                @Override
                public void resume(List<QueueMetaData> queueMetaDataList) {
                    if (null != queueMetaDataList && queueMetaDataList.size() > 0) {
                        for (QueueMetaData queueMetaData : queueMetaDataList) {
                            QueuePartition queuePartition = new QueuePartition(queueMetaData.getQueueName(), 0);
                            partitionStatusMap.remove(queuePartition);
                        }
                    }
                }

                @Override
                public KeyValue configs() {
                    return taskConfig;
                }
            });
            String queueNamesStr = taskConfig.getString(QUEUENAMES_CONFIG);
            if (!StringUtils.isEmpty(queueNamesStr)) {
                String[] queueNames = queueNamesStr.split(",");
                for (String queueName : queueNames) {
                    consumer.attachQueue(queueName);
                }
                log.debug("{} Initializing and starting task for queueNames {}", this, queueNames);
            } else {
                log.error("Lack of sink comsume queueNames config");
            }
            sinkTask.start(taskConfig);

            while (!isStopping.get()) {
                final Message message = consumer.receive();
                List<Message> messages = new ArrayList<>(16);
                messages.add(message);
                checkPause(messages);
                receiveMessages(messages);
                for (Message message1 : messages) {
                    String msgId = message1.sysHeaders().getString(Message.BuiltinKeys.MESSAGE_ID);
                    log.info("Received one message success : {}", msgId);
                    //TODO 更新queue offset
//                    partitionOffsetMap.put()
                    consumer.ack(msgId);
                }
            }
            log.info("Task stop, config:{}", JSON.toJSONString(taskConfig));
        } catch (Exception e) {
            log.error("Run task failed {}.", e);
        }
    }

    private void checkPause(List<Message> messages) {
        final Iterator<Message> iterator = messages.iterator();
        while (iterator.hasNext()) {
            final Message message = iterator.next();
            String queueName = message.sysHeaders().getString("DESTINATION");
            //TODO 缺失partition
            QueuePartition queuePartition = new QueuePartition(queueName, 0);
            if (null != partitionStatusMap.get(queuePartition)) {
                String msgId = message.sysHeaders().getString(Message.BuiltinKeys.MESSAGE_ID);
                log.info("QueueName {}, partition {} pause, Discard the message {}", queueName, 0, msgId);
                iterator.remove();
            }
        }
    }

    public void stop() {
        isStopping.set(true);
        consumer.shutdown();
        sinkTask.stop();
    }

    /**
     * receive message from MQ.
     *
     * @param messages
     */
    private void receiveMessages(List<Message> messages) {
        List<SinkDataEntry> sinkDataEntries = new ArrayList<>(8);
        for (Message message : messages) {
            SinkDataEntry sinkDataEntry = convertToSinkDataEntry(message);
            sinkDataEntries.add(sinkDataEntry);
        }
        sinkTask.put(sinkDataEntries);
    }

    private SinkDataEntry convertToSinkDataEntry(Message message) {
        String queueName = message.sysHeaders().getString("DESTINATION");
        final byte[] messageBody = message.getBody(byte[].class);
        final SourceDataEntry sourceDataEntry = JSON.parseObject(new String(messageBody), SourceDataEntry.class);
        final Object[] payload = sourceDataEntry.getPayload();
        final byte[] decodeBytes = Base64.getDecoder().decode((String) payload[0]);
        final Object recodeObject = recordConverter.byteToObject(decodeBytes);
        Object[] newObject = new Object[1];
        newObject[0] = recodeObject;
        //TODO oms-1.0.0-alpha支持获取offset，并且支持批量获取消息,SinkDataEntry & SourceDataEntry 是否要增加partition相关属性
        SinkDataEntry sinkDataEntry = new SinkDataEntry(10L, sourceDataEntry.getTimestamp(), sourceDataEntry.getEntryType(), queueName, sourceDataEntry.getSchema(), newObject);
        sinkDataEntry.setPayload(newObject);
        return sinkDataEntry;
    }

    public String getConnectorName() {
        return connectorName;
    }

    public ConnectKeyValue getTaskConfig() {
        return taskConfig;
    }

    @Override
    public String toString() {

        StringBuilder sb = new StringBuilder();
        sb.append("connectorName:" + connectorName)
                .append("\nConfigs:" + JSON.toJSONString(taskConfig));
        return sb.toString();
    }

    private enum PartitionStatus {
        PAUSE;
    }
}
