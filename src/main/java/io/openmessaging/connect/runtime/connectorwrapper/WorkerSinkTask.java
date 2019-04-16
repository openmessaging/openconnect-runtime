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
import io.openmessaging.connector.api.data.Converter;
import io.openmessaging.connector.api.data.SinkDataEntry;
import io.openmessaging.connector.api.data.SourceDataEntry;
import io.openmessaging.connector.api.sink.SinkTask;
import io.openmessaging.connector.api.sink.SinkTaskContext;
import io.openmessaging.consumer.PullConsumer;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A wrapper of {@link SinkTask} for runtime.
 */
public class WorkerSinkTask implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(LoggerName.OMS_RUNTIME);

    /**
     * The configuration key that provides the list of topics that are inputs for this
     * SinkTask.
     */
    public static final String TOPICS_CONFIG = "topics";

    /**
     * The configuration key that provides a regex specifying which topics to include as inputs
     * for this SinkTask.
     */
    public static final String TOPICS_REGEX_CONFIG = "topics.regex";

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
                public void resetOffset(String queueName, Long offset) {

                }

                @Override
                public void resetOffset(Map<String, Long> offsets) {

                }

                @Override
                public void pause(List<String> queueName) {

                }

                @Override
                public void resume(List<String> queueName) {

                }

                @Override
                public KeyValue configs() {
                    return taskConfig;
                }
            });
            String topicsStr = taskConfig.getString(TOPICS_CONFIG);
            if (!StringUtils.isEmpty(topicsStr)) {
                String[] topics = topicsStr.split(",");
                for (String topic : topics) {
                    consumer.attachQueue(topic);
                }
                log.debug("{} Initializing and starting task for topics {}", this, topics);
            } else {
                // TODO 通过正则表达式订阅topic
                /* String topicsRegexStr = taskConfig.getString(TOPICS_REGEX_CONFIG);
                Pattern pattern = Pattern.compile(topicsRegexStr);
              consumer.attachQueue(pattern）
                log.debug("{} Initializing and starting task for topics regex {}", this, topicsRegexStr);*/
            }
            sinkTask.start(taskConfig);
            log.info("Task start, config: {}", JSON.toJSONString(taskConfig));

            while (!isStopping.get()) {
                final Message message = consumer.receive();
                if (null != message) {
                    receiveMessage(message);
                    String msgId = message.sysHeaders().getString(Message.BuiltinKeys.MESSAGE_ID);
                    log.info("Received one message: {}", msgId);
                    consumer.ack(msgId);
                }
            }
            log.info("Task stop, config:{}", JSON.toJSONString(taskConfig));
        } catch (Exception e) {
            log.error("Run task failed {}.", e);
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
     * @param message
     */
    private void receiveMessage(Message message) {
        String queueName = message.sysHeaders().getString("DESTINATION");
        final byte[] messageBody = message.getBody(byte[].class);
        final SourceDataEntry sourceDataEntry = JSON.parseObject(new String(messageBody), SourceDataEntry.class);
        final Object[] payload = sourceDataEntry.getPayload();
        final byte[] decodeBytes = Base64.getDecoder().decode((String) payload[0]);
        final Object recodeObject = recordConverter.byteToObject(decodeBytes);
        Object[] newObject = new Object[1];
        newObject[0] = recodeObject;
        //TODO queueOffset如何获得
        SinkDataEntry sinkDataEntry = new SinkDataEntry(10L, sourceDataEntry.getTimestamp(), sourceDataEntry.getEntryType(), queueName, sourceDataEntry.getSchema(), newObject);
        sinkDataEntry.setPayload(newObject);
        List<SinkDataEntry> sinkDataEntries = new ArrayList<>(8);
        sinkDataEntries.add(sinkDataEntry);
        sinkTask.put(sinkDataEntries);
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
}
