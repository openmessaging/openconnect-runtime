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

package io.openmessaging.connect.runtime.service;

import io.openmessaging.KeyValue;
import io.openmessaging.MessagingAccessPoint;
import io.openmessaging.connect.runtime.common.ConnAndTaskConfigs;
import io.openmessaging.connect.runtime.common.ConnectKeyValue;
import io.openmessaging.connect.runtime.config.ConnectConfig;
import io.openmessaging.connect.runtime.config.RuntimeConfigDefine;
import io.openmessaging.connect.runtime.converter.ConnAndTaskConfigConverter;
import io.openmessaging.connect.runtime.converter.JsonConverter;
import io.openmessaging.connect.runtime.converter.ListConverter;
import io.openmessaging.connect.runtime.store.FileBaseKeyValueStore;
import io.openmessaging.connect.runtime.store.KeyValueStore;
import io.openmessaging.connect.runtime.utils.FilePathConfigUtil;
import io.openmessaging.connect.runtime.utils.datasync.BrokerBasedLog;
import io.openmessaging.connect.runtime.utils.datasync.DataSynchronizer;
import io.openmessaging.connect.runtime.utils.datasync.DataSynchronizerCallback;
import io.openmessaging.connector.api.Connector;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ConfigManagementServiceImpl implements ConfigManagementService {

    /**
     * Default topic to send/consume config change message.
     */
    private static final String CONFIG_MESSAGE_TOPIC = "config-topic";

    /**
     * Current connector configs in the store.
     */
    private KeyValueStore<String, ConnectKeyValue> connectorKeyValueStore;

    /**
     * Current task configs in the store.
     */
    private KeyValueStore<String, List<ConnectKeyValue>> taskKeyValueStore;

    /**
     * All listeners to trigger while config change.
     */
    private Set<ConnectorConfigUpdateListener> connectorConfigUpdateListener;

    /**
     * Synchronize config with other workers.
     */
    private DataSynchronizer<String, ConnAndTaskConfigs> dataSynchronizer;

    public ConfigManagementServiceImpl(ConnectConfig connectConfig,
                                       MessagingAccessPoint messagingAccessPoint){

        this.connectorConfigUpdateListener = new HashSet<>();
        this.dataSynchronizer = new BrokerBasedLog<>(messagingAccessPoint,
                                                     CONFIG_MESSAGE_TOPIC,
                                                     connectConfig.getWorkerId()+System.currentTimeMillis(),
                                                     new ConfigManagementServiceImpl.ConfigChangeCallback(),
                                                     new JsonConverter(),
                                                     new ConnAndTaskConfigConverter());
        this.connectorKeyValueStore = new FileBaseKeyValueStore<>(
                                                     FilePathConfigUtil.getConnectorConfigPath(connectConfig.getStorePathRootDir()),
                                                     new JsonConverter(),
                                                     new JsonConverter(ConnectKeyValue.class));
        this.taskKeyValueStore = new FileBaseKeyValueStore<>(
                                                     FilePathConfigUtil.getTaskConfigPath(connectConfig.getStorePathRootDir()),
                                                     new JsonConverter(),
                                                     new ListConverter(ConnectKeyValue.class));
    }

    @Override
    public void start() {

        connectorKeyValueStore.load();
        taskKeyValueStore.load();
        dataSynchronizer.start();
        sendOnlineConfig();
    }

    @Override
    public void stop() {

        connectorKeyValueStore.persist();
        taskKeyValueStore.persist();
    }

    @Override
    public Map<String, ConnectKeyValue> getConnectorConfigs() {

        Map<String, ConnectKeyValue> result = new HashMap<>();
        Map<String, ConnectKeyValue> connectorConfigs = connectorKeyValueStore.getKVMap();
        for(String connectorName : connectorConfigs.keySet()){
            ConnectKeyValue config = connectorConfigs.get(connectorName);
            if(0 != config.getInt(RuntimeConfigDefine.CONFIG_DELETED)){
                continue;
            }
            result.put(connectorName, config);
        }
        return result;
    }

    @Override
    public String putConnectorConfig(String connectorName, ConnectKeyValue configs) throws Exception {

        ConnectKeyValue exist = connectorKeyValueStore.get(connectorName);
        if (null != exist) {
            Long updateTimestamp = exist.getLong(RuntimeConfigDefine.UPDATE_TIMESATMP);
            if (null != updateTimestamp) {
                configs.put(RuntimeConfigDefine.UPDATE_TIMESATMP, updateTimestamp);
            }
        }
        if(configs.equals(exist)){
            return "Connector with same config already exist.";
        }

        Long currentTimestamp = System.currentTimeMillis();
        configs.put(RuntimeConfigDefine.UPDATE_TIMESATMP, currentTimestamp);
        for(String requireConfig : RuntimeConfigDefine.REQUEST_CONFIG){
            if(!configs.containsKey(requireConfig)){
                return "Request config key: " + requireConfig;
            }
        }

        String className = configs.getString(RuntimeConfigDefine.CONNECTOR_CLASS);
        Class clazz = Class.forName(className);
        Connector connector = (Connector) clazz.newInstance();

        String errorMessage = connector.verifyAndSetConfig(configs);
        if(errorMessage != null && errorMessage.length() > 0){
            return errorMessage;
        }

        connector.start();
        connectorKeyValueStore.put(connectorName, configs);
        List<KeyValue> taskConfigs = connector.taskConfigs();
        List<ConnectKeyValue> converterdConfigs = new ArrayList<>();
        for(KeyValue keyValue : taskConfigs){
            ConnectKeyValue newKeyValue = new ConnectKeyValue();
            for(String key : keyValue.keySet()){
                newKeyValue.put(key, keyValue.getString(key));
            }
            newKeyValue.put(RuntimeConfigDefine.TASK_CLASS, connector.taskClass().getName());
            newKeyValue.put(RuntimeConfigDefine.RUNTIME_OMS_DRIVER_URL, configs.getString(RuntimeConfigDefine.RUNTIME_OMS_DRIVER_URL));
            newKeyValue.put(RuntimeConfigDefine.UPDATE_TIMESATMP, currentTimestamp);
            converterdConfigs.add(newKeyValue);
        }
        putTaskConfigs(connectorName, converterdConfigs);
        connector.stop();
        sendSynchronizeConfig();

        triggerListener();
        return "";
    }

    @Override
    public void removeConnectorConfig(String connectorName) {

        ConnectKeyValue config = new ConnectKeyValue();
        config.put(RuntimeConfigDefine.UPDATE_TIMESATMP, System.currentTimeMillis());
        config.put(RuntimeConfigDefine.CONFIG_DELETED, 1);
        Map<String, ConnectKeyValue> connectorConfig = new HashMap<>();
        connectorConfig.put(connectorName, config);
        List<ConnectKeyValue> taskConfigList = new ArrayList<>();
        taskConfigList.add(config);

        connectorKeyValueStore.put(connectorName, config);
        putTaskConfigs(connectorName, taskConfigList);
        sendSynchronizeConfig();
    }

    @Override
    public Map<String, List<ConnectKeyValue>> getTaskConfigs() {

        Map<String, List<ConnectKeyValue>> result = new HashMap<>();
        Map<String, List<ConnectKeyValue>> taskConfigs = taskKeyValueStore.getKVMap();
        Map<String, ConnectKeyValue> filteredConnector = getConnectorConfigs();
        for(String connectorName : taskConfigs.keySet()){
            if(!filteredConnector.containsKey(connectorName)){
                continue;
            }
            result.put(connectorName, taskConfigs.get(connectorName));
        }
        return result;
    }

    private void putTaskConfigs(String connectorName, List<ConnectKeyValue> configs) {

        List<ConnectKeyValue> exist = taskKeyValueStore.get(connectorName);
        if(null != exist && exist.size() > 0){
            taskKeyValueStore.remove(connectorName);
        }
        taskKeyValueStore.put(connectorName, configs);
    }

    @Override
    public void persist() {

        this.connectorKeyValueStore.persist();
        this.taskKeyValueStore.persist();
    }

    @Override
    public void registerListener(ConnectorConfigUpdateListener listener) {

        this.connectorConfigUpdateListener.add(listener);
    }

    private void triggerListener(){

        if(null == this.connectorConfigUpdateListener){
            return;
        }
        for(ConnectorConfigUpdateListener listener : this.connectorConfigUpdateListener){
            listener.onConfigUpdate();
        }
    }

    private void sendOnlineConfig(){

        ConnAndTaskConfigs configs = new ConnAndTaskConfigs();
        configs.setConnectorConfigs(connectorKeyValueStore.getKVMap());
        configs.setTaskConfigs(taskKeyValueStore.getKVMap());
        dataSynchronizer.send(ConfigChangeEnum.ONLINE_KEY.name(), configs);
    }

    private void sendSynchronizeConfig(){

        ConnAndTaskConfigs configs = new ConnAndTaskConfigs();
        configs.setConnectorConfigs(connectorKeyValueStore.getKVMap());
        configs.setTaskConfigs(taskKeyValueStore.getKVMap());
        dataSynchronizer.send(ConfigChangeEnum.CONFIG_CHANG_KEY.name(), configs);
    }

    private class ConfigChangeCallback implements DataSynchronizerCallback<String, ConnAndTaskConfigs> {

        @Override
        public void onCompletion(Throwable error, String key, ConnAndTaskConfigs result) {

            boolean changed = false;
            switch (ConfigChangeEnum.valueOf(key)){
                case ONLINE_KEY:
                    mergeConfig(result);
                    changed = true;
                    sendSynchronizeConfig();
                    break;
                case CONFIG_CHANG_KEY:
                    changed = mergeConfig(result);
                    break;
                default:
                    break;
            }
            if(changed){
                triggerListener();
            }
        }
    }

    /**
     * Merge new received configs with the configs in memory.
     * @param newConnAndTaskConfig
     * @return
     */
    private boolean mergeConfig(ConnAndTaskConfigs newConnAndTaskConfig) {

        boolean changed = false;
        for(String connectorName : newConnAndTaskConfig.getConnectorConfigs().keySet()){
            ConnectKeyValue newConfig = newConnAndTaskConfig.getConnectorConfigs().get(connectorName);
            ConnectKeyValue oldConfig = getConnectorConfigs().get(connectorName);
            if(null == oldConfig){

                changed = true;
                connectorKeyValueStore.put(connectorName, newConfig);
                taskKeyValueStore.put(connectorName, newConnAndTaskConfig.getTaskConfigs().get(connectorName));
            }else{

                Long oldUpdateTime = oldConfig.getLong(RuntimeConfigDefine.UPDATE_TIMESATMP);
                Long newUpdateTime = newConfig.getLong(RuntimeConfigDefine.UPDATE_TIMESATMP);
                if(newUpdateTime > oldUpdateTime){
                    changed = true;
                    connectorKeyValueStore.put(connectorName, newConfig);
                    taskKeyValueStore.put(connectorName, newConnAndTaskConfig.getTaskConfigs().get(connectorName));
                }
            }
        }
        return changed;
    }

    private enum ConfigChangeEnum{

        /**
         * Insert or update config.
         */
        CONFIG_CHANG_KEY,

        /**
         * A worker online.
         */
        ONLINE_KEY
    }
}
