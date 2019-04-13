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

package io.openmessaging.connect.runtime.rest;

import com.alibaba.fastjson.JSON;
import io.javalin.Context;
import io.javalin.Javalin;
import io.openmessaging.connect.runtime.RuntimeController;
import io.openmessaging.connect.runtime.common.ConnectKeyValue;
import io.openmessaging.connect.runtime.common.LoggerName;
import io.openmessaging.connect.runtime.connectorwrapper.WorkerConnector;
import io.openmessaging.connect.runtime.connectorwrapper.WorkerSourceTask;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A rest handler to process http request.
 */
public class RestHandler {

    private static final Logger log = LoggerFactory.getLogger(LoggerName.OMS_RUNTIME);

    private final RuntimeController runtimeController;

    public RestHandler(RuntimeController runtimeController){
        this.runtimeController = runtimeController;
        Javalin app = Javalin.start(runtimeController.getConnectConfig().getHttpPort());
        app.post("/connectors/:connectorName", this::handleCreateConnector);
        app.get("/connectors/:connectorName/config", this::handleQueryConnectorConfig);
        app.get("/connectors/:connectorName/status", this::handleQueryConnectorStatus);
        app.delete("/connectors/:connectorName/stop", this::handleStopConnector);
        app.get("/getClusterInfo", this::getClusterInfo);
        app.get("/getConfigInfo", this::getConfigInfo);
        app.get("/getAllocatedInfo", this::getAllocatedInfo);
    }

    private void getAllocatedInfo(Context context){

        Set<WorkerConnector> workerConnectors = runtimeController.getWorker().getWorkingConnectors();
        Set<WorkerSourceTask> workerSourceTasks = runtimeController.getWorker().getWorkingTasks();
        StringBuilder sb = new StringBuilder();
        sb.append("working connectors:\n");
        for(WorkerConnector workerConnector : workerConnectors){
            sb.append(workerConnector.toString()+"\n");
        }
        sb.append("working tasks:\n");
        for(WorkerSourceTask workerSourceTask : workerSourceTasks){
            sb.append(workerSourceTask.toString()+"\n");
        }
        context.result(sb.toString());
    }

    private void getConfigInfo(Context context) {

        Map<String, ConnectKeyValue> connectorConfigs = runtimeController.getConfigManagementService().getConnectorConfigs();
        Map<String, List<ConnectKeyValue>> taskConfigs = runtimeController.getConfigManagementService().getTaskConfigs();
        context.result("ConnectorConfigs:"+JSON.toJSONString(connectorConfigs)+"\nTaskConfigs:"+JSON.toJSONString(taskConfigs));
    }

    private void getClusterInfo(Context context) {
        context.result(JSON.toJSONString(runtimeController.getClusterManagementService().getAllAliveWorkers()));
    }

    private void handleCreateConnector(Context context) {
        String connectorName = context.param("connectorName");
        String arg = context.queryParam("config");
        Map keyValue = JSON.parseObject(arg, Map.class);
        ConnectKeyValue configs = new ConnectKeyValue();
        for(Object key : keyValue.keySet()){
            configs.put((String)key, (String)keyValue.get(key));
        }
        try {

            String result = runtimeController.getConfigManagementService().putConnectorConfig(connectorName, configs);
            if(result != null && result.length() > 0){
                context.result(result);
            }else{
                context.result("success");
            }
        } catch (Exception e) {
            log.error("oms connect runtime create the connector exception, ", e);
            context.result("failed");
        }
    }

    private void handleQueryConnectorConfig(Context context){

        String connectorName = context.param("connectorName");

        Map<String, ConnectKeyValue> connectorConfigs = runtimeController.getConfigManagementService().getConnectorConfigs();
        Map<String, List<ConnectKeyValue>> taskConfigs = runtimeController.getConfigManagementService().getTaskConfigs();
        StringBuilder sb = new StringBuilder();
        sb.append("ConnectorConfigs:")
            .append(JSON.toJSONString(connectorConfigs.get(connectorName)))
            .append("\n")
            .append("TaskConfigs:")
            .append(JSON.toJSONString(taskConfigs.get(connectorName)));
        context.result(sb.toString());
    }

    private void handleQueryConnectorStatus(Context context){

        String connectorName = context.param("connectorName");
        Map<String, ConnectKeyValue> connectorConfigs = runtimeController.getConfigManagementService().getConnectorConfigs();

        if(connectorConfigs.containsKey(connectorName)){
            context.result("running");
        }else{
            context.result("not running");
        }
    }

    private void handleStopConnector(Context context){
        String connectorName = context.param("connectorName");
        try {

            runtimeController.getConfigManagementService().removeConnectorConfig(connectorName);
            context.result("success");
        } catch (Exception e) {
            log.error("oms connect runtime stop the connector exception, ", e);
            context.result("failed");
        }
    }
}
