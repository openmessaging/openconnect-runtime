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
import com.alibaba.fastjson.JSONObject;
import io.openmessaging.connect.runtime.ConnectController;
import io.openmessaging.connect.runtime.common.ConnectKeyValue;
import io.openmessaging.connect.runtime.config.ConnectConfig;
import io.openmessaging.connect.runtime.config.RuntimeConfigDefine;
import io.openmessaging.connect.runtime.connectorwrapper.Worker;
import io.openmessaging.connect.runtime.connectorwrapper.WorkerConnector;
import io.openmessaging.connect.runtime.connectorwrapper.WorkerSourceTask;
import io.openmessaging.connect.runtime.service.ClusterManagementService;
import io.openmessaging.connect.runtime.service.ConfigManagementService;
import io.openmessaging.connect.runtime.service.PositionManagementService;
import io.openmessaging.connector.api.Connector;
import io.openmessaging.connector.api.PositionStorageReader;
import io.openmessaging.connector.api.data.Converter;
import io.openmessaging.connector.api.source.SourceTask;
import io.openmessaging.producer.Producer;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class RestHandlerTest {

    @Mock
    private ConnectController connectController;

    @Mock
    private ConfigManagementService configManagementService;

    @Mock
    private ClusterManagementService clusterManagementService;

    @Mock
    private PositionManagementService positionManagementService;

    @Mock
    private Worker worker;

    @Mock
    private Producer producer;

    private RestHandler restHandler;

    @Mock
    private ConnectConfig connectConfig;

    @Mock
    private SourceTask sourceTask;

    @Mock
    private Converter converter;

    @Mock
    private PositionStorageReader positionStorageReader;

    @Mock
    private Connector connector;

    private byte[] sourcePartition;

    private byte[] sourcePosition;

    private Map<ByteBuffer, ByteBuffer> positions;

    private static final String CREATE_CONNECTOR_URL = "http://localhost:8081/connectors/%s";

    private static final String STOP_CONNECTOR_URL = "http://localhost:8081/connectors/%s/stop";

    private static final String GET_CLUSTER_INFO_URL = "http://localhost:8081/getClusterInfo";

    private static final String GET_CONFIG_INFO_URL = "http://localhost:8081/getConfigInfo";

    private static final String GET_POSITION_INFO_URL = "http://localhost:8081/getPositionInfo";

    private static final String GET_ALLOCATED_INFO_URL = "http://localhost:8081/getAllocatedInfo";

    private HttpClient httpClient;

    private Map<String, Long> aliveWorker;

    private Map<String, ConnectKeyValue> connectorConfigs;

    private Map<String, List<ConnectKeyValue>> taskConfigs;

    private Set<WorkerConnector> workerConnectors;

    private Set<WorkerSourceTask> workerSourceTasks;

    @Before
    public void init() throws Exception {
        when(connectController.getConnectConfig()).thenReturn(connectConfig);
        when(connectConfig.getHttpPort()).thenReturn(8081);
        when(connectController.getConfigManagementService()).thenReturn(configManagementService);
        when(configManagementService.putConnectorConfig(anyString(), any(ConnectKeyValue.class))).thenReturn("");

        String connectName = "testConnector";
        ConnectKeyValue connectKeyValue = new ConnectKeyValue();
        connectKeyValue.put(RuntimeConfigDefine.CONNECTOR_CLASS, "io.openmessaging.connect.runtime.service.TestConnector");
        connectKeyValue.put(RuntimeConfigDefine.RUNTIME_OMS_DRIVER_URL, "oms:rocketmq://localhost:9876/default:default");
        connectKeyValue.put(RuntimeConfigDefine.SOURCE_RECORD_CONVERTER, "source-record-converter");

        ConnectKeyValue connectKeyValue1 = new ConnectKeyValue();
        connectKeyValue1.put(RuntimeConfigDefine.CONNECTOR_CLASS, "io.openmessaging.connect.runtime.service.TestConnector");
        connectKeyValue1.put(RuntimeConfigDefine.RUNTIME_OMS_DRIVER_URL, "oms:kafka://localhost:1234/default:default");
        connectKeyValue1.put(RuntimeConfigDefine.SOURCE_RECORD_CONVERTER, "source-record-converter1");

        List<ConnectKeyValue> connectKeyValues = new ArrayList<ConnectKeyValue>(8) {
            {
                add(connectKeyValue);
            }
        };
        connectorConfigs = new HashMap<String, ConnectKeyValue>() {
            {
                put(connectName, connectKeyValue);
            }
        };
        taskConfigs = new HashMap<String, List<ConnectKeyValue>>() {
            {
                put(connectName, connectKeyValues);
            }
        };
        when(configManagementService.getConnectorConfigs()).thenReturn(connectorConfigs);
        when(configManagementService.getTaskConfigs()).thenReturn(taskConfigs);

        aliveWorker = new HashMap<String, Long>() {
            {
                put("workerId1", System.currentTimeMillis());
                put("workerId2", System.currentTimeMillis());
            }
        };

        when(connectController.getClusterManagementService()).thenReturn(clusterManagementService);
        when(clusterManagementService.getAllAliveWorkers()).thenReturn(aliveWorker);

        sourcePartition = "127.0.0.13306".getBytes("UTF-8");
        JSONObject jsonObject = new JSONObject();
//        jsonObject.put(MysqlConstants.BINLOG_FILENAME, "binlogFilename");
//        jsonObject.put(MysqlConstants.NEXT_POSITION, "100");
        sourcePosition = jsonObject.toJSONString().getBytes();
        positions = new HashMap<ByteBuffer, ByteBuffer>() {
            {
                put(ByteBuffer.wrap(sourcePartition), ByteBuffer.wrap(sourcePosition));
            }
        };

        WorkerConnector workerConnector1 = new WorkerConnector("testConnectorName1", connector, connectKeyValue);
        WorkerConnector workerConnector2 = new WorkerConnector("testConnectorName2", connector, connectKeyValue1);
        workerConnectors = new HashSet<WorkerConnector>() {
            {
                add(workerConnector1);
                add(workerConnector2);
            }
        };
        WorkerSourceTask workerSourceTask1 = new WorkerSourceTask("testConnectorName1", sourceTask, connectKeyValue, positionStorageReader, converter, producer);
        WorkerSourceTask workerSourceTask2 = new WorkerSourceTask("testConnectorName2", sourceTask, connectKeyValue1, positionStorageReader, converter, producer);
        workerSourceTasks = new HashSet<WorkerSourceTask>() {
            {
                add(workerSourceTask1);
                add(workerSourceTask2);
            }
        };
        when(connectController.getWorker()).thenReturn(worker);
        when(worker.getWorkingConnectors()).thenReturn(workerConnectors);
        when(worker.getWorkingTasks()).thenReturn(workerSourceTasks);

        restHandler = new RestHandler(connectController);

        httpClient = HttpClientBuilder.create().build();
    }

    @Test
    public void testRESTful() throws Exception {
        URIBuilder uriBuilder = new URIBuilder(String.format(CREATE_CONNECTOR_URL, "testConnectorName"));
        uriBuilder.setParameter("config", "{\"connector-class\": \"org.apache.rocketmq.mysql.connector.MysqlConnector\",\"mysqlAddr\": \"112.74.179.68\",\"mysqlPort\": \"3306\",\"mysqlUsername\": \"canal\",\"mysqlPassword\": \"canal\",\"source-record-converter\":\"io.openmessaging.connect.runtime.converter.JsonConverter\",\"oms-driver-url\":\"oms:rocketmq://localhost:9876/default:default\"}");
        URI uri = uriBuilder.build();
        HttpGet httpGet = new HttpGet(uri);
        HttpResponse httpResponse = httpClient.execute(httpGet);
        assertEquals(200, httpResponse.getStatusLine().getStatusCode());
        assertEquals("success", EntityUtils.toString(httpResponse.getEntity(), "UTF-8"));

        URIBuilder uriBuilder1 = new URIBuilder(String.format(STOP_CONNECTOR_URL, "testConnectorName"));
        URI uri1 = uriBuilder1.build();
        HttpGet httpGet1 = new HttpGet(uri1);
        HttpResponse httpResponse1 = httpClient.execute(httpGet1);
        assertEquals(200, httpResponse1.getStatusLine().getStatusCode());
        assertEquals("success", EntityUtils.toString(httpResponse1.getEntity(), "UTF-8"));

        URIBuilder uriBuilder2 = new URIBuilder(GET_CLUSTER_INFO_URL);
        URI uri2 = uriBuilder2.build();
        HttpGet httpGet2 = new HttpGet(uri2);
        HttpResponse httpResponse2 = httpClient.execute(httpGet2);
        assertEquals(200, httpResponse2.getStatusLine().getStatusCode());
        assertEquals(JSON.toJSONString(aliveWorker), EntityUtils.toString(httpResponse2.getEntity(), "UTF-8"));

        URIBuilder uriBuilder3 = new URIBuilder(GET_CONFIG_INFO_URL);
        URI uri3 = uriBuilder3.build();
        HttpGet httpGet3 = new HttpGet(uri3);
        HttpResponse httpResponse3 = httpClient.execute(httpGet3);
        assertEquals(200, httpResponse3.getStatusLine().getStatusCode());
        String expectedResultConfig = "ConnectorConfigs:" + JSON.toJSONString(connectorConfigs) + "\nTaskConfigs:" + JSON.toJSONString(taskConfigs);
        assertEquals(expectedResultConfig, EntityUtils.toString(httpResponse3.getEntity(), "UTF-8"));

        URIBuilder uriBuilder4 = new URIBuilder(GET_ALLOCATED_INFO_URL);
        URI uri4 = uriBuilder4.build();
        HttpGet httpGet4 = new HttpGet(uri4);
        HttpResponse httpResponse4 = httpClient.execute(httpGet4);
        assertEquals(200, httpResponse4.getStatusLine().getStatusCode());
        StringBuilder sb = new StringBuilder();
        sb.append("working connectors:\n");
        for (WorkerConnector workerConnector : workerConnectors) {
            sb.append(workerConnector.toString() + "\n");
        }
        sb.append("working tasks:\n");
        for (WorkerSourceTask workerSourceTask : workerSourceTasks) {
            sb.append(workerSourceTask.toString() + "\n");
        }
        assertEquals(sb.toString(), EntityUtils.toString(httpResponse4.getEntity(), "UTF-8"));
    }

}