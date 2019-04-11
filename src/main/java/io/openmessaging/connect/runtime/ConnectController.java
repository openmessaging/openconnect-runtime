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

package io.openmessaging.connect.runtime;

import io.openmessaging.MessagingAccessPoint;
import io.openmessaging.connect.runtime.common.LoggerName;
import io.openmessaging.connect.runtime.config.ConnectConfig;
import io.openmessaging.connect.runtime.connectorwrapper.Worker;
import io.openmessaging.connect.runtime.rest.RestHandler;
import io.openmessaging.connect.runtime.service.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Connect controller to access and control all resource in runtime.
 */
public class ConnectController {

    private static final Logger log = LoggerFactory.getLogger(LoggerName.OMS_RUNTIME);

    /**
     * Configuration of current runtime.
     */
    private final ConnectConfig connectConfig;

    /**
     * All the configurations of current running connectors and tasks in cluster.
     */
    private final ConfigManagementService configManagementService;

    /**
     * Position management of source tasks.
     */
    private final PositionManagementService positionManagementService;

    /**
     * Manage the online info of the cluster.
     */
    private final ClusterManagementService clusterManagementService;

    /**
     * A worker to schedule all connectors and tasks assigned to current process.
     */
    private final Worker worker;

    /**
     * OpenMessaging access point, which is capable of creating producer, consumer and other facility entities.
     */
    private final MessagingAccessWrapper messagingAccessWrapper;

    /**
     * A REST handler, interacting with user.
     */
    private final RestHandler restHandler;

    /**
     * Assign all connectors and tasks to all alive process in the cluster.
     */
    private final RebalanceImpl rebalanceImpl;

    /**
     * A scheduled task to rebalance all connectors and tasks in the cluster.
     */
    private final RebalanceService rebalanceService;

    /**
     * Thread pool to run schedule task.
     */
    private ScheduledExecutorService scheduledExecutorService;

    public ConnectController(ConnectConfig connectConfig) {

        this.connectConfig = connectConfig;
        this.messagingAccessWrapper = new MessagingAccessWrapper();
        MessagingAccessPoint messageAccessPoint = messagingAccessWrapper.getMessageAccessPoint(connectConfig.getRuntimeOmsDriverUrl());
        this.clusterManagementService = new ClusterManagementServiceImpl(connectConfig, messageAccessPoint);
        this.configManagementService = new ConfigManagementServiceImpl(connectConfig, messageAccessPoint);
        this.positionManagementService = new PositionManagementServiceImpl(connectConfig, messageAccessPoint);
        this.worker = new Worker(connectConfig, positionManagementService, messagingAccessWrapper);
        this.rebalanceImpl = new RebalanceImpl(worker, configManagementService, clusterManagementService);
        this.restHandler = new RestHandler(this);
        this.rebalanceService = new RebalanceService(rebalanceImpl, configManagementService, clusterManagementService);
    }

    public void initialize() {

        this.scheduledExecutorService = Executors.newSingleThreadScheduledExecutor((r) -> new Thread(r, "ConnectScheduledThread"));
    }

    public void start() {

        clusterManagementService.start();
        configManagementService.start();
        positionManagementService.start();
        worker.start();
        rebalanceService.start();

        // Persist configurations of current connectors and tasks.
        this.scheduledExecutorService.scheduleAtFixedRate(() -> {

            try {
                ConnectController.this.configManagementService.persist();
            } catch (Exception e) {
                log.error("schedule persist config error.", e);
            }
        }, 1000, this.connectConfig.getConfigPersistInterval(), TimeUnit.MILLISECONDS);

        // Persist position information of source tasks.
        this.scheduledExecutorService.scheduleAtFixedRate(() -> {

            try {
                ConnectController.this.positionManagementService.persist();
            } catch (Exception e) {
                log.error("schedule persist position error.", e);
            }
        }, 1000, this.connectConfig.getPositionPersistInterval(), TimeUnit.MILLISECONDS);
    }

    public void shutdown() {

        if (clusterManagementService != null) {
            clusterManagementService.stop();
        }

        if (configManagementService != null) {
            configManagementService.stop();
        }

        if (positionManagementService != null) {
            positionManagementService.stop();
        }

        if (worker != null) {
            worker.stop();
        }

        if (configManagementService != null) {
            configManagementService.persist();
        }

        if (positionManagementService != null) {
            positionManagementService.persist();
        }

        this.scheduledExecutorService.shutdown();
        try {
            this.scheduledExecutorService.awaitTermination(5000, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            log.error("shutdown scheduledExecutorService error.", e);
        }

        messagingAccessWrapper.removeAllMessageAccessPoint();

        if (rebalanceService != null) {
            rebalanceService.stop();
        }
    }

    public ConnectConfig getConnectConfig() {
        return connectConfig;
    }

    public ConfigManagementService getConfigManagementService() {
        return configManagementService;
    }

    public PositionManagementService getPositionManagementService() {
        return positionManagementService;
    }

    public ClusterManagementService getClusterManagementService() {
        return clusterManagementService;
    }

    public Worker getWorker() {
        return worker;
    }

    public MessagingAccessWrapper getMessagingAccessWrapper() {
        return messagingAccessWrapper;
    }

    public RestHandler getRestHandler() {
        return restHandler;
    }

    public RebalanceImpl getRebalanceImpl() {
        return rebalanceImpl;
    }
}
