# OpenMessaging Connect

## Introduction
OpenMessaging Connect is a standard to connect between data sources and data destinations. Users could easily create connector instances with configurations via REST API.

There are two types of connectors: source connector and sink connector. A source connector is used for pulling data from a data source (e.g. RDBMS).
The data is sent to corresponding message queue and expected to be consumed by one or many sink connectors.
A sink connector receives message from the queue and loads into a data destination (e.g. data warehouse).
Developers should implement source or sink connector interface to run their specific job.

Usually, connectors rely on a concrete message queue for data transportation. The message queue decouples source connectors from sink connectors.
In the meantime, it provides capabilities such as failover, rate control and one to many data transportation etc.
Some message queues (e.g. Kafka) provide bundled connect frameworks and a various of connectors developed officially or by the community.
However, these frameworks are lack of interoperability, which means a connector developed for Kafka cannot run with 
RabbitMQ without modification and vice versa.

![dataflow](flow.png "dataflow")

A connector follows OpenMessaging Connect could run with any message queues which support OpenMessaging API.
OpenMessaging Connect provides a standalone runtime which uses OpenMessaging API for sending and consuming message,
as well as the key/value operations for offset management.

## Connector

This runtime is based on interface [openmessaging-connector](https://github.com/openmessaging/openmessaging-connector).

## Runtime quick start

### Prerequisite

* 64bit JDK 1.8+;
* Maven 3.2.x;
* A running MQ cluster;

### Build

```
mvn clean install -Dmaven.test.skip=true
```

### Run Command Line

```
## Enter runtime directory
cd runtime/

## run run_worker.sh
sh ./run_worker.sh
```

### Log Path

Default path is:
```
${user.home}/logs/omsconnect/
```

If you see "The worker XXX boot success." without any exception, it means worker started successfully.

### Make Sure Worker Started Successfully

Use http request to get all alive workers in cluster:
```
GET /getClusterInfo
```
You will see all alive workers' id with latest heartbeat timestamp.

### Run The Example Mysql Source Connector

Use the following http request, to start a mysql source connector as an example.
```
GET http://(your worker ip):(port)/connectors/(Your connector name)?config={"connector-class":"io.openmessaging.mysql.connector.MysqlConnector","oms-driver-url":"oms:rocketmq://localhost:9876/default:default","mysqlAddr":"localhost","mysqlPort":"3306","mysqlUsername":"username","mysqlPassword":"password","source-record-converter":"io.openmessaging.connect.runtime.converter.JsonConverter"}
```
Note to replace the arguments in "()" with your own mysql setting.

#### Config introduction

|key               |nullable|default    |description|
|------------------|--------|-----------|-----------|
|connector-class         |false   |           |Full class name of the impl of connector|
|oms-driver-url         |false   |           |An OMS driver url, the data will send to the specified MQ|
|mysqlAddr        |false   |           |MySQL address|
|mysqlPort         |false   |           |MySQL port|
|mysqlUsername         |false   |           |Username of MySQL account|
|mysqlPassword         |false   |           |Password of MySQL account|
|source-record-converter         |false   |           |Full class name of the impl of the converter used to convert SourceDataEntry to byte[]|


### Verify Mysql Source Connector Started Successfully

Make a mysql row update, your will found the MQ you config receive a message, and the message's topic name is the table name.

## Note

Module example-mysql-connector is just a simple example of source connector. 
This module is base on [RocketMQ Mysql](https://github.com/apache/rocketmq-externals/tree/master/rocketmq-mysql) with a little code modified in order to implement connector interface.
User can reference this module to implement other source or sink connector.

## RoadMap

1. Implement WorkerSinkTask wrapper in runtime;
2. Import openMessaging impl and connector impl dynamically in runtime, rather than import them in pom file;
3. An overall optimization of runtime module, provide a better interaction.
4. Implementation of various source and sink connector.