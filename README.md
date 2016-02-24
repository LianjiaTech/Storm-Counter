# Storm-Counter
Storm consumer command queue from Kafka, then parse and excute command to storage numbers into Hbase

## Feature

* Storm as consumer to consume message queue (this version just support Kafka)，the Queue is a big command list, every command is a JSON String, Storm just parse every command then excute it.
* In this version, Storm job support INCREMENT and SET operation. Increment just increase value in HBase. Set operation can overflow old value. 
* <b>Particular attention</b>: please use INCREAMENT command to init a new KV data in HBbase.

## Graphic Rendition
 ![image](https://raw.githubusercontent.com/pangee/Storm-Counter/master/images/storm-counter.png)
 
## Command
Command is a JSON String, Command as text log write into Message queue(Kafka), and control Storm-Counter to Increase/Set a value.<br/>
Command example:
```Java
{"table":" TABLENAME ","operation":" OPERRATION ","rowkey":" HBaseRowkey ","family":" HBaseFamilyName ","qualifier":" HBaseQualifier ","value": VALUE }
```

INCREMENT Operation:
```Java
{"table":"eg:table_name","operation":"increase","rowkey":"rowkey_20160224","family":"user","qualifier":"age","value":1}
```

Set Operation:
```Java
{"table":"eg:table_name","operation":"set","rowkey":"rowkey_20160224","family":"user","qualifier":"age","value":99}
```

## Usage 
You Must Install JAVA+Maven+Storm
* git clone git@github.com:LianjiaTech/Storm-Counter.git 
* cd Storm-Counter
* mvn clean install
* cd target 
* storm jar counter-0.0.1-SNAPSHOT.jar topology.counter kafka `10.10.1.1:2181` `/brokers` `Topicname` `10` 
`10.10.1.1:2181`: your Kafka zookeeper host and port
`/brokers`: your kafka zookeeper path
`Topicname`: your kafka topic name
`10`: Storm counter concurrency number
* storm 
