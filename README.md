# Storm-Counter
Storm consumer command queue from Kafka, then parse and excute command to storage numbers into Hbase

## Feature

* Storm as consumer to consume message queue (this version just support Kafka)，the Queue is a big command list, every command is a JSON String, Storm just parse every command then excute it.
* In this version, Storm job support INCREMENT and SET operation. Increment just increase value in HBase. Set operation can overflow old value. 
* <b>Particular attention</b>: please use INCREAMENT command to init a new KV data in HBbase.

## Graphic Rendition
 ![image](https://raw.githubusercontent.com/pangee/Storm-Counter/master/images/storm-counter.png)
 
## Command
  TODO
