package topology;

import backtype.storm.*;
import backtype.storm.generated.*;
import backtype.storm.spout.*;
import backtype.storm.tuple.*;
import backtype.storm.topology.*;
import storm.trident.*;
import storm.kafka.*;
import storm.kafka.trident.*;
import storm.starter.bolt.*;
import java.util.*;
import org.slf4j.*;

import static utils.Utils.*;
import bolt.DataCheckBolt;
import bolt.CounterBolt;

public class counter {
    private static final Logger _logger = LoggerFactory.getLogger("counter.class");
    private static Config _conf = new Config();

    public static SpoutConfig kafkaConfig( String host, String path, String topicName ){
        SpoutConfig spoutConfig = new SpoutConfig(
                new ZkHosts( host, path ), topicName, "/"+topicName,
                UUID.randomUUID().toString()
        );
        spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
        //spoutConfig.startOffsetTime = kafka.api.OffsetRequest.EarliestTime();
        spoutConfig.startOffsetTime = kafka.api.OffsetRequest.LatestTime();
        spoutConfig.socketTimeoutMs = 120000;
        spoutConfig.bufferSizeBytes = 256*1024; /* 256KB */
		return spoutConfig;
    }

	/**
	 * Usage: storm jar {xxx}.jar {className} kafka {kafkaHost:Port} {kafkaPath} {topicName} {Concurrency Number}
	 */
	public static boolean checkArgs( String[] args ){
        if( args[0]==null || args[0].length()<=0 ) {
			_logger.error( "error: missing data source type" );
			_logger.error( "Data source type: kafka" );
			return false;
		} else if ( args[1]==null || args[1].length()<=0 ){
			_logger.error( "error: missing kafka zookeeper host" );
			return false;
		} else if ( args[2]==null || args[2].length()<=0 ){
			_logger.error( "error: missing kafka zookeeper path" );
			return false;
		} else if ( args[3]==null || args[3].length()<=0 ){
			_logger.error( "error: missing kafka topic name" );
			return false;
		}
		return true;
	}

    public static void main(String[] args) throws Exception {
		if( !checkArgs( args ) ){
			return ;
		}
		
		/**
		 * 并发数设置
		 */
		int concurrencyNumber = 10;
		if( args[4]==null || args[4].length()<=0 ){
			int args4 = Integer.parseInt( args[4] );
			if( args4>0 ){
				concurrencyNumber = args4;
			}
		}

		TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout( "kafkaSpout", new KafkaSpout( kafkaConfig( args[1], args[2], args[3] ) ), 1 );
        builder.setBolt( "dataCheck", new DataCheckBolt(), 10 ).shuffleGrouping( "kafkaSpout" );
        builder.setBolt( "counter", new CounterBolt(), concurrencyNumber ).fieldsGrouping( "dataCheck", new Fields("table") );

        _conf.put(Config.TOPOLOGY_RECEIVER_BUFFER_SIZE,            32);
        _conf.put(Config.TOPOLOGY_TRANSFER_BUFFER_SIZE,			  128);
        _conf.put(Config.TOPOLOGY_EXECUTOR_RECEIVE_BUFFER_SIZE, 32768);
        _conf.put(Config.TOPOLOGY_EXECUTOR_SEND_BUFFER_SIZE,    32768);

        //LocalCluster cluster = new LocalCluster();
        //cluster.submitTopology( "test", _conf, builder.createTopology() );

		StormSubmitter.submitTopology( "TJ-Platfrom-Storm-Job", _conf, builder.createTopology() );
	}

}
