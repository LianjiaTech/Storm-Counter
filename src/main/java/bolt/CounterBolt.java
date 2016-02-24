package bolt;

import backtype.storm.*;
import backtype.storm.tuple.*;
import backtype.storm.task.*;
import backtype.storm.topology.*;
import backtype.storm.topology.base.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;
import java.math.*;
import java.lang.*;
import java.util.regex.*;
import net.sf.json.*;
import org.json.JSONObject;
import org.json.simple.JSONValue;

import static utils.Utils.*;
import utils.HbaseUtil;

public class CounterBolt extends BaseRichBolt {
	private static final Logger _logger = LoggerFactory.getLogger("CounterBolt.class");
	private OutputCollector _outputCollector;
	private static HashMap<String, HbaseUtil> hbTables = new HashMap<String, HbaseUtil>();
	public void prepare(Map conf, TopologyContext topologyContext, OutputCollector outputCollector) {
		_outputCollector = outputCollector;
    }
    public void execute(Tuple tuple) {
		String table = tuple.getStringByField( "table" );
		String operation = tuple.getStringByField( "operation" );
		String rowkey = tuple.getStringByField( "rowkey" );
		String family = tuple.getStringByField( "family" );
		String qualifier = tuple.getStringByField( "qualifier" );
		long value = tuple.getLongByField( "value" );
		
		long valueOfIncrement = 0L;
		String result = operation+":"+table+","+rowkey+","+family+","+qualifier+"="+value+" | Result:";
		if( operation.equals("increase") ){
			valueOfIncrement = hbInterface( table ).increment( table, rowkey, family, qualifier, value );
		} else if ( operation.equals("set") ){
			valueOfIncrement = hbInterface( table ).set( table, rowkey, family, qualifier, value );
		}
		if( valueOfIncrement > 0L ) {
			result += "success - "+valueOfIncrement;
		} else {
			result += "failed - "+valueOfIncrement;
		}
		_logger.info( result );

		_outputCollector.ack( tuple );
	}
	private HbaseUtil hbInterface( String table ){
		if( !hbTables.containsKey( table ) ){
			hbTables.put( table, new HbaseUtil() );
		}
		return hbTables.get( table );
	}
	public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
	}
}
