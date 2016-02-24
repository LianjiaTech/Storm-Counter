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

public class DataCheckBolt extends BaseRichBolt {
	private static final Logger _logger = LoggerFactory.getLogger("DataCheckBolt.class");
	private OutputCollector _outputCollector;
	public void prepare(Map conf, TopologyContext topologyContext, OutputCollector outputCollector) {
		_outputCollector = outputCollector;
    }
    public void execute(Tuple tuple) {
		String log = tuple.getString(0);
		try {
			JSONObject jsonObj = new JSONObject( JSONValue.parse(log).toString() );
			if( 
				jsonObj.has("table") && !jsonObj.isNull("table") && 
				jsonObj.has("operation") && !jsonObj.isNull("operation") && 
				jsonObj.has("rowkey") && !jsonObj.isNull("rowkey") && 
				jsonObj.has("family") && !jsonObj.isNull("family") && 
				jsonObj.has("qualifier") && !jsonObj.isNull("qualifier") &&
				jsonObj.has("value") && !jsonObj.isNull("value")
			){
				Values emitValues = new Values();
				emitValues.add( jsonObj.getString("table") );
				emitValues.add( jsonObj.getString("operation") );
				emitValues.add( jsonObj.getString("rowkey") );
				emitValues.add( jsonObj.getString("family") );
				emitValues.add( jsonObj.getString("qualifier") );
				emitValues.add( jsonObj.getLong("value") );
				_outputCollector.emit( emitValues );
			} else {
				_logger.error( "Invalid data: "+log );
			}
		} catch ( JSONException e ){
			_logger.error( "Invalid data: "+log );
			_outputCollector.ack( tuple );
			return ;
		} catch ( Exception e ){
			_logger.error( "Invalid data: "+log );
			_outputCollector.ack( tuple );
			return ;
		}
		_outputCollector.ack( tuple );
		return ;
	}
	public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
		List<String> fields = new ArrayList<String>();
        fields.add( "table" );
        fields.add( "operation" );
        fields.add( "rowkey" );
        fields.add( "family" );
        fields.add( "qualifier" );
        fields.add( "value" );
        outputFieldsDeclarer.declare( new Fields( fields ) );	
	}
}
