package utils;

import static utils.Utils.*;
import java.util.*;
import java.io.*;
import java.lang.*;
import net.sf.json.*;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.*;
import org.apache.log4j.*;

public class HbaseUtil{
	public static Logger _logger = Logger.getLogger(HbaseUtil.class);
	public static String _hbaseHost = null;
	private static handle _handle = null;

	public HbaseUtil(){
		_hbaseHost = "jx-bd-off-hadoop00.lianjia.com";
		_handle = new handle();
	}
	public void close(){
		_handle.close();
	}
	
	public long increment( String table, String rowkey, String family, String qualifier, long value ){
		try{
			return _handle.getTableHandle( table ).incrementColumnValue( rowkey.getBytes(), family.getBytes(), qualifier.getBytes(), value, false/*设置为false可以提升性能*/ );
		} catch(IOException e){
			return 0L;
		}
	}
	public long set( String table, String rowkey, String family, String qualifier, long value ){
		try{
			Delete	dlt = new Delete( rowkey.getBytes() );
					dlt.deleteColumns( family.getBytes(), qualifier.getBytes() );
			_handle.getTableHandle( table ).delete( dlt );
			return _handle.getTableHandle( table ).incrementColumnValue( rowkey.getBytes(), family.getBytes(), qualifier.getBytes(), value, false );
		} catch(IOException e){
			return 0L;
		}
	}

	public String getValue( String table, String rowkey, String family, String column ){
		Get get = new Get( rowkey.getBytes() );
		get.addColumn( family.getBytes(), column.getBytes() );
		try{
			Result r = _handle.getTableHandle( table ).get( get );
			return new String( r.getValue( family.getBytes(), column.getBytes() ) );
		} catch(IOException e){
			_logger.error( "Get Strom_Config by "+rowkey+"."+family+"."+column+" failed. "+e.getMessage() );
			return "";
		}
	}
	public void put( String table, List<Put> puts ){
		for( Put put : puts ){
			_logger.info( "Puts: put_String: "+put.toString() );
		}
		long stime = msTime();
		try{
			_handle.getTableHandle( table ).put( puts );
		} catch (IOException e){
			_logger.error( "Put into Hbase failed. table:"+table );
		}
		long etime = msTime();
		_logger.info( "Hbase("+ table +") Put Row: "+puts.size()+", Cost: "+(etime-stime));
	}

	public NavigableMap<byte[],NavigableMap<byte[],byte[]>> loadAll( String table, String rowkey ){
		Get get = new Get( rowkey.getBytes() );
		//get.getRow();
		try{
			Result r = _handle.getTableHandle( table ).get( get );
			return r.getNoVersionMap();
		} catch(IOException e){
			_logger.error( "Get Strom_Config by "+rowkey+" failed."+e.getMessage() );
			return null;
		}
	}

	/**
	 * inner class
	 * 负责handle多表的句柄
	 */
	public class handle{
		private Logger _logger = Logger.getLogger(HbaseUtil.handle.class);
		private Configuration _hbaseConfig = null;
		private HConnection _connection = null;
		private HashMap<String, HTableInterface> _hbTables = new HashMap<String, HTableInterface>();

		public handle(){
			_hbaseHost = HbaseUtil._hbaseHost;
			_hbaseConfig = HBaseConfiguration.create();
			_logger.info( "_hbaseHost: " + _hbaseHost );
			_hbaseConfig.set("hbase.zookeeper.quorum", _hbaseHost );
		}
		public HTableInterface getTableHandle( String tableName ){
			HTableInterface table = _hbTables.get( tableName );
			_logger.info( "tableName: "+tableName  );
			if( table==null ){
				try{
					HBaseAdmin hBaseAdmin = new HBaseAdmin( _hbaseConfig );
					if(!hBaseAdmin.tableExists( tableName )){
						_logger.error( "have to create "+tableName+" table in Hbase" );
						return null;
					}
					if( _connection == null ) {
						_connection = HConnectionManager.createConnection( _hbaseConfig );
					}
					_hbTables.put( tableName, _connection.getTable(tableName) );
				} catch ( ZooKeeperConnectionException e ){
					_logger.error( "Connect Hbase Error"+e.getMessage() );
					throw new RuntimeException("Connect Hbase Error"+e.getMessage() );
				} catch (Exception e) {
					_logger.error( "Connect Hbase Error,"+e.getMessage() );
					throw new RuntimeException("Connect Hbase Error,"+e.getMessage());
				}
			}
			return _hbTables.get( tableName );
		}
		public void close(){
			if( _hbTables.size()>0 ){
				Iterator iterator = _hbTables.entrySet().iterator();
				while (iterator.hasNext()) {
					Map.Entry entry = (Map.Entry) iterator.next();
					try{
						_hbTables.get( entry.getKey() ).close();
					} catch( IOException e ){
						_logger.error( "Close hbase table handle failed. "+e.getMessage() );
					}
				}
			}
			if( _connection!=null ){
				try{
					_connection.close();
				} catch( IOException e ){
					_logger.error( "Close hbase connect handle failed. "+e.getMessage() );
				}
			}
		}
	}
}



