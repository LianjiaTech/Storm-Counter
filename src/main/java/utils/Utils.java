package utils;

import java.util.*;
import java.io.*;
import java.text.*;
import java.math.*;
import java.nio.ByteBuffer;
import java.sql.Timestamp;
import net.sf.json.*;

public class Utils {
	private static ByteBuffer buffer = ByteBuffer.allocate(8);   
	public static byte[] longToBytes(long x) {  
		buffer.putLong(0, x);  
		return buffer.array();
	}


    private static HashMap<String, String> _month = new HashMap<String, String>();
    public static String getMonth( String mBig ){
        if( _month.size()==0 ){
            _month.put("Jan", "01");
            _month.put("Feb", "02");
            _month.put("Mar", "03");
            _month.put("Apr", "04");
            _month.put("May", "05");
            _month.put("Jun", "06");
            _month.put("Jul", "07");
            _month.put("Aug", "08");
            _month.put("Sep", "09");
            _month.put("Oct", "10");
            _month.put("Nov", "11");
            _month.put("Dec", "12");
        }
        if( _month.containsKey( mBig ) ){
            return _month.get(mBig).toString();
        }
        return null;
    }


    public static void waitForSeconds(int seconds) {
        try {
            Thread.sleep(seconds * 1000);
        } catch (InterruptedException e) {
        }
    }

    public static void waitForMillis(long milliseconds) {
        try {
            Thread.sleep(milliseconds);
        } catch (InterruptedException e) {
        }
    }
	public static String getTime(String user_time) {
        String re_time = null;
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date d;
        try {
            d = sdf.parse(user_time);
            long l = d.getTime();
            String str = String.valueOf(l);
            re_time = str.substring(0, 10);
        } catch (ParseException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return re_time;
    }

    public static long msTime(){
        return System.currentTimeMillis();
    }
    public static long sTime(){
        long ms = msTime();
        String s = Long.toString(ms);
        s = s.substring( 0, s.length()-3 );
        return Long.parseLong(s);
    }

    public static String strUpper(String name) {
        name = name.substring(0, 1).toUpperCase() + name.substring(1);
        return  name;
    }

    public static List<String> str2List( String str, String tag ){
        String arr[] = str.split( tag );
        List<String> response =  new ArrayList<String>();
        for( String v : arr ){
            response.add( v );
        }
        return response;
    }
	public static long recentMinute( long time ){
        return time-(time%60);
    }
    public static long recentHour( long time ){
        return time-(time%3600);
    }
    public static long recentDay( long time ){
        return time-((time+28800)%86400);
    }
}
