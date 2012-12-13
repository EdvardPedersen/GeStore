package org.diffdb;

import java.io.IOException; 
import java.util.*; 
import java.io.*; 
import java.lang.reflect.*;
 
import org.apache.hadoop.fs.*; 
import org.apache.hadoop.conf.*; 
import org.apache.hadoop.io.*; 
import org.apache.hadoop.mapreduce.*; 
import org.apache.hadoop.util.*; 
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.mapreduce.*;
import org.apache.hadoop.hbase.client.Put;
import org.apache.zookeeper.*;
 
public class inspectpipeline extends Configured implements Tool{ 

    public static void main(String[] args) throws Exception {
        int result = ToolRunner.run(new Configuration(), new inspectpipeline(), args);
        return;
    }
    
    public int run(String[] args) throws Exception { 
        Configuration argConf = getConf();
        
        //JobConf conf = new JobConf(diffdb.class); 
        Configuration config = HBaseConfiguration.create();
        HBaseAdmin hbAdmin = new HBaseAdmin(config);
        dbutil db_util = new dbutil(config);
        
        HTable runTable = new HTable(config, "gestore_runs");
        Get runGet = new Get(argConf.get("id").getBytes());
        Result pipeline = runTable.get(runGet);
        
        NavigableMap<byte[], byte[]> pipeMap = pipeline.getFamilyMap("d".getBytes());
        
        Map.Entry<byte[], byte[]> results = pipeMap.pollFirstEntry();
        
        HashMap<String, HashMap<String,String>> resultMap = new HashMap<String, HashMap<String, String>>();
        
        while(results != null) {
	  String resultKey = new String(results.getKey());
	  String resultValue = new String(results.getValue());
	  String field = "type";
	  HashMap<String,String> tempMap = new HashMap<String,String>();
	  String entry = resultKey;
	  
	  if(resultKey.endsWith("_db_timestamp")) {
	    field = "db_timestamp";
	    entry = resultKey.substring(0, resultKey.lastIndexOf("_db_timestamp"));
	  } else if(resultKey.endsWith("_filename")) {
	    field = "filename";
	    entry = resultKey.substring(0, resultKey.lastIndexOf("_filename"));
	  } else if(resultKey.endsWith("_regex")) {
	    field = "regex";
	    entry = resultKey.substring(0, resultKey.lastIndexOf("_regex"));
	  }
	  
	  if(resultMap.containsKey(entry)) {
	    tempMap = resultMap.get(entry);
	  }
	  
	  tempMap.put(field, resultValue);
	  resultMap.put(entry, tempMap);
	  
	  //System.out.println("Key: " + resultKey + " Value: " + resultValue);
	  results = pipeMap.pollFirstEntry();
        }
        
        for(String key : resultMap.keySet()) {
	  System.out.println("File ID: " + key);
	  for(String subKey : resultMap.get(key).keySet()) {
	    //System.out.println("\t " + subKey + "\t\t" + resultMap.get(key).get(subKey));
	    System.out.format("  %1$-20s  %2$s\n", subKey, resultMap.get(key).get(subKey));
	  }
        }
        
        return 0;
    }
}