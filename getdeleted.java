package org.diffdb;

import java.io.IOException; 
import java.util.*; 
 
import org.apache.hadoop.fs.*; 
import org.apache.hadoop.conf.*; 
import org.apache.hadoop.io.*; 
import org.apache.hadoop.mapreduce.*; 
import org.apache.hadoop.util.*; 
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
import org.apache.hadoop.mapreduce.lib.output.*;
 
public class getdeleted{ 
    
    public static class getdeletedMap extends TableMapper <Text, Text>{ 
        private HTable newTable;
        private Class<?> entryType;
        private Long endTime;
        
        public void setup(Context context) throws IOException{
            try{
                entryType = Class.forName(context.getConfiguration().get("classname"));
                endTime = new Long(context.getConfiguration().get("timestamp"));
            } catch(Exception E) {
                System.out.println("Exception: " + E.toString());
                return;
            }
        }
        
        public void map(ImmutableBytesWritable key, Result value, Context context)throws IOException, InterruptedException { 
            genericEntry entry;
            try{
                entry = (genericEntry) entryType.newInstance();
            } catch(Exception E) {
                System.out.println("Exception caught: " + E.toString() + "\n Stacktrace:");
                E.printStackTrace();
                return;
            }
            entry.addEntry(value);
            Long exists = new Long(entry.getTableEntry("EXISTS"));
            if(exists < endTime) {
                Text retText = new Text(value.getRow());
                Text tesText = new Text("");
                context.write(retText, tesText);
            }
        }
    }

    public static class getdeletedReduce extends Reducer<Text, Text, Text, Text>{
        public void reduce(Text key, Text value, Context context) throws IOException, InterruptedException {
            context.write(key, null);
        }
    }

    public static void main(String[] args) throws Exception { 
        String inputTable = args[0];
        String outputPath = args[1];
        String timestamp = args[2];
        String className = args[3];
        
        //JobConf conf = new JobConf(diffdb.class); 
        Configuration config = HBaseConfiguration.create();
        HBaseAdmin hbAdmin = new HBaseAdmin(config);
        dbutil db_util = new dbutil(config);
        
        config.set("timestamp", timestamp);
        config.set("classname", "org.diffdb." + className + "Entry");
        
        Scan ourScan = new Scan();
        ourScan.setCaching(500);
        ourScan.setCacheBlocks(false);
        
        Long timestampE = new Long(timestamp);
        timestampE += 1;
        ourScan.setTimeRange(0, timestampE);
        
        //config.set(TableInputFormat.INPUT_TABLE, inputTable);
        Job job = new Job(config, "getDeleted");
        TableMapReduceUtil.initTableMapperJob(db_util.getRealName(inputTable), ourScan, getdeletedMap.class, Text.class, Text.class, job);
        job.setJarByClass(getdeletedMap.class);
        job.setMapperClass(getdeletedMap.class);
        job.setReducerClass(getdeletedReduce.class);
        TextOutputFormat.setOutputPath(job, new Path(outputPath));
        job.setOutputFormatClass(TextOutputFormat.class); 
        job.setNumReduceTasks(1);

        System.out.println("Doing the stuffz!!!\n");
        job.waitForCompletion(true); 
    }
}