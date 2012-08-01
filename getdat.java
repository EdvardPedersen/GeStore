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
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
 
public class getdat{ 
    public static class getdatMap extends TableMapper<Text, Text>{ 
        public void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException { 
            uniprotEntry entry = new uniprotEntry(value);
            Text ekey = new Text();
            Text keyVal = new Text(entry.get("DAT", "")[0]);
            try {
                context.write(ekey, keyVal);
            } catch (IOException E) {
                System.out.println("EXCEPTION! " + E.toString());
            }
        }
    } 

    public static class getdatReduce extends Reducer<Text, Text, Text, Text>{
        public void reduce(Text key, Text value, Context context) throws IOException, InterruptedException {
            context.write(null, value);
        }
    }

    public static void main(String[] args) throws Exception { 
        String inputTableS = args[0];
        String outputFile = args[1];
        String timestampStart = args[2];
        String timestampStop = args[3];
        
        Configuration config = HBaseConfiguration.create();
        config.set("mapred.textoutputformat.separator","\n");
        config.set("timestamp", timestampStop);
        
        Job job = new Job(config, "getfasta");
        HBaseAdmin hbAdmin = new HBaseAdmin(config);
        if(!hbAdmin.tableExists(inputTableS)){
            System.out.println("NO TABLE!");
        }
        
        Scan ourScan = new Scan();
        ourScan.setCaching(500);
        ourScan.setCacheBlocks(false);
        ourScan.setTimeRange(new Long(timestampStart), new Long(timestampStop));
        
        job.setJarByClass(getdatMap.class);
        
        TableMapReduceUtil.initTableMapperJob(inputTableS, ourScan, getdatMap.class, Text.class, Text.class, job);
        //job.setOutputKeyClass(Text.class);
        //job.setOutputValueClass(Text.class);
        //job.setInputFormatClass(TableInputFormat.class); 
       
        //job.setMapperClass(getfastaMap.class);
        job.setReducerClass(getdatReduce.class);
        job.setNumReduceTasks(1);

        TextOutputFormat.setOutputPath(job, new Path(outputFile));
        job.setOutputFormatClass(TextOutputFormat.class); 

        job.submit(); 
        System.out.println(job.getTrackingURL());
    }
}