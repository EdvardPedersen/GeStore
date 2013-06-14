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
 
public class countupdates{ 
    public static class countupdatesMap extends TableMapper<Text, IntWritable>{ 
        public void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
            Long timestamp = new Long(context.getConfiguration().get("timestamp"));
            IntWritable valOut = new IntWritable(1);
            NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> resultMap = value.getMap();
            while(resultMap.size() > 0) {
                Map.Entry<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> familyVal = resultMap.pollFirstEntry();
                NavigableMap<byte[],NavigableMap<Long,byte[]>> qualifiers = familyVal.getValue();
                String family = new String(familyVal.getKey());
                Vector<String> newEntries = new Vector<String>();
                int numQualifiers = qualifiers.size();
                boolean newEntry = false;
                boolean deletedEntry = false;
                boolean updatedEntry = false;
                boolean currentEntry = false;
                int updFields = 0;
                int newFields = 0;
                while(qualifiers.size() > 0) {
                    Map.Entry<byte[], NavigableMap<Long, byte[]>> qualifierVal = qualifiers.pollFirstEntry();
                    String qualifier = new String(qualifierVal.getKey());
                    NavigableMap<Long, byte[]> values = qualifierVal.getValue();
                    if(qualifier.equals("EXISTS")) {
                        Map.Entry<Long, byte[]> biggest = values.firstEntry();
                        Map.Entry<Long, byte[]> smallest = values.lastEntry();
                        Long current = biggest.getKey();
                        Long previous = smallest.getKey();
                        if(current < timestamp) {
                            deletedEntry = true;
                        } else if(previous.equals(timestamp)) {
                            newEntry = true;
                            currentEntry = true;
                        } else {
                            currentEntry = true;
                        }
                        continue;
                    }
                    while(values.size() > 0) {
                        Map.Entry<Long, byte[]> resEntry = values.pollFirstEntry();
                        if(resEntry.getKey().equals(timestamp) && !resEntry.getValue().equals("DEL".getBytes())) {
                            updatedEntry = true;
                            //currentEntry = true;
                            //Text keyOut = new Text(qualifier);
                            newEntries.add(qualifier);
                            //context.write(keyOut, valOut);
                        } else if(resEntry.getKey().equals(timestamp) && resEntry.getValue().equals("DEL".getBytes())){
                            System.out.println(resEntry.getKey().toString() + " " + "DEL");
                        }
                    }
                }
                if(newEntry) {
                    Text keyOut = new Text("NEW");
                    context.write(keyOut, valOut);
                } else if(deletedEntry) {
                    Text keyOut = new Text("DEL");
                    context.write(keyOut, valOut);
                } else if(updatedEntry) {
                    Text keyOut = new Text("UPDATE");
                    context.write(keyOut, valOut);
                    for(String entry : newEntries) {
                        keyOut = new Text(entry);
                        context.write(keyOut, valOut);
                    }
                }
                
                if(currentEntry) {
                    Text keyOutF = new Text("COUNT");
                    context.write(keyOutF, valOut);
                }
                
                Text keyOutF = new Text("TOTAL");
                context.write(keyOutF, valOut);
            }
        }
    }

    public static class countupdatesReduce extends Reducer<Text, IntWritable, Text, IntWritable>{
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for(IntWritable val : values) {
                sum += val.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }

    public static void main(String[] args) throws Exception { 
        String inputTable = args[0];
        String outputFile = args[1];
        String timestampStart = args[2];
        String timestampStop = args[3];
        
        Configuration config = HBaseConfiguration.create();
        
        //config.set(TableInputFormat.SCAN_TIMERANGE_START, timestampStart);
        config.set(TableInputFormat.SCAN_TIMERANGE_END, timestampStop);
        config.set("timestamp", timestampStop);
        config.set("mapred.task.maxpmem", "8589934592");
        config.set("mapred.job.map.memory.mb", "3072");
        config.set("mapred.job.reduce.memory.mb", "3072");
        
        
        Job job = new Job(config, "countupdates");
        HBaseAdmin hbAdmin = new HBaseAdmin(config);
        if(!hbAdmin.tableExists(inputTable)){
            System.out.println("NO TABLE!");
        }
        
        Scan ourScan = new Scan();
        ourScan.setCaching(500);
        ourScan.setCacheBlocks(false);
        
        Long timestampStopL = new Long(timestampStop);
        timestampStopL += 1;
        ourScan.setTimeRange(new Long(0), timestampStopL);
        ourScan.setMaxVersions(2);
        
        job.setJarByClass(countupdatesMap.class);
        
        TableMapReduceUtil.initTableMapperJob(inputTable, ourScan, countupdatesMap.class, Text.class, IntWritable.class, job);
        job.setReducerClass(countupdatesReduce.class);
        job.setNumReduceTasks(1);

        TextOutputFormat.setOutputPath(job, new Path(outputFile));
        job.setOutputFormatClass(TextOutputFormat.class); 
        
        job.waitForCompletion(true); 
        System.out.println(job.getTrackingURL());
    }
}
