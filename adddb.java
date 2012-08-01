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
 
public class adddb{ 
    
    public static class updatedbMap extends Mapper <LongWritable, Text, LongWritable, Put>{ 
        private HTable outputTable;
        private Long timestamp;
        private Class<?> entryType;
        
        public void setup(Context context) throws IOException{
            try {
                outputTable = new HTable(context.getConfiguration(), context.getConfiguration().get(TableOutputFormat.OUTPUT_TABLE));
                timestamp = new Long(context.getConfiguration().get("timestamp"));
                entryType = Class.forName(context.getConfiguration().get("classname"));
            } catch (Exception E) {
                System.out.println("Exception: " + E.toString());
            }
        }
        
        public void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException { 
            String[] resultStrings = value.toString().split("\n");

            genericEntry newEntry;
            genericEntry oldEntry;
            genericEntry newerEntry;
            try {
                newEntry = (genericEntry) entryType.newInstance();
                oldEntry = (genericEntry) entryType.newInstance();
                newerEntry = (genericEntry) entryType.newInstance();
            } catch(Exception E) {
                return;
            }
            newEntry.addEntries(resultStrings);
            Get newResultGet = new Get(newEntry.getRowID());
            Get oldResultGet = new Get(newEntry.getRowID());
            newResultGet.setTimeRange(timestamp+1, Long.MAX_VALUE);
            oldResultGet.setTimeRange(new Long(0), timestamp);
            
            Result newResult = outputTable.get(newResultGet);
            Result cmpResult = outputTable.get(oldResultGet);
            
            if(!oldEntry.addEntry(cmpResult)) {
                oldEntry = null;
            }
            try {
                newerEntry.addEntry(newResult);
            } catch(Exception E) {
                newerEntry = null;
            }
            
            if(oldEntry == null) {
                try{
                    oldEntry = (genericEntry) entryType.newInstance();
                }catch(Exception E) {
                    return;
                }
            }
            
            if(newerEntry == null) {
                try {
                    newerEntry = (genericEntry) entryType.newInstance();
                } catch(Exception E) {
                    return;
                }
            }
            
            Put newPut = null;
            
            Vector<String> result = newEntry.compare(oldEntry);
            if(!result.isEmpty()) {
                newPut = newEntry.getPartialPut(result, timestamp);
            } else {
                newPut = new Put(newEntry.getRowID());
            }
            
            Vector<String> newerResult = newEntry.equalFields(newerEntry);
            if(!newerResult.isEmpty()) {
                Delete delRow = new Delete(newEntry.getRowID());
                for(String line : newerResult) {
                    if(newEntry.getTimestamp(line) != newerEntry.getTimestamp(line)) {
                        delRow.deleteColumn("d".getBytes(), line.getBytes(), newerEntry.getTimestamp(line));
                    }
                }
                outputTable.delete(delRow);
            }
            
            if(oldEntry.exists()) {
                Vector<String> deletedEntries = oldEntry.getDeleted(newEntry);
                for(String line : deletedEntries) {
                    newPut.add("d".getBytes(), line.getBytes(), timestamp, "DEL".getBytes());
                }
            }
            newPut.add("d".getBytes(), "EXISTS".getBytes(), timestamp, timestamp.toString().getBytes());
            
            try {
                context.write(key, newPut);
            } catch(IOException e) {
                System.out.println(newPut.toString());
                System.out.println(e.toString());
            }
        } 
    }
    
    public static class updatedbReduce extends TableReducer<LongWritable, Put, LongWritable>{
        public void reduce(LongWritable key, Put value, Context context) throws IOException, InterruptedException {
            context.write(key, value);
        }
    }

    public static void main(String[] args) throws Exception { 
        String inputDir = args[0];
        String outputTable = args[1];
        String timestamp  = args[2];
        String type = args[3];
        
        //JobConf conf = new JobConf(diffdb.class); 
        Configuration config = HBaseConfiguration.create();
        HBaseAdmin hbAdmin = new HBaseAdmin(config);
        dbutil db_util = new dbutil(config);
        
        config.set(TableOutputFormat.OUTPUT_TABLE, db_util.getRealName(outputTable));
        //config.set("outputTable", outputTable);
        config.set("timestamp", timestamp);
        config.set("classname", "org.diffdb." + type + "Entry");

        Class<?> ourClass = Class.forName(config.get("classname"));
        genericEntry ourEntry = (genericEntry) ourClass.newInstance();
        
        String [] regexes = ourEntry.getRegexes();
        config.set("start_regex", regexes[0]);
        config.set("end_regex", regexes[1]);
        
        Job job = new Job(config, "addDb");
        db_util.register_database(outputTable, true);
        db_util.register_database("files", true);
        db_util.register_database("db_updates", true);
        
        DatInputFormat.addInputPath(job, new Path(inputDir));
        //DatInputFormat.setMinInputSplitSize(job, 10000000);
        
        job.setMapperClass(updatedbMap.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Put.class);
        job.setInputFormatClass(DatInputFormat.class); 
        job.setOutputFormatClass(TableOutputFormat.class); 
        job.setJarByClass(updatedbMap.class);
        
        job.setReducerClass(updatedbReduce.class);
        
        HTable numReduceTableTesters = new HTable(config, db_util.getRealName(outputTable));
        int regions = numReduceTableTesters.getRegionsInfo().size();
        job.setNumReduceTasks(regions);
        job.setNumReduceTasks(0);
        
        System.out.println("Table: " + outputTable + "\n");
        job.waitForCompletion(true);
        Put file_put = db_util.getPut(outputTable);
        file_put.add("d".getBytes(), "source".getBytes(), outputTable.getBytes());
        db_util.doPut("files", file_put);
        
        Put update_put = db_util.getPut(outputTable);
        Date theTime = new Date();
        update_put.add("d".getBytes(), "update".getBytes(), new Long(timestamp), Long.toString(theTime.getTime()).getBytes());
        db_util.doPut("db_updates", update_put);
        // Move to base_path
        // Add to gepan_files
    }
}