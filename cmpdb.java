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
 
public class cmpdb{ 
    
    public static class cmpdbMap extends TableMapper <ImmutableBytesWritable, Put>{ 
        private HTable oldTable;
        
        public void setup(Context context) throws IOException{
            try {
                oldTable = new HTable(context.getConfiguration().get("oldInputTable"));
            } catch (Exception E) {
                System.out.println("Exception: " + E.toString());
            }
        }
        
        public void map(ImmutableBytesWritable key, Result value, Context context)throws IOException, InterruptedException { 
            
            uniprotEntry newEntry = new uniprotEntry(value);
            Get oldResultGet = new Get(value.getRow());
            Result cmpResult = oldTable.get(oldResultGet);
            uniprotEntry oldEntry;
            try {
                oldEntry = new uniprotEntry(cmpResult);
            } catch(Exception E) {
                oldEntry = new uniprotEntry();
            }
            Vector<String> result = newEntry.compare(oldEntry);
            if(result.size() > 0) {
                Put newPut = new Put(value.getRow());
                for(Enumeration resItems = result.elements(); resItems.hasMoreElements();) {
                    String newElement = (String)resItems.nextElement();
                    newPut.add("d".getBytes(), newElement.getBytes(), "1".getBytes());
                }

                try {
                    context.write(key, newPut);
                } catch(IOException e) {
                    System.out.println(newPut.toString());
                    System.out.println(e.toString());
                }
            }
        } 
    } 

    public static class cmpdbReduce extends TableReducer<LongWritable, Put, LongWritable>{
        public void reduce(LongWritable key, Put value, Context context) throws IOException, InterruptedException {
            try {
                context.write(key, value);
            } catch(IOException e) {
                System.out.println("HADOOP!");
            }
        }
    }

    public static void main(String[] args) throws Exception { 
        String inputTableNew = args[0];
        String inputTableOld = args[1];
        String outputTable = args[2];
        
        //JobConf conf = new JobConf(diffdb.class); 
        Configuration config = HBaseConfiguration.create();
        HBaseAdmin hbAdmin = new HBaseAdmin(config);
        if(!hbAdmin.tableExists(outputTable)){
            System.out.println("Table does not exist! Creating it");
            HTableDescriptor newTable = createTable(outputTable);
            hbAdmin.createTable(newTable);
        }
        config.set(TableOutputFormat.OUTPUT_TABLE, outputTable);
        config.set(TableInputFormat.INPUT_TABLE, inputTableNew);
        config.set("oldInputTable", inputTableOld);
        Job job = new Job(config, "cmpdb");
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Put.class);
        job.setInputFormatClass(TableInputFormat.class); 
        job.setOutputFormatClass(TableOutputFormat.class); 
        job.setJarByClass(cmpdbMap.class);
        job.setMapperClass(cmpdbMap.class);
        //job.setReducerClass(diffdbReduce.class);
        job.setNumReduceTasks(0);

        System.out.println("Doing the stuffz!!!\n");
        job.submit(); 
        System.out.println(job.getTrackingURL());
    }

    private static HTableDescriptor createTable(String tableName){
        try {
            HTableDescriptor htTable = new HTableDescriptor(tableName);
            HColumnDescriptor htCD = new HColumnDescriptor("d");
            System.out.println(htCD.toString());
            htTable.addFamily(htCD);
            System.out.println(htTable.toString());
            return htTable;
        } catch (Exception E) {
            System.out.println("Exception! " + E.toString());
            return null;
        }
    }
}