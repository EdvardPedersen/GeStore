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
 
public class diffdb extends Configured implements Tool{ 
    public static class diffdbMap extends Mapper <LongWritable, Text, LongWritable, Put>{ 
        // Input is key (offset), value (text entry), output is HFile
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException { 
            String line = value.toString();
            String[] lineSplits = line.split("\\n");
            Long tempLong = new Long(key.get());
            Integer offset = new Integer(tempLong.intValue());

            uniprotEntry entry = new uniprotEntry(offset);
            entry.addEntries(lineSplits);
            Put newPut = entry.getPut(new Long(0));

            try{ 
                context.write(key, newPut);
            } catch (NullPointerException N) {
                System.out.println("Error! " + N.toString());
                System.out.println(newPut.toString());
                System.out.println(key.toString());
            }
        } 
    } 

    public static class diffdbReduce extends TableReducer<LongWritable, Put, LongWritable>{
        public void reduce(LongWritable key, Put value, Context context) throws IOException, InterruptedException {
            context.write(key, value);
        }
    }

    public int run(String[] args) throws Exception {
        String inputDir = args[0];
        String outputTableName = args[1];
        //JobConf conf = new JobConf(diffdb.class); 
        Configuration config = HBaseConfiguration.create();
        HBaseAdmin hbAdmin = new HBaseAdmin(config);
        if(!hbAdmin.tableExists(outputTableName)){
            System.out.println("Table does not exist! Creating it");
            HTableDescriptor newTable = createTable(outputTableName);
            hbAdmin.createTable(newTable);
        }
        config.set(TableOutputFormat.OUTPUT_TABLE, outputTableName);
        Job job = new Job(config, "diffdb");
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Put.class);
        DatInputFormat.addInputPath(job, new Path(inputDir));
        DatInputFormat.setMinInputSplitSize(job, 100000000);
        job.setInputFormatClass(DatInputFormat.class); 
        job.setOutputFormatClass(TableOutputFormat.class); 
        job.setJarByClass(diffdbMap.class);
        job.setMapperClass(diffdbMap.class);
        //job.setReducerClass(diffdbReduce.class);
        job.setNumReduceTasks(0);

        System.out.println("Doing the stuffz!!!\n");
        job.submit(); 
        System.out.println(job.getTrackingURL());
        return 0;
    }

    public static void main(String[] args) throws Exception { 
        int result = ToolRunner.run(new Configuration(), new diffdb(), args);
        System.exit(result);
    }

    private static HTableDescriptor createTable(String tableName){
        try {
            HTableDescriptor htTable = new HTableDescriptor(tableName);
            HColumnDescriptor htCD = new HColumnDescriptor("d");
            htCD.setMaxVersions(0);
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