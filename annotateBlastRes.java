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
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
 
public class annotateBlastRes{ 
    
    public static class updatedbMap extends Mapper <LongWritable, Text, LongWritable, Text>{ 
        private HTable ourTable;
        private Long timestamp;
        
        public void setup(Context context) throws IOException{
            try {
                ourTable = new HTable(context.getConfiguration(), context.getConfiguration().get("database"));
                timestamp = new Long(context.getConfiguration().get("timestamp"));
            } catch (Exception E) {
                System.out.println("Exception: " + E.toString());
            }
        }
        
        public void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException { 
            blastoutputformat blast_result = new blastoutputformat(value);
            Get annotationGet = new Get(blast_result.db_row());
            annotationGet.setTimeRange(0, timestamp);
            Result db_result = ourTable.get(annotationGet);
            uniprotEntry annotation_data = new uniprotEntry(db_result);
            String resultString = blast_result.toString() + annotation_data.toString();
            context.write(key, new Text(resultString));
        } 
    }
    
    public static class updatedbReduce extends Reducer<LongWritable, Text, LongWritable, Text>{
        public void reduce(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            context.write(key, value);
        }
    }

    public static void main(String[] args) throws Exception { 
        String inputfile = args[0];
        String database = args[1];
        String timestamp = args[2];
        String outputfile = args[3];
        
        //JobConf conf = new JobConf(diffdb.class); 
        Configuration config = HBaseConfiguration.create();
        HBaseAdmin hbAdmin = new HBaseAdmin(config);
        
        config.set("database", database);
        config.set("timestamp", timestamp);
        
        dbutil db_util = new dbutil(config);
        
        
        Job job = new Job(config, "annotate");
        db_util.register_database(database, true);
        
        DatInputFormat.addInputPath(job, new Path(inputfile));
        DatInputFormat.setMinInputSplitSize(job, 100000000);
        
        job.setMapperClass(updatedbMap.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Put.class);
        job.setInputFormatClass(FileInputFormat.class); 
        job.setOutputFormatClass(FileOutputFormat.class); 
        job.setJarByClass(updatedbMap.class);
        
        job.setReducerClass(updatedbReduce.class);

        job.setNumReduceTasks(1);
        
        job.waitForCompletion(true);
        // Move to base_path
        // Add to gepan_files
    }
}