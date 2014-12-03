package org.gestore.hadoop;

import java.io.IOException; 
import java.util.*; 
 
import org.apache.hadoop.fs.*; 
import org.apache.hadoop.conf.*; 
import org.apache.hadoop.io.*; 
import org.apache.hadoop.mapreduce.*; 
import org.apache.hadoop.util.*; 
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.hbase.*;


public class DatInputFormat extends FileInputFormat<LongWritable, Text>  {
    public RecordReader<LongWritable, Text>  createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {  
        String startRegex = context.getConfiguration().get("start_regex");
        String endRegex = context.getConfiguration().get("end_regex");
        //return new LongRecordReader("^ID .*", "^//");
        return new LongRecordReader(startRegex, endRegex);
    }
}
