package org.diffdb;

import java.io.IOException; 
import java.util.*;
import java.util.regex.*; 
 
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
 
public class getfasta extends Configured implements Tool{ 
    public static class getfastaMap extends TableMapper<Text, Text>{ 
        private HTable inputTable;
        private Long timestamp;
        private Long lowTimestamp;
        private String taxon;
        private String type;
        private Hashtable <String, String> delimiters;
        private Class<?> entryType;
        
        
        public void setup(Context context) throws IOException{
            try {
                inputTable = new HTable(context.getConfiguration().get(TableInputFormat.INPUT_TABLE));
                timestamp = new Long(context.getConfiguration().get("timestamp"));
                lowTimestamp = new Long(context.getConfiguration().get("low_timestamp"));
                timestamp += 1;
                taxon = context.getConfiguration().get("taxon");
                type = context.getConfiguration().get("type");
                String [] entries = context.getConfiguration().get("delimiter").trim().split(",");
                System.out.println("Timestamp_start: " + context.getConfiguration().get("low_timestamp") + " Timestamp_stop: " + context.getConfiguration().get("timestamp"));
                delimiters = new Hashtable<String,String>();
                for(String entry : entries) {
                    String [] delimiter_parts = entry.split("=");
                    System.out.println("Data: " + delimiter_parts[0] + " Regex: \"" + delimiter_parts[1] + "\"");
                    delimiters.put(delimiter_parts[0], delimiter_parts[1]);
                }
                entryType = Class.forName(context.getConfiguration().get("classname"));
            } catch (Exception E) {
                System.out.println("Exception: " + E.toString());
                E.printStackTrace();
            }
        }
        
        public void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException { 
            genericEntry entry;
            try{
                entry = (genericEntry) entryType.newInstance();
            } catch(Exception E) {
                System.out.println("Aborting (at creation), exception: " + E.toString());
                System.out.println("Class name: " + context.getConfiguration().get("classname"));
                System.out.println("Class: " + entryType.toString());
                return;
            }
            
            if(!entry.addEntry(value)) {
                System.out.println("Error parsing entry: " + value.toString());
            }
            Long exists = new Long(entry.getTableEntry("EXISTS"));
            if(null == exists) {
                return;
            } else if(exists < (timestamp - 1)) {
                Text tesText = new Text(value.getRow());
                Text retText = new Text("DELETED");
                context.write(retText, tesText);
                return;
            }
            
            for(Enumeration field = delimiters.keys(); field.hasMoreElements(); ) {
                String delim_field = (String)field.nextElement();
                String match_entry = entry.getTableEntry(delim_field);
                if(null == match_entry) {
                    return;
                }
                Pattern delimiter_regex = Pattern.compile(delimiters.get(delim_field), Pattern.CASE_INSENSITIVE + Pattern.DOTALL);
                Matcher delimiter_matcher = delimiter_regex.matcher(match_entry);
                //delim_value.matches(delim_regex)
                if(!delimiter_matcher.matches()) {
                    return;
                }
            }
            
            int sanity_level = entry.sanityCheck(type);
            
            if(sanity_level == 0) {
                return;
            } else if(sanity_level == 1) {
                String[] keyVal = entry.get(type, taxon);
                Text outKey = new Text(keyVal[0]);
                Text outValue = new Text(keyVal[1]);
                context.write(outKey, outValue);
            } else {
                Get resultGet = new Get(value.getRow());
                resultGet.setTimeRange(new Long(0), timestamp);
                Result newResult = inputTable.get(resultGet);
                genericEntry completeEntry;
                try{
                    completeEntry = (genericEntry) entryType.newInstance();
                    completeEntry.addEntry(newResult);
                } catch (Exception E) {
                    System.out.println("Aborting (at complete entry), exception: " + E.toString());
                    return;
                }
                String[] keyVal = completeEntry.get(type, taxon);
                Text outKey = new Text(keyVal[0]);
                Text outValue = new Text(keyVal[1]);
                context.write(outKey, outValue);
            }
        }
    } 

    public static class getfastaReduce extends Reducer<Text, Text, Text, Text>{
        private MultipleOutputs<Text, Text> multiOut;
        
        public void setup(Context context) {
            multiOut = new MultipleOutputs<Text, Text>(context);
        }
        
        public void reduce(Text key, Iterable<Text> value, Context context) throws IOException, InterruptedException {
            if(key.toString().equals("DELETED")) {
                System.out.println("DELETED");
                Iterator iter = value.iterator();
                while(iter.hasNext()){
                    multiOut.write((Text)iter.next(), new Text(""), "deleted");
                }
            } else {
                multiOut.write(key, value.iterator().next(), "existing");
                //context.write(key, value);
            }
        }
        
        public void cleanup(Context context) {
            try{
                multiOut.close();
            } catch (Exception E) {
                return;
            }
        }
    }

    // required -Ds:
    // -D input_table=something
    // -D output_file=optional output file
    // -D timestamp_start=optional start time
    // -D timestamp_stop=optional end time
    // -D regex=OC=*.bacteria*.,ID=*.MOUSE*.
    // -D addendum=optional addendum in fasta file

    public static void main(String[] args) throws Exception { 
        for(String line : args) {
            System.out.println(line);
        }
        int result = ToolRunner.run(new Configuration(), new getfasta(), args);
        return;
    }

    public int run(String[] args) throws Exception {
        Configuration argConf = getConf();
        String inputTableS = argConf.get("input_table");
        String outputFile = argConf.get("output_file", "temp");
        String timestampStart = argConf.get("timestamp_start", "0");
        String timestampStop = argConf.get("timestamp_stop", Integer.toString(Integer.MAX_VALUE));
        String delimiter = argConf.get("regex", "ID=.*");
        String taxon = argConf.get("addendum", "");
        String type = argConf.get("type");
        String className = argConf.get("classname", "uniprot");
        
        System.out.println(outputFile);
        
        Configuration config = HBaseConfiguration.create();
        config.set("mapred.textoutputformat.separator","\n");
        config.set("timestamp", timestampStop);
        config.set("low_timestamp", timestampStart);
        config.set("delimiter", delimiter);
        config.set("taxon", taxon);
        config.set("type", type);
        config.set("classname", "org.diffdb." + className + "Entry");
        
        dbutil db_util = new dbutil(config);
        Job job = new Job(config, "getfasta");
        HBaseAdmin hbAdmin = new HBaseAdmin(config);
        if(!hbAdmin.tableExists(inputTableS)){
            System.out.println("NO TABLE!");
        }
        
        Scan ourScan = new Scan();
        ourScan.setCaching(500);
        ourScan.setCacheBlocks(false);
        Long timestampS = new Long(timestampStart);
        Long timestampE = new Long(timestampStop);
        timestampE += 1;
        ourScan.setTimeRange(timestampS, timestampE);
        
        job.setJarByClass(getfastaMap.class);
        
        TableMapReduceUtil.initTableMapperJob(db_util.getRealName(inputTableS), ourScan, getfastaMap.class, Text.class, Text.class, job);
        job.setReducerClass(getfastaReduce.class);
        job.setNumReduceTasks(1);

        TextOutputFormat.setOutputPath(job, new Path(outputFile));
        //job.setOutputFormatClass(TextOutputFormat.class); 
        MultipleOutputs.addNamedOutput(job, "existing", FileOutputFormat.class, Text.class, Text.class);
        MultipleOutputs.addNamedOutput(job, "deleted", FileOutputFormat.class, Text.class, Text.class);
        job.waitForCompletion(true); 
        return 0;
    }
}