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
    private static Long timestamp;
    private static Long lowTimestamp;
    private static Long entNum;
    
    public static enum ENTRY_COUNTER {
        ENTRIES,
        MODIFIED,
        NEW,
        DELETED,
        NON_MATCHING
    }
    
    public static class getfastaMap extends TableMapper<Text, Text>{ 
        private HTable inputTable;
        private String taxon;
        private String type;
        private Hashtable <String, String> delimiters;
        private Class<?> entryType;
        
        
        public void setup(Context context) throws IOException{
            try {
                inputTable = new HTable(context.getConfiguration(), context.getConfiguration().get(TableInputFormat.INPUT_TABLE));
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
                entNum = new Long(0);
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
                context.getCounter(ENTRY_COUNTER.DELETED).increment(1);
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
                    context.getCounter(ENTRY_COUNTER.NON_MATCHING).increment(1);
                    return;
                }
            }
            
            int sanity_level = entry.sanityCheck(type);
            
            String[] keyVal = {null, null};
            
            if(sanity_level == 0) {
                return;
            } else if(sanity_level == 1) {
                keyVal = entry.get(type, taxon);
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
                keyVal = completeEntry.get(type, taxon);
            }
            
            if(keyVal != null) {
                Text outKey = new Text(keyVal[0]);
                Text outValue = new Text(keyVal[1]);
                context.getCounter(ENTRY_COUNTER.ENTRIES).increment(1);
                entNum += 1;
                context.write(outKey, outValue);
            } else {
                System.out.println("Null values returned from get() in " + context.getConfiguration().get("classname") + ", unsupported format (" + type + ") specified?");
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
                    context.getCounter(ENTRY_COUNTER.DELETED).increment(1);
                    multiOut.write((Text)iter.next(), new Text(""), "deleted");
                }
            } else {
                Text textValue = value.iterator().next();
                context.getCounter(ENTRY_COUNTER.ENTRIES).increment(1);
                if(!textValue.toString().isEmpty()) {
                    multiOut.write(key, textValue, "existing");
                } else {
                    //multiOut.write(key, (Text)null, "existing");
                    multiOut.write((Text)null, key, "existing");
                }
                //context.write(key, value);
            }
        }
        
        
        public void cleanup(Context context) throws IOException, InterruptedException{
            Long timestamp_start = new Long(context.getConfiguration().get("low_timestamp"));
            Long timestamp_stop = new Long(context.getConfiguration().get("timestamp"));
            timestamp_stop += 1;
            String table_name = "gestore_db_updates";
            String runId = context.getConfiguration().get("run_id");
            String taskId = context.getConfiguration().get("task_id");
            if(runId == null)
                runId = "";
            if(taskId == null)
                taskId = "";
            String row_name = dbutil.getShortName(context.getConfiguration().get(TableInputFormat.INPUT_TABLE)) + runId + "_" + taskId;
            String family = "d";
            String column_name = "entries";
            
            String entries = Long.toString(context.getCounter(ENTRY_COUNTER.ENTRIES).getValue());
            String metadata = "# Metadata for getfasta\n";
            
            // Get the number of entries for the current database version
            HTable updates = new HTable(context.getConfiguration(), table_name);
            Get full_entries_get = new Get(row_name.getBytes());
            full_entries_get.setTimeRange(timestamp_start, timestamp_stop);
            full_entries_get.addColumn(family.getBytes(), column_name.getBytes());
            Result entries_res = updates.get(full_entries_get);
            System.out.println("Table: " + table_name + " row:" +  row_name + " column: " + column_name);
            KeyValue run_file_prev = entries_res.getColumnLatest(family.getBytes(), column_name.getBytes());
            String lastEntries = new String(run_file_prev.getValue());
            
            metadata = metadata + "TABLE_NAME\t" + table_name + "\n";
            metadata = metadata + "INC_ENTRIES\t" + entries + "\n";
            metadata = metadata + "FULL_ENTRIES\t" + lastEntries + "\n";
            metadata = metadata + "TYPE\t" + context.getConfiguration().get("type") + "\n";
            metadata = metadata + "TAXON\t" + context.getConfiguration().get("taxon") + "\n";
            metadata = metadata + "CLASSNAME\t" + context.getConfiguration().get("classname") + "\n";
            multiOut.write((Text)null, new Text(metadata), "metadata");
            //Long.toString(allCounter.findCounter(ENTRY_COUNTER.EXISTING).getValue());
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
        
        String runId = argConf.get("run_id", "");
        String taskId = argConf.get("task_id", "");
        String database = argConf.get("database", "n");
        
        String startRow = "";
        String endRow = "";
        
        if(!runId.isEmpty() && database.equals("n")) {
            startRow = runId;
            endRow = Integer.toString(1 + new Integer(runId));
        }
        if(!taskId.isEmpty() && database.equals("n")) {
	    endRow = startRow + "_" + Integer.toString(1 + new Integer(taskId));
            startRow = startRow + "_" + taskId;
        }
        
        System.out.println(outputFile);
        
        Configuration config = HBaseConfiguration.create();
        config.set("mapred.textoutputformat.separator","\n");
        config.set("timestamp", timestampStop);
        config.set("low_timestamp", timestampStart);
        config.set("delimiter", delimiter);
        config.set("taxon", taxon);
        config.set("type", type);
        config.set("classname", "org.diffdb." + className + "Entry");
        
        config.set("mapred.job.map.memory.mb", "3072");
        config.set("mapred.job.reduce.memory.mb", "3072");
        
        if(database.equals("n")) {
	  config.set("run_id", runId);
	  config.set("task_id", taskId);
	}
        
        dbutil db_util = new dbutil(config);
	config.setBoolean("mapred.task.profile", true);
        Job job = new Job(config, "getfasta_" + className + "_" + inputTableS);
        MultipleOutputs.setCountersEnabled(job, true);
        System.out.println("Type: " + type);
        System.out.println("Taxon: " + taxon);
        System.out.println("Classname: " + className);
        System.out.println("Input table: " + inputTableS);
        System.out.println("Start row: " + startRow);
        System.out.println("End row: " + endRow);
        HBaseAdmin hbAdmin = new HBaseAdmin(config);
        if(!hbAdmin.tableExists(inputTableS)){
            System.out.println("NO TABLE!");
        }
        
        Scan ourScan = new Scan();
        ourScan.setCaching(100);
        ourScan.setCacheBlocks(false);
        if(!startRow.isEmpty() && !endRow.isEmpty()) {
            ourScan.setStartRow(startRow.getBytes());
            ourScan.setStopRow(endRow.getBytes());
        }
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
