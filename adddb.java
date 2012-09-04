package org.diffdb;

import java.io.IOException; 
import java.util.*; 
import java.io.*; 
import java.lang.reflect.*;
 
import org.apache.hadoop.fs.*; 
import org.apache.hadoop.conf.*; 
import org.apache.hadoop.io.*; 
import org.apache.hadoop.mapreduce.*; 
import org.apache.hadoop.util.*; 
import org.apache.hadoop.util.ToolRunner;
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
 
public class adddb extends Configured implements Tool{ 
    
    public static enum ENTRY_COUNTER {
        DELETED,
        NEW,
        EXISTING
    }
    
    public static class updatedbMap extends Mapper <LongWritable, Text, LongWritable, Put>{ 
        private HTable outputTable;
        private Long timestamp;
        private Class<?> entryType;
        private Constructor<?> entryConstructor;
        
        public void setup(Context context) throws IOException{
            try {
                outputTable = new HTable(context.getConfiguration(), context.getConfiguration().get(TableOutputFormat.OUTPUT_TABLE));
                timestamp = new Long(context.getConfiguration().get("timestamp"));
                entryType = Class.forName(context.getConfiguration().get("classname"));
                Configuration test = new Configuration();
                entryConstructor = entryType.getDeclaredConstructor(test.getClass());
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
                newEntry = (genericEntry) entryConstructor.newInstance(context.getConfiguration());
                oldEntry = (genericEntry) entryConstructor.newInstance(context.getConfiguration());
                newerEntry = (genericEntry) entryConstructor.newInstance(context.getConfiguration());
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
                context.getCounter(ENTRY_COUNTER.NEW).increment(1);
                try{
                    oldEntry = (genericEntry) entryConstructor.newInstance(context.getConfiguration());
                }catch(Exception E) {
                    return;
                }
            }
            
            if(newerEntry == null) {
                try {
                    newerEntry = (genericEntry) entryConstructor.newInstance(context.getConfiguration());
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
            context.getCounter(ENTRY_COUNTER.EXISTING).increment(1);
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
        int result = ToolRunner.run(new Configuration(), new adddb(), args);
        return;
    }
    
    public int run(String[] args) throws Exception { 
        Configuration argConf = getConf();
        String inputDir = argConf.get("input");
        String outputTable = argConf.get("table");
        String timestamp  = argConf.get("timestamp");
        String type = argConf.get("type");
        String targetDir = argConf.get("target_dir", "") + timestamp;
        
        String tempHDFSPath = argConf.get("temp_hdfs_path", "/tmp/gestore/");
       
        String baseFile = "adddbFile";
        String dirFile = "adddbDirlist";
        
        //JobConf conf = new JobConf(diffdb.class); 
        Configuration config = HBaseConfiguration.create();
        HBaseAdmin hbAdmin = new HBaseAdmin(config);
        dbutil db_util = new dbutil(config);
        
        config.set(TableOutputFormat.OUTPUT_TABLE, db_util.getRealName(outputTable));
        //config.set("outputTable", outputTable);
        config.set("timestamp", timestamp);
        config.set("classname", "org.diffdb." + type + "Entry");
        
        //Enable profiling information
        config.setBoolean("mapred.task.profile", false);
        config.set("mapred.task.maxpmem", "8589934592");
        System.out.println(config.get("mapred.capacity-scheduler.task.limit.maxpmem"));
        System.out.println(config.get("mapred.capacity-scheduler.task.default-pmem-percentage-in-vmem"));
        System.out.println(config.get("mapred.task.default.maxvmem"));
        System.out.println(config.get("mapred.task.limit.maxvmem"));

        Class<?> ourClass = Class.forName(config.get("classname"));
        genericEntry ourEntry = (genericEntry) ourClass.newInstance();
        
        String [] regexes = ourEntry.getRegexes();
        config.set("start_regex", regexes[0]);
        config.set("end_regex", regexes[1]);
        
        config.set("mapred.job.map.memory.mb", "3072");
        config.set("mapred.job.reduce.memory.mb", "3072");
        config.setBoolean("mapreduce.job.maps.speculative.execution", false);
        
        Job job = new Job(config, "addDb_" + type + "_" + outputTable);
        db_util.register_database(outputTable, true);
        db_util.register_database("files", true);
        db_util.register_database("db_updates", true);
        
        FileSystem fs = FileSystem.get(config);
        FileSystem localfs = FileSystem.getLocal(config);
        Path tempFile = new Path(tempHDFSPath + baseFile);
        
        File target = new File(inputDir);
        
        if(target.isDirectory()) {
            fs.copyFromLocalFile(new Path(inputDir), new Path(tempHDFSPath));
            FileStatus [] files = fs.globStatus(new Path(tempHDFSPath + "/*"));
            List<String> filenames = getFilesAndChecksums(files, fs, timestamp, targetDir, tempHDFSPath);
            System.out.println(tempHDFSPath);
            FSDataOutputStream newFile = fs.create(new Path(tempHDFSPath + dirFile));
            for(String file : filenames) {
                String output = file + "\n";
                newFile.write(output.getBytes());
            }
            newFile.close();
            DatInputFormat.addInputPath(job, new Path(tempHDFSPath + dirFile));
        } else {
            fs.copyFromLocalFile(new Path(inputDir), tempFile);
            DatInputFormat.addInputPath(job, tempFile);
        }
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
        
        try {
            job.waitForCompletion(true);
        } catch (Exception E) {
            System.out.println("ERROR with the STUFFZ!!");
        }
        fs.delete(tempFile, true);
        Put file_put = db_util.getPut(outputTable);
        file_put.add("d".getBytes(), "source".getBytes(), type.getBytes());
        db_util.doPut("files", file_put);
        
        Put update_put = db_util.getPut(outputTable);
        Date theTime = new Date();
        update_put.add("d".getBytes(), "update".getBytes(), new Long(timestamp), Long.toString(theTime.getTime()).getBytes());
        
        Counters allCounter = job.getCounters();
        update_put.add("d".getBytes(), "entries".getBytes(), new Long(timestamp), Long.toString(allCounter.findCounter(ENTRY_COUNTER.EXISTING).getValue()).getBytes());
        db_util.doPut("db_updates", update_put);
        // Move to base_path
        // Add to gepan_files
        return 0;
    }
    
    private static List<String> getFilesAndChecksums(FileStatus [] targets, FileSystem fs, String timestamp, String targetDir, String basePath) {
        List<String> returnStrings = new LinkedList<String>();
        for(FileStatus currentTarget : targets) {
            if(currentTarget.isDir()) {
                try {
                    returnStrings.addAll(getFilesAndChecksums(fs.globStatus(currentTarget.getPath().suffix("/*")), fs, timestamp, targetDir, basePath));
                } catch (IOException E) {
                    System.out.println("Error parsing files: " + E.toString());
                    return null;
                }
            } else {
                try{
                    String append = currentTarget.getPath().toString() + "\t" + fs.getFileChecksum(currentTarget.getPath()).toString() + "\t" + timestamp + "\t" + targetDir + "\t" + basePath;
                    returnStrings.add(append);
                } catch (IOException E) {
                    System.out.println("Error parsing files: " + E.toString());
                    return null;
                }
            }
        }
        return returnStrings;
    }
}