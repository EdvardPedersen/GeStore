package org.diffdb;

import java.io.IOException; 
import java.util.*; 
import java.lang.reflect.*;

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
import java.text.*;
 
/* TODO:
 * 
 * - Move setup out of run
 * - Refactor to reduce clutter
 * - Sanitize timestamp parsing to move back to arbitrary timestamp support
 * - Call adddb when format is supported
 * - Add command-line arguments to get system status
 * - add wildcard support?
 * 
 */

public class move extends Configured implements Tool{ 
    public static enum toFrom { LOCAL2REMOTE, REMOTE2LOCAL, LOCAL2LOCAL, REMOTE2REMOTE, ERROR}
    
    public static void main(String[] args) throws Exception { 
        int result = ToolRunner.run(new Configuration(), new move(), args);
        System.exit(result);
    }

    public int run(String[] args) throws Exception { 
        Hashtable<String,String> confArg = new Hashtable<String,String>();
        setup(confArg);
        
        //Get type of movement
        toFrom type_move = checkArgs(confArg);
        
        //Database registration
        Configuration config = HBaseConfiguration.create();
        dbutil db_util = new dbutil(config);
        db_util.register_database(confArg.get("db_name_files"), true);
        db_util.register_database(confArg.get("db_name_runs"), true);
        db_util.register_database(confArg.get("db_name_updates"), true);
        FileSystem hdfs = FileSystem.get(config);
        
        //Get source type
        confArg.put("source", getSource(db_util, confArg.get("db_name_files"), confArg.get("file_id")));
        if(!confArg.get("source").equals("local") && type_move==toFrom.REMOTE2LOCAL && !confArg.get("timestamp_stop").equals(getUnixTime("999920"))) {
            confArg.put("timestamp_stop", getUnixTime(latestVersion(confArg, db_util).toString()));
        }
        
        // Get previous timestamp
        Get run_id_get = new Get(confArg.get("run_id").getBytes());
        Result run_get = db_util.doGet(confArg.get("db_name_runs"), run_id_get);
        KeyValue run_file_prev = run_get.getColumnLatest("d".getBytes(), confArg.get("file_id").getBytes());
        String last_timestamp = new String("0");
        if(null != run_file_prev && !confArg.get("source").equals("local")) {
            last_timestamp = new String(Long.toString(run_file_prev.getTimestamp()));
            Date last_run = new Date(run_file_prev.getTimestamp());
            if(last_run.after(endDate) || last_run.equals(endDate)) {
                System.out.println("Last run was done after timestamp_stop (" + Long.toString(endDate.getTime()) + " vs. " + Long.toString(last_run.getTime()) + ", should have file waiting!");
            } else {
                System.out.println("Updates have (possibly) been made since last update, should generate intermediate file...");
                if(!full_run) {
                    confArg.put("timestamp_start", getUnixTime(last_timestamp));
                    System.out.println("Timestamp start: " + confArg.get("timestamp_start"));
                }
            }
        }
        
        String file_name_short = confArg.get("file_id") + "_" + confArg.get("timestamp_start") + "_" + confArg.get("timestamp_stop");
        String filenames_put = new String();
        
        Get file_id_get = new Get(confArg.get("file_id").getBytes());
        Result file_get = db_util.doGet(confArg.get("db_name_files"), file_id_get);
        //String source = "local";
        if(!file_get.isEmpty()) {
            boolean found = hasFile(db_util, hdfs, confArg.get("db_name_files"), confArg.get("file_id"), getFullPath(confArg));
            filenames_put = getFileNames(db_util, confArg.get("db_name_files"), confArg.get("file_id"), getFullPath(confArg));
            // Filename not found in file database
            if (!found){
                // Moving from remote to local
                if(type_move == toFrom.REMOTE2LOCAL) {
                    if(confArg.get("source").equals("local")) {
                        System.out.println("Remote file not found, but other versions exist");
                    } else {
                        // Generate intermediate file
                        System.out.println("getting file...");
                        getFile(hdfs, confArg, db_util);
                        // Put generated file into file database
                        putFileEntry(db_util, hdfs, confArg.get("db_name_files"), confArg.get("file_id"), confArg.get("full_file_name"), confArg.get("source"));
                    }
                } else if (type_move == toFrom.LOCAL2REMOTE) {
                    // Moving from local to remote, no remote file found with same name
                }
            } else if (found) {
                System.out.println("Found remote file!");
            }
        } else {
            if(type_move == toFrom.REMOTE2LOCAL) {
                System.out.println("ERROR: Remote file not found, and cannot be generated!");
                System.out.println("config: " + confArg.toString() );
                return 1;
            }
        }
        
        if(type_move == toFrom.LOCAL2REMOTE) {
            putFileEntry(db_util, hdfs, confArg.get("db_name_files"), confArg.get("file_id"), getFullPath(confArg), confArg.get("source"));
            putRunEntry(db_util, confArg.get("db_name_runs"), confArg.get("run_id"), confArg.get("file_id"), confArg.get("type"), confArg.get("timestamp_real"));
            hdfs.copyFromLocalFile(new Path(confArg.get("local_path")), new Path(getFullPath(confArg)));
        } else if(type_move == toFrom.REMOTE2LOCAL) {
            FileStatus[] files = hdfs.globStatus(new Path(getFullPath(confArg) + "*"));
            putRunEntry(db_util, confArg.get("db_name_runs"), confArg.get("run_id"), confArg.get("file_id"), confArg.get("type"), confArg.get("timestamp_real"));
            for(FileStatus file : files) {
                Path cur_file = file.getPath();
                Path cur_local_path = new Path(new String(confArg.get("local_path") + confArg.get("file_id")));
                String suffix = getSuffix(getFileName(confArg), cur_file.getName());
                if(suffix.length() > 0) {
                    cur_local_path = cur_local_path.suffix(new String("." + suffix));
                }
                System.out.println("Local file: " + cur_local_path.toString());
                System.out.println("Suffix: " + suffix);
                hdfs.copyToLocalFile(cur_file, cur_local_path);
            }
        }
        return 0;
    }

    private static boolean setup(Hashtable curConf) {
        Configuration argConf = getConf();
        
        //Input paramaters
        confArg.put("run_id", argConf.get("run", "1"));
        confArg.put("file_id", argConf.get("file"));
        confArg.put("local_path", argConf.get("path", ""));
        confArg.put("type", argConf.get("type", "l2r"));
        confArg.put("timestamp_start", argConf.get("timestamp_start", "197001"));
        confArg.put("timestamp_stop", argConf.get("timestamp_stop", "999920"));
        confArg.put("delimiter", argConf.get("regex", "ID=.*"));
        confArg.put("taxon", argConf.get("taxon", "all"));
        confArg.put("intermediate", argConf.get("full_run", "false"));
        Boolean full_run = confArg.get("intermediate").matches("(?i).*true.*");
        
        //Constants
        confArg.put("base_path", "/user/epe005/files/");
        confArg.put("db_name_files", "files");
        confArg.put("db_name_runs", "runs");
        confArg.put("db_name_updates", "db_updates");

        //Timestamps
        Date currentTime = new Date();
        Date endDate = new Date(new Long(getUnixTime(confArg.get("timestamp_stop"))));
        confArg.put("timestamp_real", Long.toString(currentTime.getTime()));
        confArg.put("timestamp_start", getUnixTime(confArg.get("timestamp_start")));
        confArg.put("timestamp_stop", getUnixTime(confArg.get("timestamp_stop")));
        return true;
    }

    private static toFrom checkArgs(Hashtable config) {
        if(config.get("type").equals("l2r")) {
            return toFrom.LOCAL2REMOTE;
        } else if(config.get("type").equals("r2l")) {
            return toFrom.REMOTE2LOCAL;
        } else {
            return toFrom.ERROR;
        }
    }
    
    private static String getSuffix(String base_file, String extended_file){
        String retString = "";
        try {
            retString = extended_file.substring(base_file.length() + 1);
        } catch (IndexOutOfBoundsException E){
            System.out.println("WARNING: getSuffix() used with incompatable paramaters!");
            System.out.println("base_file = " + base_file);
            System.out.println("extended_file = " + extended_file);
            retString = "";
        }
        return retString;
    }
    
    // Information needed to get a single file:
    // BASE_PATH, FILE_ID, TIMESTAMP_START, TIMESTAMP_STOP, SOURCE, FILESYSTEM
    private static Vector<Path> getFile(FileSystem fs, Hashtable<String, String> config, dbutil db_util) throws Exception {
        Long latestVersion = latestVersion(config, db_util);
                
        config.put("timestamp_start", config.get("timestamp_start"));
        config.put("timestamp_real", getUnixTime(latestVersion.toString()));
        config.put("timestamp_stop", getUnixTime(latestVersion.toString()));
                
        System.out.println("Getting DB for timestamp: " + config.get("timestamp_start") + " to " + config.get("timestamp_stop"));
        
        String final_result = getFullPath(config);

        String temp_path_base = "/tmp/gestore/";
        Path newPath = new Path(final_result);
        Vector<Path> ret_path = new Vector<Path>();
        if(fs.exists(newPath)) {
            ret_path.add(newPath);
            return ret_path;
        } else {
            if(!config.get("source").equals("local")) {
                config.put("temp_path_base", temp_path_base);
                
                config.put("timestamp_start", getShortTime(config.get("timestamp_start")));
                config.put("timestamp_real", latestVersion.toString());
                config.put("timestamp_stop", latestVersion.toString());
                
                Class<?> sourceClass = Class.forName("org.diffdb." + config.get("source"));
                Method process_data = sourceClass.getMethod("process", Hashtable.class, FileSystem.class);
                Object processor = sourceClass.newInstance();
                Object retVal;
                try{
                    retVal = process_data.invoke(processor, config, fs);
                } catch (InvocationTargetException E) {
                    Throwable exception = E.getTargetException();
                    System.out.println("Exception: " + exception.toString() + "Stacktrace: ");
                    exception.printStackTrace(System.out);
                    return null;
                }
                FileStatus [] files = (FileStatus [])retVal;
                
                config.put("timestamp_start", getUnixTime(config.get("timestamp_start")));
                config.put("timestamp_real", getUnixTime(latestVersion.toString()));
                config.put("timestamp_stop", getUnixTime(latestVersion.toString()));
                
                for(FileStatus file : files) {
                    Path cur_file = file.getPath();
                    Path cur_local_path = new Path(temp_path_base + config.get("file_id"));
                    String suffix = getSuffix(config.get("file_id"), cur_file.getName());
                    cur_local_path = cur_local_path.suffix(suffix);
                    Path res_path = new Path(new String(final_result + suffix));
                    System.out.println("Moving file " + cur_file.toString() + " to " + res_path.toString());
                    fs.moveFromLocalFile(cur_file, res_path);
                }

                config.put("full_file_name", final_result);
            }
        }
        return ret_path;
    }
    
    private static Long latestVersion(Hashtable<String, String> config, dbutil db_util) throws Exception{
        if(!getShortTime(config.get("timestamp_stop")).equals("999920")) {
            return new Long(getShortTime(config.get("timestamp_stop")));
        }
        Get timestampGet = new Get(config.get("source").getBytes());
        timestampGet.addColumn("d".getBytes(), "update".getBytes());
        Result timestampResult = db_util.doGet(config.get("db_name_updates"), timestampGet);
        KeyValue tsKv = timestampResult.getColumnLatest("d".getBytes(), "update".getBytes());
        Long latestVersion = new Long(tsKv.getTimestamp());
        return latestVersion;
    }
    
    private static boolean putFileEntry(dbutil db_util, FileSystem fs, String db_name, String file_id, String file_path, String source) throws Exception{
        String all_paths = file_path;
        if(hasFile(db_util, fs, db_name, file_id, file_path)) {
            System.out.println("File already found! PutFileEntry aborting...");
            return false;
        } else {
            Get file_id_get = new Get(file_id.getBytes());
            Result file_result = db_util.doGet(db_name, file_id_get);
            KeyValue file_names = file_result.getColumnLatest("d".getBytes(), "filenames".getBytes());
            if(file_names != null) {
                String paths = new String(file_names.getValue());
                all_paths = paths + "\n" + file_path;
            }
        }
        Put file_id_put = new Put(file_id.getBytes());
        file_id_put.add("d".getBytes(), "source".getBytes(), source.getBytes());
        file_id_put.add("d".getBytes(), "filenames".getBytes(), all_paths.getBytes());
        db_util.doPut(db_name, file_id_put);
        return true;
    }
    
    private static boolean hasFile(dbutil db_util, FileSystem fs, String db_name, String file_id, String file_path) throws Exception {
        Get file_id_get = new Get(file_id.getBytes());
        Result file_result = db_util.doGet(db_name, file_id_get);
        
        KeyValue file_names = file_result.getColumnLatest("d".getBytes(), "filenames".getBytes());
        if(file_names == null) {
            return false;
        }
        String all_files = new String(file_names.getValue());
        String[] files = all_files.split("\n");
        System.out.println("Searching file table for row " + file_id + " with value " + file_path);
        for(String line : files) {
            if(line.equals(file_path)) {
                System.out.println("Match found! Should not need recomputation!");
                if(fs.globStatus(new Path(line + "*")).length == 0) {
                    System.out.println("False match, file does not exist!");
                    Put new_put = new Put(file_id.getBytes());
                    new_put.add("d".getBytes(), "filenames".getBytes(), all_files.replace(file_path + "\n", "").getBytes());
                    db_util.doPut(db_name, new_put);
                    return false;
                }
                return true;
            }
        }
        return false;
    }
    
    private static String getFullPath(Hashtable<String, String> config) {
        return new String(config.get("base_path") + "_" + getFileName(config));
    }
    
    private static String getFileNames(dbutil db_util, String db_name, String file_id, String file_path) throws Exception {
        Get file_id_get = new Get(file_id.getBytes());
        Result file_result = db_util.doGet(db_name, file_id_get);
        
        KeyValue file_names = file_result.getColumnLatest("d".getBytes(), "filenames".getBytes());
        if(file_names == null) {
            return "";
        }
        String all_files = new String(file_names.getValue());
        String[] files = all_files.split("\n");
        for(String line : files) {
            if(line.equals(file_path)) {
                return all_files;
            }
        }
        return all_files + "\n" + file_path;
    }
    
    private static String getSource(dbutil db_util, String db_name, String file_id) throws Exception {
        Get file_id_get = new Get(file_id.getBytes());
        Result file_result = db_util.doGet(db_name, file_id_get);
        KeyValue file_source = file_result.getColumnLatest("d".getBytes(), "source".getBytes());
        if(file_source == null) {
            return "local";
        }
        return new String(file_source.getValue());
    }
    
    private static boolean putRunEntry(dbutil db_util, String db_name, String run_id, String file_id, String type, String timestamp) throws Exception {
        Put run_id_put = new Put(run_id.getBytes());
        run_id_put.add("d".getBytes(), file_id.getBytes(), new Long(timestamp), type.getBytes());
        db_util.doPut(db_name, run_id_put);
        return true;
    }
    
    private static String getFileName(Hashtable<String, String> config) {
        String retString =      config.get("file_id") + "_" + config.get("timestamp_start") + "_" + config.get("timestamp_stop") + "_" +
                                config.get("delimiter").hashCode() + "_" + config.get("taxon");
        if(!config.get("source").equals("local")) {
            return retString;
        } else {
            return retString + "_" + config.get("run_id");
        }
    }
    
    private static String getUnixTime(String short_time) {
        SimpleDateFormat converter = new SimpleDateFormat("yyyyMM");
        Date startDate = converter.parse(short_time, new ParsePosition(0));
        return Long.toString(startDate.getTime());
    }
    
    private static String getShortTime(String unix_time) {
        SimpleDateFormat converter = new SimpleDateFormat("yyyyMM");
        Date formatDate = new Date(new Long(unix_time));
        StringBuffer retString = new StringBuffer();
        StringBuffer startDate = converter.format(formatDate, retString, new FieldPosition(0));
        return retString.toString();
    }
}
