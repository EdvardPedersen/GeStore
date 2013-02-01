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
import org.apache.hadoop.hbase.zookeeper.*;
import org.apache.zookeeper.data.*;
//import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;

public class move extends Configured implements Tool{ 
    static ZooKeeper zkInstance;
    static ZooKeeperWatcher zkWatcher;
    public static enum toFrom { LOCAL2REMOTE, REMOTE2LOCAL, LOCAL2LOCAL, REMOTE2REMOTE, ERROR}
    
    public static void main(String[] args) throws Exception { 
        int result = ToolRunner.run(new Configuration(), new move(), args);
        System.exit(result);
    }

    public int run(String[] args) throws Exception { 
        //printUsage();
        /*
         * SETUP
         */
        Configuration argConf = getConf();
        Hashtable<String,String> confArg = new Hashtable<String,String>();
        setup(confArg, argConf);
        Date currentTime = new Date();
        Date endDate = new Date(new Long(confArg.get("timestamp_stop")));
        Boolean full_run = confArg.get("intermediate").matches("(?i).*true.*");
        
        //ZooKeeper setup
        Configuration config = HBaseConfiguration.create();
        zkWatcher = new ZooKeeperWatcher(config, "Testing", new HBaseAdmin(config));
        zkInstance = new ZooKeeper(ZKConfig.getZKQuorumServersString(config), config.getInt("zookeeper.session.timeout", -1), zkWatcher);
        
        //Get type of movement
        toFrom type_move = checkArgs(confArg);
        if(type_move == toFrom.LOCAL2REMOTE && !confArg.get("format").equals("unknown")) {
            List<String> arguments = new ArrayList<String>();
            arguments.add("-Dinput=" + confArg.get("local_path"));
            arguments.add("-Dtable=" + confArg.get("file_id"));
            arguments.add("-Dtimestamp=" + confArg.get("timestamp_stop"));
            arguments.add("-Dtype=" + confArg.get("format"));
            arguments.add("-Dtarget_dir=" + confArg.get("base_path") + "_" + confArg.get("file_id"));
            arguments.add("-Dtemp_hdfs_path=" + confArg.get("temp_path"));
            arguments.add("-Drun_id=" + confArg.get("run_id"));
            if(!confArg.get("run_id").isEmpty())
                arguments.add("-Drun_id=" +confArg.get("run_id"));
            if(!confArg.get("task_id").isEmpty())
                arguments.add("-Dtask_id=" + confArg.get("task_id"));
            String lockName = lock(confArg.get("file_id"));
            String [] argumentString = arguments.toArray(new String [arguments.size()]);
            adddb.main(argumentString);
            unlock(lockName);
            System.exit(0);
        }
        
        //Database registration
        
        dbutil db_util = new dbutil(config);
        db_util.register_database(confArg.get("db_name_files"), true);
        db_util.register_database(confArg.get("db_name_runs"), true);
        db_util.register_database(confArg.get("db_name_updates"), true);
        FileSystem hdfs = FileSystem.get(config);
        
        
        
        //Get source type
        confArg.put("source", getSource(db_util, confArg.get("db_name_files"), confArg.get("file_id")));
        confArg.put("database", isDatabase(db_util, confArg.get("db_name_files"), confArg.get("file_id")));
        if(!confArg.get("source").equals("local") && type_move==toFrom.REMOTE2LOCAL && !confArg.get("timestamp_stop").equals(Integer.toString(Integer.MAX_VALUE))) {
            confArg.put("timestamp_stop", Long.toString(latestVersion(confArg, db_util)));
        }
        
        /*
         * Get previous timestamp
         */
        Get run_id_get = new Get(confArg.get("run_id").getBytes());
        Result run_get = db_util.doGet(confArg.get("db_name_runs"), run_id_get);
        KeyValue run_file_prev = run_get.getColumnLatest("d".getBytes(), (confArg.get("file_id") + "_db_timestamp").getBytes());
        String last_timestamp = new String("0");
        if(null != run_file_prev && !confArg.get("source").equals("local")) {
            last_timestamp = new String(run_file_prev.getValue());
            Integer lastTimestamp = new Integer(last_timestamp);
            lastTimestamp += 1;
            last_timestamp = lastTimestamp.toString();
            System.out.println("Last timestamp: " + last_timestamp + "End date:" + endDate);
            Date last_run = new Date(run_file_prev.getTimestamp());
            if(last_run.before(endDate) && !full_run) {
                confArg.put("timestamp_start", last_timestamp);
            }
        }
        
        /*
         * Generate file
         */
        
        Get file_id_get = new Get(confArg.get("file_id").getBytes());
        Result file_get = db_util.doGet(confArg.get("db_name_files"), file_id_get);
        if(!file_get.isEmpty()) {
            boolean found = hasFile(db_util, hdfs, confArg.get("db_name_files"), confArg.get("file_id"), getFullPath(confArg));
            String filenames_put = getFileNames(db_util, confArg.get("db_name_files"), confArg.get("file_id"), getFullPath(confArg));
            // Filename not found in file database
            if (!found && type_move == toFrom.REMOTE2LOCAL){
                    if(!confArg.get("source").equals("local")) {
                        // Generate intermediate file
                        getFile(hdfs, confArg, db_util);
                        // Put generated file into file database
                        putFileEntry(db_util, hdfs, confArg.get("db_name_files"), confArg.get("file_id"), confArg.get("full_file_name"), confArg.get("source"));
                    } else {
                        System.err.println("ERROR: Remote file not found, and cannot be generated!");
                        return 1;
                    }
            }
        } else {
            if(type_move == toFrom.REMOTE2LOCAL) {
                System.err.println("ERROR: Remote file not found, and cannot be generated!");
                System.err.println("config: " + confArg.toString() );
                return 1;
            }
        }
        
        /*
         * Copy file
         * Update tables
         */
        
        if(type_move == toFrom.LOCAL2REMOTE) {
            putFileEntry(db_util, hdfs, confArg.get("db_name_files"), confArg.get("file_id"), getFullPath(confArg), confArg.get("source"));
            putRunEntry(db_util, confArg.get("db_name_runs"), confArg.get("run_id"), confArg.get("file_id"), confArg.get("type"), confArg.get("timestamp_real"), confArg.get("timestamp_stop"), getFullPath(confArg), confArg.get("delimiter"));
            hdfs.copyFromLocalFile(new Path(confArg.get("local_path")), new Path(getFullPath(confArg)));
        } else if(type_move == toFrom.REMOTE2LOCAL) {
            FileStatus[] files = hdfs.globStatus(new Path(getFullPath(confArg) + "*"));
            putRunEntry(db_util, confArg.get("db_name_runs"), confArg.get("run_id"), confArg.get("file_id"), confArg.get("type"), confArg.get("timestamp_real"), confArg.get("timestamp_stop"), getFullPath(confArg), confArg.get("delimiter"));
            for(FileStatus file : files) {
                Path cur_file = file.getPath();
                Path cur_local_path = new Path(new String(confArg.get("local_path") + confArg.get("file_id")));
                String suffix = getSuffix(getFileName(confArg), cur_file.getName());
                if(suffix.length() > 0) {
                    cur_local_path = cur_local_path.suffix(new String("." + suffix));
                }
                hdfs.copyToLocalFile(cur_file, cur_local_path);
            }
        }
        return 0;
    }

    /**
    * Sets up configuration based on params
    */
    private static boolean setup(Hashtable<String, String> curConf, Configuration argConf) {
        
        if(argConf.get("file") == null) {
            System.err.println("Missing file paramater!");
            System.exit(1);
        }
        
        if(argConf.get("hdfs_base_path") == null) {
            System.err.println("Missing HDFS base path, check gestore-conf.xml");
            System.exit(1);
        }
        
        if(argConf.get("hdfs_temp_path") == null) {
            System.err.println("Missing HDFS temp path, check gestore-conf.xml");
            System.exit(1);
        }
        
        if(argConf.get("local_temp_path") == null) {
            System.err.println("Missing local_temp_path, check gestore-conf.xml");
            System.exit(1);
        }
        
        //Input paramaters
        curConf.put("run_id", argConf.get("run", ""));
        curConf.put("task_id", argConf.get("task", ""));
        curConf.put("file_id", argConf.get("file"));
        curConf.put("local_path", argConf.get("path", ""));
        curConf.put("type", argConf.get("type", "l2r"));
        curConf.put("timestamp_start", argConf.get("timestamp_start", "0"));
        curConf.put("timestamp_stop", argConf.get("timestamp_stop", Integer.toString(Integer.MAX_VALUE)));
        curConf.put("delimiter", argConf.get("regex", "ID=.*"));
        curConf.put("taxon", argConf.get("taxon", "all"));
        curConf.put("intermediate", argConf.get("full_run", "false"));
        Boolean full_run = curConf.get("intermediate").matches("(?i).*true.*");
        curConf.put("format", argConf.get("format", "unknown"));
        
        //Constants
        curConf.put("base_path", argConf.get("hdfs_base_path"));
        curConf.put("temp_path", argConf.get("hdfs_temp_path"));
        curConf.put("local_temp_path", argConf.get("local_temp_path"));
        curConf.put("db_name_files", argConf.get("hbase_file_table"));
        curConf.put("db_name_runs", argConf.get("hbase_run_table"));
        curConf.put("db_name_updates", argConf.get("hbase_db_update_table"));

        //Timestamps
        Date currentTime = new Date();
        Date endDate = new Date(new Long(curConf.get("timestamp_stop")));
        curConf.put("timestamp_real", Long.toString(currentTime.getTime()));
        
        return true;
    }
    
    private static String lock(String lock) {
        String realPath = "";
        String parent = "/lock";
        String lockName = parent + "/" + lock;
        
        try{
            if(zkInstance.exists(parent, false) == null)
                zkInstance.create(parent, new byte [0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.fromFlag(0));
        } catch (Exception E) {
            System.out.println("Error creating lock node: " + E.toString());
            E.printStackTrace();
            return null;
        }
        
        List <String> children = new LinkedList<String>();
        try {
            //List <ACL> ACLList = zkInstance.getACL(lockName, zkInstance.exists(lock, false));
            
            realPath = zkInstance.create(lockName, new byte [0], ZooDefs.Ids.OPEN_ACL_UNSAFE ,CreateMode.EPHEMERAL_SEQUENTIAL);
            //children = zkInstance.getChildren(realPath, false);
            checkLock: while(true){
                children = zkInstance.getChildren(parent, false);
                for(String curChild : children) {
                    String child = parent + "/" + curChild;
                    //System.out.println(child + " " + realPath + " " + Integer.toString(child.compareTo(realPath)));
                    if(child.compareTo(realPath) < 0) {
                        Thread.sleep(300);
                        continue checkLock;
                    }
                }
                return realPath;
            }
        } catch(Exception E) {
            System.out.println("While trying to get lock " + lockName);
            System.out.println("ZKException " + E.toString());
            E.printStackTrace();
            return null;
        }
    }
    
    private static boolean unlock(String lock) {
        try {
            zkInstance.delete(lock, -1);
        } catch(Exception E) {
            System.out.println("Error releasing lock: " + E.toString());
            E.printStackTrace();
            return false;
        }
        return true;
    }
    
    private static String getConfigString(Configuration config) {
        String output = "";
        Iterator<Map.Entry<String,String>> iterConfig = config.iterator();
        while(iterConfig.hasNext()) {
            Map.Entry<String,String> curEntry = iterConfig.next();
            output = output + "Key: \t" + curEntry.getKey() + "\nValue: \t" + curEntry.getValue() + "\n";
        }
        return output;
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
        config.put("timestamp_real", latestVersion.toString());
        config.put("timestamp_stop", latestVersion.toString());
               
        System.out.println("Getting DB for timestamp: " + config.get("timestamp_start") + " to " + config.get("timestamp_stop"));
        
        String final_result = getFullPath(config);

        String temp_path_base = config.get("local_temp_path");
        Path newPath = new Path(final_result + "*");
        Vector<Path> ret_path = new Vector<Path>();
        String lockName = lock(final_result.replaceAll("/", "_"));
        if(fs.globStatus(newPath).length != 0) {
            ret_path.add(newPath);
            unlock(lockName);
            config.put("full_file_name", final_result);
            return ret_path;
        } else {
            if(!config.get("source").equals("local")) {
                config.put("temp_path_base", temp_path_base);
                
                config.put("timestamp_start", config.get("timestamp_start"));
                config.put("timestamp_real", latestVersion.toString());
                config.put("timestamp_stop", latestVersion.toString());
                
                Class<?> sourceClass = Class.forName("org.diffdb." + config.get("source") + "Source");
                Method process_data = sourceClass.getMethod("process", Hashtable.class, FileSystem.class);
                Object processor = sourceClass.newInstance();
                Object retVal;
                try{
                    retVal = process_data.invoke(processor, config, fs);
                } catch (InvocationTargetException E) {
                    Throwable exception = E.getTargetException();
                    System.out.println("Exception: " + exception.toString() + "Stacktrace: ");
                    exception.printStackTrace(System.out);
                    unlock(lockName);
                    return null;
                }
                FileStatus [] files = (FileStatus [])retVal;
                
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
        unlock(lockName);
        return ret_path;
    }
    
    private static Long latestVersion(Hashtable<String, String> config, dbutil db_util) throws Exception{
        if(!config.get("timestamp_stop").equals(Integer.toString(Integer.MAX_VALUE))) {
            return new Long(config.get("timestamp_stop"));
        }
        String rowName = config.get("file_id") + config.get("run_id") + "_" + config.get("task_id");
        System.out.println(rowName);
        Get timestampGet = new Get(rowName.getBytes());
        timestampGet.addColumn("d".getBytes(), "update".getBytes());
        Result timestampResult = db_util.doGet(config.get("db_name_updates"), timestampGet);
        KeyValue tsKv = timestampResult.getColumnLatest("d".getBytes(), "update".getBytes());
        if(tsKv == null) {
	  System.out.println("Probably a meta-data collection...");
	  rowName = config.get("file_id") + "_";
	  timestampGet = new Get(rowName.getBytes());
	  timestampGet.addColumn("d".getBytes(), "update".getBytes());
	  timestampResult = db_util.doGet(config.get("db_name_updates"), timestampGet);
	  tsKv = timestampResult.getColumnLatest("d".getBytes(), "update".getBytes());
        }
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
        for(String line : files) {
            if(line.equals(file_path)) {
                if(fs.globStatus(new Path(line + "*")).length == 0) {
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
    
    private static String isDatabase(dbutil db_util, String db_name, String file_id) throws Exception {
      Get file_id_get = new Get(file_id.getBytes());
      Result file_result = db_util.doGet(db_name, file_id_get);
      KeyValue file_db = file_result.getColumnLatest("d".getBytes(), "database".getBytes());
      if(file_db == null){
	return "n";
	}
      String db = new String(file_db.getValue());
      if(db.equals("y")) {
	return "y";
      } else {
	return "n";
      }
    }
    
    private static boolean putRunEntry(dbutil db_util, String db_name, String run_id, String file_id, String type, String timestamp, String timestamp_stop, String path, String regex) throws Exception {
        Put run_id_put = new Put(run_id.getBytes());
        run_id_put.add("d".getBytes(), file_id.getBytes(), new Long(timestamp), type.getBytes());
        run_id_put.add("d".getBytes(), (file_id + "_db_timestamp").getBytes(), new Long(timestamp), timestamp_stop.getBytes());
        run_id_put.add("d".getBytes(), (file_id + "_filename").getBytes(), new Long(timestamp), path.getBytes());
        run_id_put.add("d".getBytes(), (file_id + "_regex").getBytes(), new Long(timestamp), regex.getBytes());
        db_util.doPut(db_name, run_id_put);
        return true;
    }
    
    private static String getFileName(Hashtable<String, String> config) {
        String retString =      config.get("file_id") + "_" + config.get("timestamp_start") + "_" + config.get("timestamp_stop") + "_" +
                                config.get("delimiter").hashCode() + "_" + config.get("taxon");
        if(config.get("database").equals("y")) {
            return retString;
        } else {
	    if(config.get("task_id").isEmpty()) {
	      return retString + "_" + config.get("run_id");
	    } else {
	      return retString + "_" + config.get("run_id") + "_" + config.get("task_id");
	    }
        }
    }
    
/*    private static String getUnixTime(String short_time) {
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
    }*/
    
    private static void printUsage()
    {
        System.out.println("GeStore v0.1");
        System.out.println("Usage:");
        System.out.println("-D run = unique identifier of pipeline run");
        System.out.println("-D file = identifier of the file");
        System.out.println("-D path = local path, where to find or put the file");
        System.out.println("-D type = l2r or r2l (local to remote, or remote to local)");
        System.out.println("-D timestamp_start = time to start processing");
        System.out.println("-D timestamp_stop = time to stop processing");
        System.out.println("-D regex = limit the results (ex. -Dregex=OC=.*bacteria.* for only bacterial results)");
        System.out.println("-D full_run = 'true' if you want to re-run the complete pipeline (ie. no incremental computations");
        System.out.println("-D format = the format of the file you are moving to or from");
        System.out.println("");
        System.out.println("Example usage:");
        System.out.println("hadoop jar diffdb.jar org.diffdb.move -Drun=23452 -Dfile=sprot -Dtype=r2l -Dtimestamp_start=201109 -Dtimestamp_stop=201112 -Dregex=OC=.*bacteria.* -Dfull_run=false");
        return;
    }
}
