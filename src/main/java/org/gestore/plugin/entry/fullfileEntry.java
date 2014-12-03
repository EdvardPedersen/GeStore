package org.gestore.plugin.entry;

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
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import java.util.regex.*;

public class fullfileEntry extends genericEntry{
    FileSystem hdfs;
    public fullfileEntry(Configuration config) {
        fieldKeys = new Hashtable<String, String>();
        try{
            hdfs = FileSystem.get(config);
        } catch (IOException E) {
            System.out.println("Error generating filesystem: " + E.toString());
        }
        selfConfig = config;
    }
    
    public fullfileEntry() {
        fieldKeys = new Hashtable<String, String>();
    }
    
    // Parses the string to add to a certain field
    public boolean addEntry(String entry) {
        String [] pathHashTimestamp = entry.split("\t");

	String inputFile = pathHashTimestamp[0].substring(pathHashTimestamp[0].lastIndexOf("/") + 1);
        
        Path targetPath = new Path(pathHashTimestamp[3] + selfConfig.get("task_id", ""));
        Path sourcePath = new Path(pathHashTimestamp[0]);
        Path sourceBase = new Path(pathHashTimestamp[4].trim());
	Path extendedBase = new Path(pathHashTimestamp[5]);
        
        FileStatus targetStat;
        FileStatus baseStat;
        
        try {
            hdfs.mkdirs(targetPath);
            targetStat = hdfs.getFileStatus(targetPath);
            baseStat = hdfs.getFileStatus(sourceBase);
        } catch (IOException E) {
            System.out.println("Error getting file status for files: " + E.toString());
            return false;
        }
        
        String suffix = sourcePath.toString().substring(baseStat.getPath().toString().length());
        
        if(hdfs != null) {
            try {
                hdfs.mkdirs(targetStat.getPath().suffix(suffix).getParent());
                try {
                    hdfs.rename(sourcePath, targetStat.getPath().suffix(suffix));
                } catch (IOException E) {
                    System.out.println("ERROR: Moving file " + sourcePath.toString() + " to " + targetStat.getPath().suffix(suffix).toString());
                    System.out.println("Error: " + E.toString());
                    if(!hdfs.isFile(sourcePath)) {
                        System.out.println("REASON: Source is not a file!");
                    }
                    return false;
                }
            } catch (IOException E) {
                System.out.println("Unable to copy file" + E.toString());
                return false;
            }
        } else {
            System.out.println("No filesystem!");
        }
        
        fieldKeys.put("ID",  selfConfig.get("task_id", "") + pathHashTimestamp[5].substring(0, pathHashTimestamp[5].lastIndexOf("/")) );
	System.out.println("Task-id:" + selfConfig.get("task_id") + " file " + extendedBase);
        fieldKeys.put("HASH", pathHashTimestamp[1]);
        fieldKeys.put("PATH", targetPath + suffix);
        fieldKeys.put("SUFFIX", suffix);

        System.out.println("File:" + inputFile);
        
        // IDEAS:
        // Key = hash, value = file position
        // Key = filename, value = file position, value2 = hash
        
        return true;
    }
    
    // Returns a Put containing the fields, done for the Timestamp
    public Put getPartialPut(Vector<String> fields, Long timestamp){
        Put retPut = null;
        String fam = "d";
        String ID = new String(getRowID());
        if(ID.isEmpty()) {
            System.out.println("NO ID!!");
        } else {
            String value = ID;
            try {
                retPut = new Put(value.getBytes());
            } catch (StringIndexOutOfBoundsException E) {
                System.out.println("Exception: " + E.toString() + "Value: " + value);
            }
            for(String key : fields) {
                if(key.equals("NEW")) {
                    for(Enumeration field = fieldKeys.keys(); field.hasMoreElements(); ) {
                        String keyL = (String)field.nextElement();
                        String valueL = (String)fieldKeys.get(keyL);
                        if(timestamp > 0) {
                            retPut.add(fam.getBytes(), keyL.getBytes(), timestamp, valueL.getBytes());
                        } else {
                            retPut.add(fam.getBytes(), keyL.getBytes(), valueL.getBytes());
                        }               
                    }
                    return retPut;
                }
                
                String valueL = (String)getTableEntry(key);
                if(timestamp > 0) {
                    retPut.add(fam.getBytes(), key.getBytes(), timestamp, valueL.getBytes());
                } else {
                    retPut.add(fam.getBytes(), key.getBytes(), valueL.getBytes());
                }
            }
        }
        return retPut;
    }

    public byte[] getRowID() {
        String idStripped = (String)fieldKeys.get("ID").trim();
        return idStripped.getBytes();
    }

    // Check if the entry is well-formed, based on type 
    // (e.g. if all the fields required to do a get of a certain type exist)
    public int sanityCheck(String type){
        if(type.equals("FULL")) {
            String ID = getTableEntry("ID");
            if(null == ID) {
                return 0;
            } else if(null != ID) {
                return 1;
            }
        }
        return -1;
    }
    
    // Returns an array of strings containing each field based on type and options
    public String[] get(String type, String options) {
        if (type.equals("files")) {
            String[] retString = {getFileList().toString(), ""};
            return retString;
        }
        String[] retString = {null, null};
        return retString;
    }

    // Returns list of updated fields
    public Vector<String> compare(genericEntry entry) {
        Vector<String> retList = new Vector<String>();
        
        if(getTableEntry("ID") == null || entry.getTableEntry("ID") == null) {
            retList.add("NEW");
            return retList;
        }
        
        for(Enumeration field = fieldKeys.keys(); field.hasMoreElements(); ) {
            String key = (String)field.nextElement();
            String localVal = (String)getTableEntry(key);
            String remoteVal = (String)entry.getTableEntry(key);
            if(key.equals("PATH") && getTableEntry("HASH").equals(entry.getTableEntry("HASH"))) {
                // If the hash is the same, do not add the new path!
                // And delete the new file to save storage space
                try {
                    hdfs.delete(new Path(localVal),false);
                } catch (IOException E) {
                    System.out.println("WARNING: Tried to delete file non-successfully, something might be wrong in the data! Exception:" + E.toString());
                }
                continue;
            }
            if(!localVal.equals(remoteVal)) {
                retList.add(key);
            }
        }
        return retList;
    }
    
    public String[] getRegexes() {
        String[] retString = {".*", ".*"};
        return retString;
    }
    
    
    // PRIVATE
    
    private Text getFileList() {
        Text outText = new Text(fieldKeys.get("PATH") + "\t" + fieldKeys.get("SUFFIX") + "\t" + fieldKeys.get("ID"));
        return outText;
    }
}
