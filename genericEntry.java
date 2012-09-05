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
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;

public abstract class genericEntry{
    protected Hashtable<String, String> fieldKeys = new Hashtable<String, String>();
    protected Hashtable<String, Long> fieldTimestamps = new Hashtable<String, Long>();
    protected int numEntries = 0;
    protected int keyV = 0;
    protected byte[] family = "d".getBytes();
    protected Configuration selfConfig;
    
    /********************************************
     * Abstract methods
     * *****************************************/
    
    /*
     * Add entry from a String (generally from file)
     * Must be implemented per-format
     */
    public abstract boolean addEntry(String entry);
    
    /*
     * Returns a Put with elements more recent than timestamp
     */
    public abstract Put getPartialPut(Vector<String> fields, Long timestamp);
    
    /*
     * Check if the entry contains all the elements required for the type of output specified
     */
    public abstract int sanityCheck(String type);
    
    /*
     * Generate output in the given format, with optional options
     */
    public abstract String[] get(String type, String options);

    /*
     * Returns a list of updated/different elements
     */
    public abstract Vector<String> compare(genericEntry entry);
    
    /*********************************************
     * Implemented methods
     * ******************************************/
    
    /*
     * Returns the row ID of the entry
     */
    public byte[] getRowID() {
        String idStripped = (String)fieldKeys.get("ID").trim();
        if(selfConfig.get("task_id") != null) {
            idStripped = selfConfig.get("task_id") + "_" + idStripped;
        }
        if(selfConfig.get("run_id") != null) {
            idStripped = selfConfig.get("run_id") + "_" + idStripped;
        }
        return idStripped.getBytes();
    }
    
    /* 
     * Constructor
     */
    public genericEntry(Configuration config) {
        fieldKeys = new Hashtable<String, String>();
        selfConfig = config;
    }
    
    public genericEntry() {
        fieldKeys = new Hashtable<String, String>();
    }

    /*
     * Populate the entry with an HBase Result
     * Returns true on success, false otherwise
     */
    public boolean addEntry(Result entry) {
        NavigableMap<byte[], byte[]> resultMap = entry.getFamilyMap(family);
        if(resultMap == null) {
            return false;
        }
        while(resultMap.size() > 0) {
            Map.Entry<byte[], byte[]> keyVal = resultMap.pollFirstEntry();
            String key = new String(keyVal.getKey());
            String value = new String(keyVal.getValue());
            fieldKeys.put(key, value);
            Long timestamp = entry.getColumnLatest(family, keyVal.getKey()).getTimestamp();
            fieldTimestamps.put(key, timestamp);
        }
        return true;
    }
    
    /*
     * Regexes to determine start and end of an entry within the input file
     * Used for splitting
     * Returns two strings representing regexes
     */
    public String[] getRegexes() {
        String[] retString = {"^ID .*", "^//"};
        return retString;
    }
    
    /*
     * Adds several entries, calls addEntry(String) once per element in the array passed in
     */
    public boolean addEntries(String[] entries) {
        for(int i = 0; i < entries.length; i++){
            addEntry(entries[i]);
        }
        return true;
    }
    
    /*
     * Check if the entry has been initialized properly
     */
    public boolean exists() {
        if(fieldKeys.isEmpty()) {
            return false;
        } else {
            return true;
        }
    }
    
    /*
     * Returns a single element from the entry
     */
    public String getTableEntry(String key) {
        if(fieldKeys.containsKey(key)) {
            return (String)fieldKeys.get(key);
        } else {
            return null;
        }
    }
    
    /*
     * Returns the timestamp for the given element
     */
    public Long getTimestamp(String field) {
        if(fieldTimestamps.containsKey(field)) {
            return fieldTimestamps.get(field);
        } else {
            return null;
        }
    }
    
    /*
     * Returns a list of elements that are equal
     */
    public Vector<String> equalFields(genericEntry entry) {
        Vector<String> retList = new Vector<String>();
        for(Enumeration field = fieldKeys.keys(); field.hasMoreElements(); ) {
            String key = (String)field.nextElement();
            if(fieldKeys.get(key).equals(entry.getTableEntry(key))) {
                retList.add(key);
            }
        }
        return retList;
    }
    
    /*
     * Returns a list of elements that have been deleted since the passed-in entry
     */
    public Vector<String> getDeleted(genericEntry entry) {
        Vector<String> retList = new Vector<String>();
        for(Enumeration field = fieldKeys.keys(); field.hasMoreElements(); ) {
            String key = (String)field.nextElement();
            if(key.equals("EXISTS")) {
                continue;
            }
            if(entry.getTableEntry(key) == null) {
                retList.add(key);
            }
        }
        return retList;
    }
}