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
    
    public genericEntry() {
        fieldKeys = new Hashtable<String, String>();
    }

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
    
    public abstract boolean addEntry(String entry);
    
    public String[] getRegexes() {
        String[] retString = {"^ID .*", "^//"};
        return retString;
    }
    
    public boolean addEntries(String[] entries) {
        for(int i = 0; i < entries.length; i++){
            addEntry(entries[i]);
        }
        return true;
    }
    
    public boolean exists() {
        if(fieldKeys.isEmpty()) {
            return false;
        } else {
            return true;
        }
    }
    
    public abstract Put getPartialPut(Vector<String> fields, Long timestamp);
    
    public String getTableEntry(String key) {
        if(fieldKeys.containsKey(key)) {
            return (String)fieldKeys.get(key);
        } else {
            return null;
        }
    }
    
    public abstract int sanityCheck(String type);
    
    public Long getTimestamp(String field) {
        if(fieldTimestamps.containsKey(field)) {
            return fieldTimestamps.get(field);
        } else {
            return null;
        }
    }

    public abstract byte[] getRowID();
    
    public abstract String[] get(String type, String options);

    // Returns list of updated fields
    public abstract Vector<String> compare(genericEntry entry);
    
    // Returns list of equal fields
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
    
    // Returns list of deleted fields
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