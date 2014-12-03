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
    
    /**
     * Add entry from a String (generally from file)
     * Must be implemented per-format
     * 
     * @param	entry	A string representing the entry
     * @return	True if successful, false otherwise
     */
    public abstract boolean addEntry(String entry);
    
    /**
     * Returns a Put with elements more recent than timestamp
     * 
     * @param	fields	A list of fields to be included
     * @param	timestamp	The earlist entries to look at
     */
    public abstract Put getPartialPut(Vector<String> fields, Long timestamp);
    
    /**
     * Check if the entry contains all the elements required for the type of output specified
     *
     * @param	type	The format to check the sanity format
     * @return	0 if the entry contains none of the fields required for the type, 1 if it contains all the fields required, and -1 if some fields are missing
     */
    public abstract int sanityCheck(String type);
    
    /**
     * Generate output in the given format, with optional options
     *
     * @param	type	The format of the output
     * @param	options	A string containing options for the format
     * @return	A string array representing the entry in the specified format
     */
    public abstract String[] get(String type, String options);

    /**
     * Returns a list of updated/different elements
     * 
     * @param	entry	The entry to compare to
     * @return	A list of entries that differ between the two entries
     */
    public abstract Vector<String> compare(genericEntry entry);
    
    /*********************************************
     * Implemented methods
     * ******************************************/
    
    /**
     * Returns the row ID of the entry
     *
     * @return	The ID of the entry
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
    
    /** 
     * Constructor
     *
     * @param	config	The configuration passed in to the entry
     */
    public genericEntry(Configuration config) {
        fieldKeys = new Hashtable<String, String>();
        selfConfig = config;
    }
    
    /** 
     * Constructor
     */
    public genericEntry() {
        fieldKeys = new Hashtable<String, String>();
    }

    /**
     * Populate the entry with an HBase Result
     *
     * @param	entry	The HBase result for an entry
     * @return	True on success, false otherwise
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
    
    /**
     * Regexes to determine start and end of an entry within the input file
     * Used for splitting
     *
     * @return	Two regular expressions as strings representing the start and end of an entry, respectively
     */
    public String[] getRegexes() {
        String[] retString = {"^ID .*", "^//"};
        return retString;
    }
    
    /**
     * Adds several entries, calls {@link addEntry} once per element in the array passed in
     * @param	entries	An array of strings representing an entry
     * @return	True if all of the {@link addEntry} calls succeeded, false otherwise
     */
    public boolean addEntries(String[] entries) {
	boolean result = true;
        for(int i = 0; i < entries.length; i++){
            if(!addEntry(entries[i]))
	      result = false;
        }
        return result;
    }
    
    /**
     * Check if the entry has been initialized properly
     *
     * @return	True if the entry contains some data, false if it is empty
     */
    public boolean exists() {
        if(fieldKeys.isEmpty()) {
            return false;
        } else {
            return true;
        }
    }
    
    /**
     * Returns a single element from the entry
     *
     * @param	key 	The field to retrieve
     * @return The value of the field, null if the field doesn't exist
     */
    public String getTableEntry(String key) {
        if(fieldKeys.containsKey(key)) {
            return (String)fieldKeys.get(key);
        } else {
            return null;
        }
    }
    
    /**
     * Returns the timestamp for the given element
     *
     * @param	field	The field to retrieve the timestamp for
     * @return	The timestamp for the given field, null if the field doesn't exist
     */
    public Long getTimestamp(String field) {
        if(fieldTimestamps.containsKey(field)) {
            return fieldTimestamps.get(field);
        } else {
            return null;
        }
    }
    
    /**
     * Returns a list of elements that are equal
     *
     * @param	entry	The entry to compare to
     * @return	A list of fields that are equal
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
    
    /**
     * Returns a list of elements that have been deleted since the passed-in entry
     *
     * @param	entry	The entry to compare to
     * @return	A list of entries that have been deleted
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
