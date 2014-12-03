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

public class nullEntry extends genericEntry{
    public nullEntry() {
        fieldKeys = new Hashtable<String, String>();
    }
    
    public nullEntry(Configuration config) {
        fieldKeys = new Hashtable<String, String>();
        selfConfig = config;
    }
    
    public boolean addEntry(String entry) {
        return true;
    }
    
    public Put getPartialPut(Vector<String> fields, Long timestamp){
        Put retPut = new Put();
        return retPut;
    }

    public int sanityCheck(String type){
        return -1;
    }
    
    public String[] get(String type, String options) {
        String[] retString = {"NULL"};
        return retString;
    }

    // Returns list of updated fields
    public Vector<String> compare(genericEntry entry) {
        Vector<String> retList = new Vector<String>();
        return retList;
    }
    
    public String[] getRegexes() {
        String[] retString = {"^>.*", "^>.*"};
        return retString;
    }
}
