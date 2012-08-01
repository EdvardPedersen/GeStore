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
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.mapreduce.*;
import org.apache.hadoop.hbase.client.Put;
import org.apache.zookeeper.*;

public class blastoutputformat{
    private Hashtable<String, String> entry = new Hashtable<String, String>();
    private enum Fields{ queryId, subjectId, percIdentity, alnLength, mismatchCount, gapOpenCount, queryStart, queryEnd, subjectStart, subjectEnd, eVal, bitScore };
    
    public blastoutputformat(Text input){
        String ourString = input.toString();
        String[] inSplit = ourString.split("\t");
        int i = 0;
        for(Fields field : Fields.values()) {
            entry.put(field.toString(), inSplit[i]);
            i += 1;
        }
    }
    
    public String toString() {
        return entry.toString();
    }
    
    public byte[] db_row() {
        return entry.get("subjectId").getBytes();
    }
}