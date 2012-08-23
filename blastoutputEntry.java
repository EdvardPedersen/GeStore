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

public class blastoutputEntry extends genericEntry{
    String [] requiredFields = {"query_id", "subject_id", "identity", "align_length", "mismatches", "gaps", "query_start", "q_stop", "s_start", "s_stop", "e_value", "hit"};
    public blastoutputEntry(Configuration config) {
        fieldKeys = new Hashtable<String, String>();
    }
    
    public blastoutputEntry() {
        fieldKeys = new Hashtable<String, String>();
    }
    
    // Parses the string to add to a certain field
    public boolean addEntry(String entry) {
        String [] fields = entry.split("\t");
        if(fields.length < 5)
            return false;
        
        for(Integer i = 0; i < fields.length; i++) {
            fieldKeys.put(requiredFields[i], fields[i]);
        }
        
        fieldKeys.put("ID", entry);
        
        numEntries += 1;
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
    
    public String[] getRegexes() {
        String[] retString = {".*", ".*"};
        return retString;
    }

    // Check if the entry is well-formed, based on type 
    // (e.g. if all the fields required to do a get of a certain type exist)
    public int sanityCheck(String type){
        if(type.equals("blastoutput")) {
            String ID = getTableEntry("ID");
            
            if(null == ID) {
                return 0;
            } else {
                for(String entry : requiredFields) {
                    if(getTableEntry(entry) == null) {
                        return -1;
                    }
                }
            }
        }
        return 1;
    }

    // Gets the ID of the row
    public byte[] getRowID() {
        String id = (String)fieldKeys.get("ID").trim();
        return id.getBytes();
    }
    
    // Returns an array of strings containing each field based on type and options
    public String[] get(String type, String options) {
        if(type.equals("blastoutput")) {
            if(sanityCheck(type) == 1) {
                return getBlastOutput(options);
            }
        }
        return null;
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
            if(!localVal.equals(remoteVal)) {
                retList.add(key);
            }
        }
        return retList;
    }
    
    
    // PRIVATE
    private String[] getBlastOutput(String taxon) {
        String rets = new String();
        for(String entry : requiredFields) {
            rets = rets + getTableEntry(entry) + "\t";
        }
        String[] retString = {rets.trim(),""};
        return retString;
    }
}