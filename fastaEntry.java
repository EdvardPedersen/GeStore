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

public class fastaEntry extends genericEntry{
    public fastaEntry() {
        fieldKeys = new Hashtable<String, String>();
    }
    
    public fastaEntry(Configuration config) {
        fieldKeys = new Hashtable<String, String>();
        selfConfig = config;
    }
    
    public boolean addEntry(String entry) {
        String typeIn = entry.substring(0,1);
        if(typeIn.equals(">")) {
            typeIn = "ID";
            String existing = (String)fieldKeys.get(typeIn);
            if(null != existing){
                System.out.println("ERROR: ID " + entry + " and ID " + existing + " duplicate!");
            }
        } else {
            typeIn = "SEQ";
        }
        String existing = (String)fieldKeys.get(typeIn);
        if(existing != null) {
            String putString = existing + entry.trim();
            fieldKeys.put(typeIn, putString);
        } else {
            String putString = entry.trim();
            fieldKeys.put(typeIn, putString);
        }
        numEntries += 1;
        return true;
    }
    
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
                
                String valueL = (String)fieldKeys.get(key);
                if(timestamp > 0) {
                    retPut.add(fam.getBytes(), key.getBytes(), timestamp, valueL.getBytes());
                } else {
                    retPut.add(fam.getBytes(), key.getBytes(), valueL.getBytes());
                }
            }
        }
        return retPut;
    }

    public int sanityCheck(String type){
        if(type.equals("FASTA")) {
            String ID = getTableEntry("ID");
            String SE = getTableEntry("SEQ");
            
            if(null == SE && null == ID) {
                return 0;
            } else if(null != SE && null != ID) {
                return 1;
            }
        }
        return -1;
    }
    
    public String[] get(String type, String options) {
        if(type.equals("FASTA")) {
            if(sanityCheck(type) == 1) {
                return getFasta(options);
            }
        }
        String[] retString = {"NULL"};
        return retString;
    }

    // Returns list of updated fields
    public Vector<String> compare(genericEntry entry) {
        Vector<String> retList = new Vector<String>();
        
        if(!fieldKeys.containsKey("ID") || entry.getTableEntry("ID") == null) {
            retList.add("NEW");
            return retList;
        }
        
        for(Enumeration field = fieldKeys.keys(); field.hasMoreElements(); ) {
            String key = (String)field.nextElement();
            if(!fieldKeys.get(key).equals(entry.getTableEntry(key))) {
                retList.add(key);
            }
        }
        return retList;
    }
    
    public String[] getRegexes() {
        String[] retString = {"^>.*", "^>.*"};
        return retString;
    }
    
    
    // PRIVATE
    private String[] getFasta(String taxon) {
        System.out.println(getTableEntry("ID").trim());
        System.out.println(taxon);
        String id = getTableEntry("ID").trim();
        String seq = getTableEntry("SEQ").trim();
        seq = seq.replaceAll(" ", "");
        seq = seq.replaceAll("\0", "");
        seq = seq.replaceAll("\\n","");
        String[] retString = {id,seq};
        return retString;
    }
}