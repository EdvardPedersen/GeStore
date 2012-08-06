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

public class uniprotEntry extends genericEntry{
    public uniprotEntry() {
        fieldKeys = new Hashtable<String, String>();
    }
    
    // Parses the string to add to a certain field
    public boolean addEntry(String entry) {
        if(entry.length() < 5) {
            return false;
        }
        String typeIn = entry.substring(0,2);
        if(typeIn.equals("  ")) {
            typeIn = "SE";
        }
        String existing = (String)fieldKeys.get(typeIn);
        if(existing != null) {
            String putString = existing + entry.substring(2) + "\n";
            fieldKeys.put(typeIn, putString);
        } else {
            String putString = entry.substring(2) + "\n";
            fieldKeys.put(typeIn, putString);
        }
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
                        if(keyL.equals("  ")) {
                            keyL = "SE";
                        }
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

    // Check if the entry is well-formed, based on type 
    // (e.g. if all the fields required to do a get of a certain type exist)
    public int sanityCheck(String type){
        if(type.equals("FASTA")) {
            String ID = getTableEntry("ID");
            String SE = getTableEntry("SE");
            
            if(null == SE && null == ID) {
                return 0;
            } else if(null != SE && null != ID) {
                return 1;
            }
        }
        return -1;
    }

    // Gets the ID of the row
    public byte[] getRowID() {
        String idStripped = (String)fieldKeys.get("ID").trim();
        String id = idStripped.substring(0,12).trim();
        return id.getBytes();
    }
    
    // Returns an array of strings containing each field based on type and options
    public String[] get(String type, String options) {
        if(type.equals("FASTA")) {
            if(sanityCheck(type) == 1) {
                return getFasta(options);
            }
        } else if (type.equals("DAT")) {
            String[] retString = {getDat().toString()};
            return retString;
        }
        String[] retString = {"NULL"};
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
            if(!localVal.equals(remoteVal)) {
                retList.add(key);
            }
        }
        return retList;
    }
    
    
    // PRIVATE
    private String[] getFasta(String taxon) {
        String id = ">" + getTableEntry("ID").trim() + " " + taxon;
        String seq = getTableEntry("SE").trim();
        seq = seq.replaceAll(" ", "");
        seq = seq.replaceAll("\0", "");
        seq = seq.replaceAll("\\n","");
        String[] retString = {id,seq};
        return retString;
    }
    
    private Text getDat() {
        Text outText = new Text();
        for(Enumeration field = fieldKeys.keys(); field.hasMoreElements(); ) {
            String key = (String)field.nextElement();
            String value = fieldKeys.get(key);
            String[] lines = value.split("\n");
            for(String line : lines) {
                String appendString = key + "     " + line + "\n";
                outText.append(appendString.getBytes(), 0, appendString.length());
            }
        }
        return outText;
    }
}