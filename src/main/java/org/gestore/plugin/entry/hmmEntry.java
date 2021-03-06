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
//import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ConcurrentSkipListSet;

public class hmmEntry extends genericEntry{
    String[] mandatoryFields = {"NAME", "LENG", "ALPH", "HMM"};
    String[] optionalFields = {"ACC", "DESC", "RF", "CS", "MAP", "DATE", "COM", "NSEQ", "EFFN", "CKSUM", "GA", "TC", "NC", "STATS", "COMPO"};
    int numField;
    public hmmEntry(Configuration config) {
        fieldKeys = new Hashtable<String, String>();
        //fieldKeys.put("ORDER", "");
        selfConfig = config;
        numField = 0;
    }
    
    public hmmEntry() {
        fieldKeys = new Hashtable<String, String>();
        //fieldKeys.put("ORDER", "");
        numField = 0;
    }
    
    // Parses the string to add to a certain field
    public boolean addEntry(String entry) {
        if(entry.length() == 0) {
            return false;
        }
        String typeIn;
        if(entry.length() > 4) {
            typeIn = entry.substring(0,4).trim();
        } else {
            typeIn = "//";
        }
        if(!Arrays.asList(mandatoryFields).contains(typeIn) && !Arrays.asList(optionalFields).contains(typeIn)){
            typeIn = "DAT";
        }
        if(typeIn.equals("NAME")) {
            typeIn = "ID";
        }
        String existing = (String)fieldKeys.get(typeIn);
//        Integer offset = 0;
        numField += 1;
        if(existing != null) {
            //StringBuilder putString = new StringBuilder(existing.length() + entry.length() + 5);
            //offset = existing.length();
            //putString.append(existing);
            //putString.append(entry);
            //putString.append("\n");
//            numHMM += 1;
            String putString = entry + "\n";
            fieldKeys.put(Integer.toString(numField) + "-" + typeIn, new String(putString));
        } else {
            String putString = entry + "\n";
            if(typeIn.equals("ID")) {
                fieldKeys.put(typeIn, putString);
            } 
            fieldKeys.put(Integer.toString(numField) + "-" + typeIn, putString);
        }
        //fieldKeys.put("ORDER", fieldKeys.get("ORDER") + typeIn + ":" + offset.toString() + "\n");
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

    // Check if the entry is well-formed, based on type 
    // (e.g. if all the fields required to do a get of a certain type exist)
    public int sanityCheck(String type){
        if(type.equals("FULL")) {
            String ID = getTableEntry("ID");
            String DAT = getTableEntry("DAT");
            
            if(null == DAT && null == ID) {
                return 0;
            } else if(null != DAT && null != ID) {
                return 1;
            }
        }
        return -1;
    }
    
    // Returns an array of strings containing each field based on type and options
    public String[] get(String type, String options) {
        if (type.equals("hmm")) {
            String[] retString = {getHmm2().toString(), ""};
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
            if(!localVal.equals(remoteVal)) {
                retList.add(key);
            }
        }
        return retList;
    }
    
    public String[] getRegexes() {
        String[] retString = {"^HMMER.*", "^HMMER.*"};
        return retString;
    }
    
    
    // PRIVATE
    
    private Text getHmm2() {
      StringBuilder outString = new StringBuilder();
      List<String> l = new LinkedList<String>(fieldKeys.keySet());
      ConcurrentSkipListSet<String> list2 = new ConcurrentSkipListSet<String>(new HMMComparator());
      for(String key : l) {
        if(key.split(" ")[0].split("-").length < 2) {
 //         l.remove(key);
        } else {
          list2.add(key);
        }
      }
//      Collections.sort(list2, new HMMComparator());
//      outString.append(fieldKeys.get("ID"));
      for(String key : list2) {
        if(key.equals("ID")) {
          continue;
        }
        outString.append(fieldKeys.get(key));
      }
      return new Text(outString.toString().replaceAll("\0", ""));
    }

    private class HMMComparator implements Comparator<String>{
      @Override
      public int compare(String str1, String str2) {

        // extract numeric portion out of the string and convert them to int
        // and compare them, roughly something like this

        int num1 = Integer.parseInt(str1.split("-")[0]);
        int num2 = Integer.parseInt(str2.split("-")[0]);

        return num1 - num2;
      }
    }

    private Text getHmm() {
        Text outText = new Text();
        String order = fieldKeys.get("ORDER");
        LinkedList<String> outputs = new LinkedList<String>();
        
        for(String field : order.split("\n")) {
            String[] rowOffset = field.split(":");
            if(rowOffset[0].equals("ORDER")){
                continue;
            }
            String value = "";
            try{
                String completeEntry = fieldKeys.get(rowOffset[0]).substring(new Integer(rowOffset[1]));
                String [] smallerEntries = completeEntry.split("\n");
                value = smallerEntries[0];
            } catch (NumberFormatException E) {
                System.out.println("NumberFormatException: " + field + "----" + rowOffset[0] + "||" + rowOffset[1]);
            } catch (ArrayIndexOutOfBoundsException Ar) {
                System.out.println("Not two elements in split: " + field);
            }
            outputs.add(value);
            //outText.append(appendString.getBytes(), 0, appendString.length());
        }
        StringBuilder sb = new StringBuilder();
        String delim = "";
        for(String line : outputs) {
            sb.append(delim).append(line);
            delim = "\n";
        }
        outText = new Text(sb.toString().replaceAll("\0", ""));
        return outText;
    }
}
