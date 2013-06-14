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
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.mapreduce.*;
import org.apache.hadoop.hbase.client.Put;
import org.apache.zookeeper.*;

public class dbutil{
    Hashtable<String, HTable> databases = new Hashtable<String, HTable>();
    HBaseAdmin hbAdmin;
    Configuration conf;
    
    public dbutil(Configuration config) throws Exception{
        hbAdmin = new HBaseAdmin(config);
        conf = config;
    }
    
    private boolean check_or_create() throws Exception{
        boolean success = true;
        
        for(Enumeration dbs = databases.keys(); dbs.hasMoreElements();) {
            String tempTable = (String)dbs.nextElement();
            if(!check_or_create(tempTable)) {
                success = false;
            }
        }
        return success;
    }
    
    private boolean check_or_create(String tableName) throws Exception{
        if(!hbAdmin.tableExists(tableName)){
            HTableDescriptor newTable = createTable(tableName);
            hbAdmin.createTable(newTable);
            return false;
        }
        return true;
    }
    
    private boolean check(String tableName) throws Exception{
        return hbAdmin.tableExists(tableName);
    }
    
    public boolean register_database(String tableName, boolean create) throws Exception {
        String realTableName = getRealName(tableName);
        if(create) {
            check_or_create(realTableName);
            HTable tempTable = new HTable(conf, realTableName);
            databases.put(realTableName, tempTable);
            return true;
        } else {
            if(check(realTableName)) {
                HTable tempTable = new HTable(conf, realTableName);
                databases.put(realTableName, tempTable);
                return true;
            } else {
                return false;
            }
        }
    }
    
    public Result doGet(String tableName, Get getAction) throws Exception {
        HTable ourTable = (HTable)databases.get(getRealName(tableName));
        return ourTable.get(getAction);
    }
    
    public void doPut(String tableName, Put putAction) throws Exception {
        HTable ourTable = (HTable)databases.get(getRealName(tableName));
        ourTable.put(putAction);
    }
    
    private HTableDescriptor createTable(String tableName) throws Exception{
        try {
            HTableDescriptor htTable = new HTableDescriptor(tableName);
            HColumnDescriptor htCD = new HColumnDescriptor("d".getBytes(), 2000, HColumnDescriptor.DEFAULT_COMPRESSION, false, true, HConstants.FOREVER, HColumnDescriptor.DEFAULT_BLOOMFILTER);
            System.out.println(htCD.toString());
            htTable.addFamily(htCD);
            System.out.println(htTable.toString());
            return htTable;
        } catch (Exception E) {
            System.out.println("Exception! " + E.toString());
            return null;
        }
    }
    
    public String getRealName(String tableName)
    {
        return "gestore_" + tableName;
    }
    
    public static String getShortName(String tableName) {
        return tableName.substring("gestore_".length());
    }
    
    public Put getPut(String row) {
        return new Put(row.getBytes());
    }
    public void close() {
        for(Enumeration dbs = databases.keys(); dbs.hasMoreElements();) {
            String tempTable = (String)dbs.nextElement();
            try {
                databases.get(tempTable).close();
            } catch (Exception E) {
                System.out.println("Exception:" + E.toString());
            }
        }
    }
}
