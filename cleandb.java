package org.diffdb;

import java.io.IOException; 
import java.util.*; 
import java.io.*; 
import java.lang.reflect.*;
 
import org.apache.hadoop.fs.*; 
import org.apache.hadoop.conf.*; 
import org.apache.hadoop.io.*; 
import org.apache.hadoop.mapreduce.*; 
import org.apache.hadoop.util.*; 
import org.apache.hadoop.util.ToolRunner;
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
 
public class cleandb extends Configured implements Tool{ 

    public static void main(String[] args) throws Exception {
        int result = ToolRunner.run(new Configuration(), new cleandb(), args);
        return;
    }
    
    public int run(String[] args) throws Exception { 
        Configuration argConf = getConf();
        
        //JobConf conf = new JobConf(diffdb.class); 
        Configuration config = HBaseConfiguration.create();
        HBaseAdmin hbAdmin = new HBaseAdmin(config);
        dbutil db_util = new dbutil(config);
        
        HTableDescriptor [] tables = hbAdmin.listTables();
        for(HTableDescriptor table : tables) {
            if(table.getNameAsString().startsWith("gestore_")) {
                //BufferedReader reader = new BufferedReader(System.in);
                Console console = System.console();
                if(args.length > 0) {
                  if(args[0].equals("f")) {
                    hbAdmin.disableTable(table.getName());
                    hbAdmin.deleteTable(table.getName());
                    continue;
                  }
                }
                System.out.println("Delete " + table.getNameAsString() + "? (y/n)");
                if(console.readLine().equals("y")) {
                  System.out.println("Deleting table...");
                  hbAdmin.disableTable(table.getName());
                  hbAdmin.deleteTable(table.getName());
                  System.out.println("Table deleted!");
                }
            }
        }
        return 0;
    }
}
