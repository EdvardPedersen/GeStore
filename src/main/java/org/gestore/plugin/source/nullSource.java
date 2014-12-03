package org.gestore.plugin.source;

import org.gestore.getfasta;

import org.apache.hadoop.fs.*; 
import java.util.*; 
import java.io.File;

public class nullSource implements sourceType{
    nullSource() {

    }
    
    public FileStatus[] process(Hashtable<String, String> params, FileSystem fs) throws Exception{
        FileSystem local_file_system = fs.getLocal(fs.getConf());
        FileStatus[] files = null;
        return files;
    }
}
