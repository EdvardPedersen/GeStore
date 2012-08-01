package org.diffdb;

import org.apache.hadoop.fs.*; 
import java.util.*; 

public abstract interface sourceType{
    public abstract FileStatus[] process(Hashtable<String, String> params, FileSystem fs) throws Exception;
}