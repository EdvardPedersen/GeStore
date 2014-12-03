package org.gestore.plugin.source;

import org.apache.hadoop.fs.*; 
import java.util.*; 

public abstract interface sourceType{
    /**
    * Returns a list of files produced by this source
    * 
    * @param	params	The configuration used in move
    * @param	fs	The filesystem the files should reside in
    * @return	A list of files generated
    */
    public abstract FileStatus[] process(Hashtable<String, String> params, FileSystem fs) throws Exception;
}
