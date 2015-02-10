package org.gestore.plugin.source;

import org.gestore.getfasta;

import org.apache.hadoop.fs.*; 
import java.util.*; 
import java.io.File;

public class hmmSource implements sourceType{
    public hmmSource() {

    }
    
    public FileStatus[] process(Hashtable<String, String> params, FileSystem fs) throws Exception{
        FileSystem local_file_system = fs.getLocal(fs.getConf());
        local_file_system.mkdirs(new Path(params.get("temp_path_base")));
        String temp_dir = params.get("temp_path") + params.get("file_id") + "_dir";
        String real_temp_path = params.get("temp_path_base") + params.get("file_id");
        String[] submission = { "-Dinput_table=" + params.get("file_id"),
                                "-Doutput_file=" + temp_dir, 
                                "-Dtimestamp_start=" + params.get("timestamp_start"),
                                "-Dtimestamp_stop=" + params.get("timestamp_stop"),
                                "-Dregex=" + params.get("delimiter"), 
                                "-Daddendum=" + params.get("taxon"),
                                "-Dtype=hmm",
                                "-Dclassname=hmm" };
        fs.delete(new Path(temp_dir), true);
        getfasta.main(submission);
        String result_path_existing = temp_dir + "/existing-r-00000";
        String result_path_deleted = temp_dir + "/deleted-r-00000";
        //fs.rename(new Path(resultPath), new Path(final_result));
        
        local_file_system.delete(new Path(real_temp_path + "*"), true);
        
        if(fs.exists(new Path(result_path_existing)))
            fs.copyToLocalFile(true, new Path(result_path_existing), new Path(real_temp_path));
        if(fs.exists(new Path(result_path_deleted)))
            fs.copyToLocalFile(true, new Path(result_path_deleted), new Path(real_temp_path + ".deleted"));
        fs.delete(new Path(temp_dir), true);

        //Need to run HMMpress
        
        //String line = "/opt/local/hmmer30rc2/binaries/hmmpress " + params.get("file_id");
	String line = "/opt/bio/hmmer/bin/hmmpress " + params.get("file_id");
        System.out.println("Running command: " + line);
        Runtime ourRuntime = Runtime.getRuntime();
        File workingDir = new File(params.get("temp_path_base"));
        Process formatdb = ourRuntime.exec(line, null, workingDir);
        formatdb.waitFor();
        
        FileStatus[] files = local_file_system.globStatus(new Path(real_temp_path + "*"));
        return files;
    }
}
