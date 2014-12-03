package org.diffdb;

import org.apache.hadoop.fs.*; 
import java.util.*; 
import java.io.File;

public class fastaSource implements sourceType{
    fastaSource() {

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
                                "-Ddatabase=" + params.get("database"),
                                "-Dtype=FASTA",
                                "-Dclassname=fasta",
                                "-Drun_id=" + params.get("run_id"),
                                "-Dtask_id=" + params.get("task_id"),
                                "-Dsplit=" + params.get("split")
        };
        fs.delete(new Path(temp_dir), true);
        getfasta.main(submission);
        
        FileStatus[] resultFilesExists = fs.globStatus(new Path(temp_dir + "/existing-r-*"));
        FileStatus[] resultFilesDeleted = fs.globStatus(new Path(temp_dir + "/deleted-r-*"));
        FileStatus[] resultFilesMetadata = fs.globStatus(new Path(temp_dir + "/metadata-r-*"));


        int fileCounter = 0;

        for(FileStatus file : resultFilesExists) {
            if(Integer.parseInt(params.get("split")) == 1) {
                fs.copyToLocalFile(true, file.getPath(), new Path(real_temp_path));
            } else {
                fs.copyToLocalFile(true, file.getPath(), new Path(real_temp_path + "." + Integer.toString(fileCounter)));
            }
            fileCounter++;
        }
        fileCounter = 0;
        for(FileStatus file : resultFilesDeleted) {
            if(Integer.parseInt(params.get("split")) == 1 ) {
                fs.copyToLocalFile(true, file.getPath(), new Path(real_temp_path + ".deleted"));
            } else {
                fs.copyToLocalFile(true, file.getPath(), new Path(real_temp_path + ".deleted." + Integer.toString(fileCounter)));
            }
            fileCounter++;
        }
        fileCounter = 0;
        for(FileStatus file : resultFilesMetadata) {
            if(Integer.parseInt(params.get("split")) == 1 ) {
                fs.copyToLocalFile(true, file.getPath(), new Path(real_temp_path + ".metadata"));
            } else {
                fs.copyToLocalFile(true, file.getPath(), new Path(real_temp_path + ".metadata." + Integer.toString(fileCounter)));
            }
            fileCounter++;
        }
        
        local_file_system.delete(new Path(real_temp_path + "*"), true);
        
        
        fs.delete(new Path(temp_dir), true);
        
        FileStatus[] files = local_file_system.globStatus(new Path(real_temp_path + "*"));
        return files;
    }
}
