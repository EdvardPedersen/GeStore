package org.diffdb;

import org.apache.hadoop.fs.*; 
import java.util.*; 
import java.io.File;

public class uniprotSource implements sourceType{
    uniprotSource() {

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
                                "-Dtype=FASTA",
                                "-Dclassname=uniprot",
                                "-Dsplit=" + params.get("split")};
        fs.delete(new Path(temp_dir), true);
        getfasta.main(submission);
        String result_path_existing = temp_dir + "/existing-r-00000";
        String result_path_deleted = temp_dir + "/deleted-r-00000";
        String result_path_metadata = temp_dir + "/metadata-r-00000";

        FileStatus[] resultFilesExists = fs.globStatus(new Path(temp_dir + "/existing-r-*"));
        FileStatus[] resultFilesDeleted = fs.globStatus(new Path(temp_dir + "/deleted-r-*"));
        FileStatus[] resultFilesMetadata = fs.globStatus(new Path(temp_dir + "/metadata-r-*"));
        int fileCounter = 0;
        Boolean splitting = Integer.parseInt(params.get("split")) != 1;

        for(FileStatus file : resultFilesExists) {
            if(splitting) {
                fs.copyToLocalFile(true, file.getPath(), new Path(real_temp_path + "." + Integer.toString(fileCounter)));
            } else {
                fs.copyToLocalFile(true, file.getPath(), new Path(real_temp_path));
            }
            fileCounter++;
        }
        fileCounter = 0;
        for(FileStatus file : resultFilesDeleted) {
            if(splitting) {
                fs.copyToLocalFile(true, file.getPath(), new Path(real_temp_path + ".deleted." + Integer.toString(fileCounter)));
            } else {
                fs.copyToLocalFile(true, file.getPath(), new Path(real_temp_path + ".deleted"));
            }

            fileCounter++;
        }
        fileCounter = 0;
        for(FileStatus file : resultFilesMetadata) {
            if(splitting) {
                fs.copyToLocalFile(true, file.getPath(), new Path(real_temp_path + ".metadata." + Integer.toString(fileCounter)));
            } else {
                fs.copyToLocalFile(true, file.getPath(), new Path(real_temp_path + ".metadata"));
            }
            fileCounter++;
        }
        //fs.rename(new Path(resultPath), new Path(final_result));
        
        local_file_system.delete(new Path(real_temp_path + "*"), true);
        
//        if(fs.exists(new Path(result_path_existing)))
//            fs.copyToLocalFile(true, new Path(result_path_existing), new Path(real_temp_path));
//        if(fs.exists(new Path(result_path_deleted)))
//            fs.copyToLocalFile(true, new Path(result_path_deleted), new Path(real_temp_path + ".deleted"));
//        if(fs.exists(new Path(result_path_metadata))) 
//            fs.copyToLocalFile(true, new Path(result_path_metadata), new Path(real_temp_path + ".metadata"));
        fs.delete(new Path(temp_dir), true);
        // Run formatdb
        // Needs to be moved to somewhere else!
        for(int fcFormat = 0; fcFormat < fileCounter; fcFormat++) {
            String line;
            if(splitting) {
                line = "/opt/bio/ncbi/bin/formatdb -t " + params.get("file_id") + "." + Integer.toString(fcFormat) + " -p T -i " + params.get("file_id") + "." + Integer.toString(fcFormat);
            } else {
                line = "/opt/bio/ncbi/bin/formatdb -t " + params.get("file_id") + " -p T -i " + params.get("file_id");
            }
            System.out.println("Running command: " + line);
            Runtime ourRuntime = Runtime.getRuntime();
            File workingDir = new File(params.get("temp_path_base"));
            Process formatdb = ourRuntime.exec(line, null, workingDir);
            formatdb.waitFor();
        }
        
        FileStatus[] files = local_file_system.globStatus(new Path(real_temp_path + ".*"));
        return files;
    }
}
