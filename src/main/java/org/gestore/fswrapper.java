// Wrapper for FS-like functions:
// ls
// recursive ls
// get a file
// get all files in dir
// put a file (+ compression maybe)
// put a dir
// move file
// check if file exists
// check if dir exists
// mkdir
// delete file
// delete a directory
// get file size
// recursive file listing

package org.gestore;

import java.util.*;
import java.io.*;
import java.nio.*;
import java.nio.charset.*;
import java.nio.*;
import org.gestore.dbutil.*;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.fs.*;


public class fswrapper {
  HBaseConfiguration conf;
  String full_namespace;
  String namespace;
  HTable ourTable;
  FileSystem fs;
  Scan ourScan;
  dbutil db_util;
  


  public fswrapper(String namespc) {
    try {
      conf = new HBaseConfiguration();
      db_util = new dbutil(conf);
      fs = FileSystem.get(conf);
      full_namespace = "gestore_" + namespc;
      namespace = namespc;
      if(!db_util.register_database(namespc, true)) {
        throw new Exception("Trouble creating table");
      }
      ourTable = db_util.getTable(namespace);
    } catch (Exception E) {
      System.out.println("Problem with Hadoop: " + E.toString());
    }
  }
 
  public int run_gestore(String[] params) {
    List<String> args = new ArrayList<String>();
    args.add("hadoop");
    args.add("jar");
    args.add("/home/epe005/gestore/move.jar");
    args.add("org.diffdb.move");
    for(String param : params) {
      args.add(param);
    }
    ProcessBuilder gsp = new ProcessBuilder(args);
    gsp.inheritIO();
    try {
      Process running = gsp.start();
      return running.waitFor();
    } catch (Exception E) {
      System.out.println("Unable to run GeStore: " + E.toString());
    }
    return 1;
  }

  public ArrayList<String> ls(String path) {
    // List files in directory
    // Create a scan over namespace, from path to path+1
    // Remove files that are in a subdirectory
    // return list of files

    ArrayList<String> files = lsRec(path);

    if(files == null) {
      return null;
    }
    //For each in file, remove if it contains a / after path
    
    Iterator <String> i = files.iterator();

    while(i.hasNext()) {
       String curFile = i.next();
       if(curFile.indexOf('/', path.length()) > 1) {
         i.remove();
         System.out.println("Removing: " + curFile);
       }
    }
    for(String file : files) {
      System.out.println("LSFILE: " + file);
    }
    return files;
  }

  public ArrayList<String> getFile(String path) {
    // Do gestore_get
    //  Get pipeline ID from filename?
    //  Might need fix in IMP code to append timestamp to meta-data filenames for retrieval
    if(path == null) {
      RuntimeException E = new RuntimeException("Trying to get file that is NULL");
      throw(E);
    }
    String supPath = getSuperPath(path);
    ArrayList<String> returns = new ArrayList<String>();
    System.out.println("supPath = " + supPath);
    String subPath = "";
    if(!supPath.equals(path) && supPath != null ) {
      int supPathLength = 1;
      String subString = path.substring(supPathLength);
      subPath = subString;
    }
    String [] args = {"-Dtype=r2l", "-Dfile=" + namespace,"-Dfull_run=true" , "-Dtask=" + supPath, "-conf=/home/epe005/gestore/gestore-conf.xml"};
    if(run_gestore(args) != 0) {
      System.out.println("Problem getting file...");
    } else {
      System.out.println("File got successfully!");
    }
   
    try {
      List<String> outFiles = java.nio.file.Files.readAllLines(java.nio.file.Paths.get(namespace), Charset.defaultCharset());
      for(String line : outFiles) {
        String [] paths = line.split("\t");
        String hdfsPath = paths[0];
        String okPath = paths[1];
        System.out.println("hdfsPath = " + hdfsPath + "  supPath = " + supPath + " second path: " + okPath);
        int indexOne = hdfsPath.indexOf(supPath);
        int indexTwo = supPath.length();
        String hitPath = paths[0].substring(paths[0].indexOf(paths[1]) + 1);
        if(hitPath.equals(subPath)) {
          System.out.println("File found!");
          //fs.copyToLocalFile(false, new Path(paths[0]), new Path("."));
          returns.add(paths[0]);
        }
      }
    } catch (Exception E) {
      System.out.println("Error reading result file...");
      E.printStackTrace();
    }

    return returns;
  }

  public ArrayList<String> getFiles(String path) {
    // Do gestore_get for all files in directory
    String supPath = getSuperPath(path);
    ArrayList<String> returns = new ArrayList<String>();
    System.out.println("supPath = " + supPath);
    String subPath = "";
    if(!supPath.equals(path)) {
      subPath = path.substring(supPath.length());
    }
    String [] args = {"-Dtype=r2l", "-Dfile=" + namespace, "-Dfull_run=true", "-Dtask=" + supPath, "-conf=/home/epe005/gestore/gestore-conf.xml"};
    if(run_gestore(args) != 0) {
      System.out.println("Problem getting file...");
    } else {
      System.out.println("File got successfully!");
    }

    try {
      List<String> outFiles = java.nio.file.Files.readAllLines(java.nio.file.Paths.get(namespace), Charset.defaultCharset());
      for(String line : outFiles) {
	
        String [] paths = line.split("\t");
        System.out.println("GETFILESRESULT: " + line + " COMP " + path);
        if(paths[2].contains(path)){
          returns.add(paths[0]);
        }
      }
    } catch (Exception E) {
      System.out.println("Error reading result file...");
      E.printStackTrace();
    }
    /*if(returns.size() == 0) {
      return null;
    }*/
    return returns;
  }

  public int putFile(String src_path, String dst_path) {
    // Do gestore_put

    String [] args = {"-Dtype=l2r", "-Dfile=" + namespace, "-Dtask=" + dst_path, "-Dpath=" + src_path, "-Dformat=fullfile", "-conf=/home/epe005/gestore/gestore-conf.xml"};

    if(run_gestore(args) != 0) {
      System.out.println("Problem putting file into GeStore: " + dst_path + "ns: " + namespace);
      return 1;
    } else {
      System.out.println("File put into GeStore successfully: " + dst_path + " ns: " + namespace);
      return 0;
    }
  }

  public int putFiles(String src_path, String dst_path) {
    // Do Gestore_put for all files in directory
    putFile(src_path, dst_path);
    return 1;
  }

  public int mkdir(String dst_path) {
    // Make a new directory (useful?)
    // Possibly always return true?
    return 1;
  }

  public int delFile(String dst_path) {
    // Remove a file
    // Remove entry in db, remove file(s) on HDFS
    try {
      System.out.printf(new String(dst_path + "_" + '\uffff'));
      ourScan = new Scan(new String(dst_path + "_").getBytes(), incremented(new String(dst_path + "_")).getBytes());
      ResultScanner result = ourTable.getScanner(ourScan);
      int delete = 0;
      System.out.println("Deleting a fiiiile" + dst_path);
      for(Result res : result) {
        System.out.println("Result: " + new String(res.getRow()));
        Path delPath = new Path(new String(res.getColumnLatest("d".getBytes(), "PATH".getBytes()).getValue()));
        Delete delRow = new Delete(res.getRow());
        Put delPut = new Put(res.getRow(), res.getColumnLatest("d".getBytes(), "EXISTS".getBytes()).getTimestamp());
        System.out.println(delPut.toString());
        delPut.add("d".getBytes(), "EXISTS".getBytes(), Integer.MAX_VALUE, "0".getBytes());
 
        ourTable.put(delPut);
        fs.delete(delPath, true);
        // Decrement ENTRIES?
      } 
    } catch (Exception E) {
      System.out.println("Exception caught! " + E.toString());
      return 0;
    }

    return 1;
  }

  public int delDir(String dst_path) {
    // Remove directory
    // same as delFile, but remove subdirs as well
    try {
      ourScan = new Scan(dst_path.getBytes(), incremented(dst_path).getBytes());
      ResultScanner result = ourTable.getScanner(ourScan);
      int delete = 0;
      for(Result res : result) {
        System.out.println("Result: " + new String(res.getRow()));
        Path delPath = new Path(new String(res.getColumnLatest("d".getBytes(), "PATH".getBytes()).getValue()));
        Delete delRow = new Delete(res.getRow());
        Put delPut = new Put(res.getRow(), res.getColumnLatest("d".getBytes(), "EXISTS".getBytes()).getTimestamp());
        System.out.println(delPut.toString());
        delPut.add("d".getBytes(), "EXISTS".getBytes(), Integer.MAX_VALUE, "0".getBytes());

        ourTable.put(delPut);

        //ourTable.delete(delRow);
        // CHANGE TO EXISTS PUT
        // Decrement ENTRIES?
        fs.delete(delPath, true);
      }
    } catch (Exception E) {
      System.out.println("Exception caught! " + E.toString());
      return 0;
    }
    return 1;
  }

  public long getFileSize(String dst_path) {
    // Return bytes in HDFS
    // Get handle on file, check size
    try {
      ourScan = new Scan(dst_path.getBytes(), new String(dst_path + "\000").getBytes());
      ResultScanner result = ourTable.getScanner(ourScan);
      int delete = 0;
      for(Result res : result) {
        System.out.println("Result: " + new String(res.getRow()));
        Path delPath = new Path(new String(res.getColumnLatest("d".getBytes(), "PATH".getBytes()).getValue()));
        FileStatus file = fs.getFileStatus(delPath);
        return file.getLen();
      }
    } catch (Exception E) {
      System.out.println("Exception caught! " + E.toString());
      return -1;
    }
    return -1;
  }

  public ArrayList<String> lsRec(String dst_path){
    // Do a recursive ls
    // Same as LS, but do not remove files
    ArrayList<String> files = new ArrayList<String>();
    try {
      ourScan = new Scan(dst_path.getBytes(), incremented(dst_path).getBytes());
      ResultScanner result = ourTable.getScanner(ourScan);
      for(Result res : result) {
        if(Arrays.equals(res.getColumnLatest("d".getBytes(), "EXISTS".getBytes()).getValue(), "0".getBytes())) {
          System.out.println("Deleted!");
        } else {
          System.out.println("Result: " + new String(res.getColumnLatest("d".getBytes(), "EXISTS".getBytes()).getValue()));
          files.add(new String(res.getColumnLatest("d".getBytes(), "SUFFIX".getBytes()).getValue()));
        }
      } 
    } catch (Exception E) {
      System.out.println("Exception caught! " + E.toString());
      E.printStackTrace(System.out);
      return null;
    }
    return files;
  }

  public static void main(String[] args) {
    fswrapper runner = new fswrapper(args[1]);
    switch(args[0]) {
      case "ls":
        System.out.println("LIST stub");
        runner.ls(args[2]);
        break;
      case "lsr":
        System.out.println("Retrieving recursive list of files...");
        runner.lsRec(args[2]);
      case "getfile":
        System.out.println("Getting file...");
        ArrayList<String> files = runner.getFile(args[2]);
        for(String file : files) {
          System.out.println("File: " + file);
        }
        break;
      case "getfiles":
        ArrayList<String> filess = runner.getFiles(args[2]);
        for(String file : filess) {
          System.out.println("GETFILES: " + file);
        }
        break;
      case "putfile":
        System.out.println("Putting file...");
        runner.putFile(args[2], args[3]);
        break;
      case "rmr":
        System.out.println("Removing file...");
        runner.delFile(args[2]);
        break;
      case "rm":
        System.out.println("Removing file...");
        runner.delDir(args[2]);
    }
  }

  public String incremented(String original) {
    StringBuilder buf = new StringBuilder(original);
    int index = buf.length() -1;
    while(index >= 0) {
       char c = buf.charAt(index);
       c++;
       buf.setCharAt(index, c);
       if(c == 0) { // overflow, carry one
          index--;
          continue;
       }
       return buf.toString();
    }
    // overflow at the first "digit", need to add one more digit
    buf.insert(0, '\1');
    return buf.toString();
  }
  
  public String getSuperPath(String path) {
    try {
      //HBaseConfiguration conf = new HBaseConfiguration();
      //String full_namespace = "gestore_" + namespace;
      String dst_path = path;

      Scan ourScan = new Scan(dst_path.getBytes(), incremented(dst_path).getBytes());
      ResultScanner result = ourTable.getScanner(ourScan);

      System.out.println("Getting SUPERPATH for path " + dst_path);
      
      while(result.next() == null) {
        System.out.println("No results for: " + dst_path);
        dst_path = stripPath(dst_path);
        ourScan.setStartRow(dst_path.getBytes());
        ourScan.setStopRow(incremented(dst_path).getBytes());
        result = ourTable.getScanner(ourScan);
        System.out.println(ourScan.toString());
      }
      System.out.println("Lenght of returned string: " + dst_path);
      if(dst_path != null) {
        return dst_path;
      } else {
        return "";
      }
    } catch (Exception E) {
      System.out.println("Exception caught! " + E.toString());
      E.printStackTrace(System.out);
      return "";
    }
  }

  private String stripPath(String inputPath) {
    return inputPath.substring(0, inputPath.lastIndexOf('/'));
  }
  
  public FileSystem getFS() {
    return fs;
  }

}
