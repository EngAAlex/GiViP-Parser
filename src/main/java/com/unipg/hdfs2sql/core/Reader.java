/**
 * 
 */
package com.unipg.hdfs2sql.core;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * @author maria
 *
 */
public interface Reader {
  
  
  public void getData(String folder, String jobID, FileSystem fileSystem);
  
  
}
