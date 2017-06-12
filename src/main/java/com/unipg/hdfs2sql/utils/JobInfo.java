/**
 * 
 */
package com.unipg.hdfs2sql.utils;

import java.sql.Date;

/**
 * @author maria
 *
 */
public class JobInfo {
  public String id;
  public Date date;
  public int nWorkers;
  public int nSupersteps;
  public int nRack;
  public int nHost;
  
  public JobInfo(String id, Date date, int workers, int host, int rack, int supersteps){
    this.id = id;
    this.date = date;
    this.nWorkers = workers;
    this.nSupersteps = supersteps;
    this.nHost = host;
    this.nRack = rack;
    
  }

  public String toString(){
    String jobInfo = 
          "Job{"
        + "\nid: "+this.id
        + "\ndate: "+this.date
        + "\nnWorkers: "+this.nWorkers
        + "\nnHost: "+this.nHost
        + "\nnRack: "+this.nRack
        + "\nnSupersteps: "+this.nSupersteps
        +"\n}";   
    
    return jobInfo;
  }
  
}
