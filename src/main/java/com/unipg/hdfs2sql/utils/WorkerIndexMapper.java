/**
 * 
 */
package com.unipg.hdfs2sql.utils;

import java.util.TreeMap;

/**
 * @author maria
 *
 */
public class WorkerIndexMapper {
  
  TreeMap<Integer,Integer> pairWorkerIndex = new TreeMap<Integer,Integer>();
  private int mapIndex=0;
  
  public void mapWorker(int workerId){
    this.pairWorkerIndex.put(workerId, this.mapIndex);
    this.mapIndex++;
  }
  
  public int getIndexWorker(int workerId){
    return this.pairWorkerIndex.get(workerId);
  }

  public TreeMap<Integer,Integer> getpairWorkerIndex(){
    return this.pairWorkerIndex;
  }
  
}
