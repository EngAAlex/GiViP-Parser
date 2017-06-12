package com.unipg.hdfs2sql.utils;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Iterator;
import java.util.Map;

/**
 * 
 */

/**
 * @author maria
 *
 */
public class SuperstepMatrix {
    private int[][] stepMat;
    private int superStep;
    private String ROW_VALUES="SourceWorkerTaskId";
    private String COL_VALUES="DestinationWorkerTaskId";
    private int nRow;
    private int nCol;
    private static File currentJobOutputFile;
    private static String currentJob;
    
    public SuperstepMatrix(int row,int col, int superstep){
      this.stepMat=new int[row][col];
      this.superStep=superstep;
      this.nRow=row;
      this.nCol=col;
    }
    
    public void updateMatrixDimension(){
      int[][] newMatrix = new int[nRow+1][nCol+1];
      for(int r=0;r<nRow;r++){
        for(int c=0;c<nCol;c++){
          newMatrix[r][c]=this.stepMat[r][c];
        }
      }
      this.stepMat= newMatrix;
      nRow++;
      nCol++;
    }
    
    public void insertValueIn(int row,int col,double d){
      this.stepMat[row][col]=(this.stepMat[row][col])+(int)(d);
    }
    
    public double readPosition(int row,int col){
      return this.stepMat[row][col];
    }
    
//    public String generateCSVString(){
//      String csvString="S"+this.superStep;
//      
////      csvString=csvString+"SuperstepN"+this.superStep;
//      
//      //Header --> Superstep0;w1;w2;w3;....;wn
//      for(int k=0;k<this.nCol;k++){
//        csvString=csvString+";"+"w"+k;
//      }
//      //Rest of matrix contents and row header
//      csvString=csvString+"\n";
//      for(int i=0; i<this.nRow; i++){
//        csvString=csvString+"w"+i+";";
//        for(int j=0;j<this.nCol; j++){
//            csvString=csvString+this.stepMat[i][j]+";";
//        }
//        csvString=csvString+"\n";
//      }
//      return csvString;
//      
//    }
    
    
    /**
     * Method to create file path and current jobFile
     * 
     * @param jobId
     */
    public static void initializesFileCSV(String jobId){
      @SuppressWarnings("unused")
      boolean success =
          (new File(
            System.getProperty("user.home") + "/Profiler/"+jobId))
          .mkdirs(); 

      currentJobOutputFile = new File(System.getProperty("user.home")
        + "/Profiler/" +jobId+"/"+"ExchangedData");
      currentJob=jobId;
    }
    
    
    
    /**
     * Method to generate a CSV file with matrix content
     * 
     * @param mapper
     */
    public void printCSVFile(WorkerIndexMapper mapper){
      PrintWriter printer=null;
      try {
        printer=new PrintWriter(new BufferedWriter(new FileWriter(currentJobOutputFile,true)));
      } catch (IOException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      } 
      
      //Header
      String intestazione="S"+this.superStep;
      printer.print(intestazione);
      System.out.print(intestazione);
      //Rest of matrix
      
      for (Map.Entry<Integer, Integer> entry : mapper.getpairWorkerIndex().entrySet()) { 
          
          printer.print(";w"+entry.getKey());
                      System.out.print(";w"+entry.getKey());
      }
                        
//        System.out.println("Key = " + entry.getKey());
//        System.out.println("Value = " + entry.getValue()); 
//      } 
//    }
      printer.println();
                      System.out.println();
      
      for (Map.Entry<Integer, Integer> entry : mapper.getpairWorkerIndex().entrySet()) { 
        
        printer.print("w"+entry.getKey()+";");
                      System.out.print("w"+entry.getKey()+";");
          for(int j=0;j<this.nCol; j++){
            printer.print(this.stepMat[mapper.getIndexWorker(entry.getKey())][j]+";");
            
                      System.out.print(this.stepMat[mapper.getIndexWorker(entry.getKey())][j]+";");
            
          }
          printer.println();
                      System.out.println();
      
      }
      
      
//      //Rest of the matrix
//      for(int k=0;k<this.nCol;k++){
//        printer.print(";w");
//      }
//      printer.println();

//      for(int i=0; i<this.nRow; i++){
//        printer.print("w"+i+";");
//        for(int j=0;j<this.nCol; j++){
//          printer.print(this.stepMat[i][j]+";");
//        }
//        printer.println();
//      }
      printer.println();
      printer.close();
    }
    
    
    

    /**
     * @return the rOW_VALUES
     */
    public String getROW_VALUES() {
      return ROW_VALUES;
    }

    /**
     * @param rOW_VALUES the rOW_VALUES to set
     */
    public void setROW_VALUES(String rOW_VALUES) {
      ROW_VALUES = rOW_VALUES;
    }

    /**
     * @return the cOL_VALUES
     */
    public String getCOL_VALUES() {
      return COL_VALUES;
    }

    /**
     * @param cOL_VALUES the cOL_VALUES to set
     */
    public void setCOL_VALUES(String cOL_VALUES) {
      COL_VALUES = cOL_VALUES;
    }
      
}
