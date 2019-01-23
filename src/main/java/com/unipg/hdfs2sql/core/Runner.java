package com.unipg.hdfs2sql.core;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;

import org.apache.hadoop.fs.UnsupportedFileSystemException;
import org.apache.hadoop.security.AccessControlException;

import com.unipg.hdfs2sql.controller.HierarchyReader;
import com.unipg.hdfs2sql.controller.SuperstepInfoReader;
import com.unipg.hdfs2sql.controller.SuperstepPerScaleInfoReader;
import com.unipg.hdfs2sql.controller.WorkerDataReader;
import com.unipg.hdfs2sql.db.ConnectionFactory;
import com.unipg.hdfs2sql.utils.JobInfo;

public class Runner {

	public static void main(String[] args) throws AccessControlException, 
	FileNotFoundException, UnsupportedFileSystemException, 
	IllegalArgumentException, IOException{

		String folder = "";

		if(args.length == 1 && args[0].equals("-h")) {
			System.out.println("Please put as parameter the folder from which extract the data."
					+ " If no parameter is given, a 'profiler' folder will be searched for in the "
					+ "directory of the jar file.");
			System.exit(0);
			return;
		}
		
		//		if(args[0]!= null/* && args[1]!= null*/){
		if(args.length > 0)
			folder = args[0];
		else
			folder = File.separator + "profiler" + File.separator;

		DataExtractor de = new DataExtractor(folder);
		try {
			ConnectionFactory.beginTransaction("");
		} catch (SQLException e) {
			e.printStackTrace();
			System.exit(2);
		}
		//		if(Boolean.parseBoolean(args[1])){
		try{
			de.createDatabase(ConnectionFactory.dbName);
		}catch(SQLException se){
			se.printStackTrace();
			try{
				ConnectionFactory.closeConnection();
			}catch (SQLException ses){}
			System.exit(2);
		}

		try{
			ConnectionFactory.closeConnection();
		}
		catch(Exception e){}

		try {
			ConnectionFactory.beginTransaction(ConnectionFactory.dbName);
			de.createJobsTable();
			ConnectionFactory.commit(false);
		} catch (SQLException e) {
			e.printStackTrace();
			try{
				ConnectionFactory.closeConnection();
			}catch(Exception se){}
			System.exit(2);
		}

		//		}

		ArrayList<JobInfo>  jobs = null;

		try{
			jobs = de.getHDFSJobsList();
		}catch(Exception e){
			e.printStackTrace();
			try{
				ConnectionFactory.closeConnection();
			}catch (SQLException ses){}
			System.exit(2);			
		}

		int exitValue = 0;				
		try{
			for(JobInfo job : jobs){
				de.addJobToTable(job);
				System.out.println("Found job " + job.id);
			}
			ConnectionFactory.commit(false);
		}catch(SQLException se){
			se.printStackTrace();
			exitValue = 2;
		}catch(Exception e){
			e.printStackTrace();
			exitValue = 2;				
		}finally{
			try{
				ConnectionFactory.closeConnection();
			}catch(Exception e){
				e.printStackTrace();
				System.exit(exitValue);
			}
		}

		for(JobInfo job : jobs){
			try{
				
				System.out.println("Importing...");
				
				ConnectionFactory.beginTransaction("");
				de.createDatabase(job.id);
				ConnectionFactory.commit(true);
				ConnectionFactory.beginTransaction(job.id);
				de.initializeDatabase(job.id);
				ConnectionFactory.commit(false);
				
				HierarchyReader hReader = new HierarchyReader();				
				hReader.getData(folder, job.id, de.getFileSystem());
				
				WorkerDataReader wdr = new WorkerDataReader();
				wdr.setHierarchy(hReader);
				wdr.getData(folder, job, de.getFileSystem());
				
				SuperstepInfoReader.getData(folder, job.id, de.getFileSystem()); //THIS HAS TO BE DONE BEFORE THE FOLLOWING
				SuperstepPerScaleInfoReader.getData(folder, job.id, de.getFileSystem(), hReader);
				
//				ConnectionFactory.commit(false);
//				
//				wdr.correctTable(job);
				
				ConnectionFactory.commit(true);

			}catch(Exception se){
				se.printStackTrace();
				try{
					ConnectionFactory.closeConnection();
				}catch (SQLException ses){}
			}

		}
		
		try{
			ConnectionFactory.closeConnection();
		}catch (SQLException ses){}
		
	}
	
}
