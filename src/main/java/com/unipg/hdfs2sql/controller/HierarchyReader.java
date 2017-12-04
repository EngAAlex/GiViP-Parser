/**
 * 
 */
package com.unipg.hdfs2sql.controller;


import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.unipg.givip.common.protoutils.HierarchyProtoBook.SingleWorkerTreeHierarchy;
import com.unipg.givip.common.protoutils.HierarchyProtoBook.WorkerHierarchy;
import com.unipg.hdfs2sql.db.ConnectionFactory;

/**
 * @author maria
 *
 */
public class HierarchyReader/* implements Reader*/{

	private HashMap<String, HashMap<String, HashSet<Integer>>> hierarchy;
	
	private HashMap<Integer, String> workerToHostMapping;
	private HashMap<String, String> hostToRackMapping;

	public HierarchyReader(){
		hierarchy = new HashMap<String, HashMap<String, HashSet<Integer>>>();
		workerToHostMapping = new HashMap<Integer, String>();	
		hostToRackMapping = new HashMap<String, String>();
	}
	
	/* 
	 * 
	 */
	public void getData(String folder, String jobID, FileSystem fileSystem) throws IOException, SQLException, NullPointerException{ 

		InputStream inputStream = null;
		WorkerHierarchy parsedHierarchy = null;

		Path hpt = new Path(folder + File.separator + jobID + File.separator+"JobHierarchy");
		inputStream = new FSDataInputStream(fileSystem.open(hpt).getWrappedStream());
		parsedHierarchy = WorkerHierarchy.parseFrom(inputStream);     
		
		HashSet<String> addedHostRackRelationships = new HashSet<String>();

		for(SingleWorkerTreeHierarchy singleWorker: parsedHierarchy.getSingleWorkerTreeHierarchyList()){

			String rack = singleWorker.getRack().substring(1);
			String host = singleWorker.getHostName();
			int worker = singleWorker.getWorker();
			
			workerToHostMapping.put(worker, host);
			hostToRackMapping.put(host, rack);			
			
			if(!hierarchy.containsKey(rack))
				hierarchy.put(rack, new HashMap<String, HashSet<Integer>>());
			if(!hierarchy.get(rack).containsKey(host))
				hierarchy.get(rack).put(host, new HashSet<Integer>());
			
			hierarchy.get(rack).get(host).add(worker);			
			
			//Insert Data Into MySql Database
			String insertIntoHierarchyQuery = "INSERT INTO hierarchy "
					+ "VALUES(?,?,?)";

			PreparedStatement st = null;
			PreparedStatement sp = null;

			try{
				st = ConnectionFactory.prepare(insertIntoHierarchyQuery);

				st.setString(1, ""+singleWorker.getWorker());
				st.setString(2, singleWorker.getHostName());
				st.setInt(3, 0); //relation type --> 0 : worker-host			
				st.executeUpdate();
			}catch(SQLException se){
//				ConnectionFactory.closeStatement(st);
				throw new SQLException(se);
			} finally {
				ConnectionFactory.closeStatement(st);
			}
			
			String builtName = singleWorker.getHostName() + "$_$" + singleWorker.getRack().substring(1);
			
			if(addedHostRackRelationships.contains(builtName))
				continue;

			try{

				sp = ConnectionFactory.prepare(insertIntoHierarchyQuery);

				sp.setString(1, singleWorker.getHostName());
				sp.setString(2, singleWorker.getRack().substring(1));
				sp.setInt(3, 1); //relation type --> 1 : host-rack			
				sp.executeUpdate();
				
				addedHostRackRelationships.add(builtName);
				
			}catch(SQLException se){
//				ConnectionFactory.closeStatement(sp);
				throw new SQLException(se);
			} finally {
				sp.close();
			}

		}

	}
//
//	public HashMap<String, HashMap<String, HashSet<Integer>>> getHierarchy() {
//		return hierarchy;
//	}

	public HashMap<Integer, String> getWorkerToHostMapping() {
		return workerToHostMapping;
	}

	public HashMap<String, String> getHostToRackMapping() {
		return hostToRackMapping;
	}

	public HashMap<String, HashMap<String, HashSet<Integer>>> getHierarchy() {
		return hierarchy;
	}
}
