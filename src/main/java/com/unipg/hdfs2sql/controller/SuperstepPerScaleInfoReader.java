/**
 * 
 */
package com.unipg.hdfs2sql.controller;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;

import com.unipg.hdfs2sql.db.ConnectionFactory;
import com.unipg.profilercommon.protoutils.ExecutedSuperstepWorkerInfoProto;
import com.unipg.profilercommon.protoutils.ExecutedSuperstepWorkerInfoProto.ExecSuperstepWorkerInfo;
import com.unipg.profilercommon.protoutils.ExecutedSuperstepWorkerInfoProto.ExecSuperstepWorkerInfo.SuperstepInfo;

/**
 * @author maria
 *
 */
public class SuperstepPerScaleInfoReader{

	public static final String workerFolderPrefix = "WorkerSuperstepInfo";
	public static final String superstepMessagesFilePrefix = "ExecutedSuperstepWorkerInfoWorkerN-";

	/* 
	 * 
	 */
	public static void getData(String folder, String jobID, FileSystem fileSystem, HierarchyReader hr) throws IOException, SQLException, NullPointerException{

		int workerIndex = -1;

		RemoteIterator<LocatedFileStatus> it = fileSystem.listLocatedStatus(new Path(folder + File.separator + jobID + File.separator+ workerFolderPrefix));

		ArrayList<HashMap<String, double[]>> tempToHost = new ArrayList<HashMap<String, double[]>>();
		ArrayList<HashMap<String, double[]>> tempToRack = new ArrayList<HashMap<String, double[]>>();

		HashMap<Integer, String> wToH = hr.getWorkerToHostMapping();
		HashMap<String, String> hToR = hr.getHostToRackMapping();

		String sqlWorkerSuperstepInfoInsertQuery = "INSERT INTO superstepinfo_per_%SCALE VALUES(?,?,?,?)";
		String insertComputationNameIntoTableQuery = "UPDATE superstepinfo SET computation = ? WHERE superstepId = ?";

		int supersteps = -1;
		
		Comparator<SuperstepInfo> ss = new Comparator<ExecutedSuperstepWorkerInfoProto.ExecSuperstepWorkerInfo.SuperstepInfo>() {

			@Override
			public int compare(SuperstepInfo o1, SuperstepInfo o2) {
				if(o1.getSuperstepIndex() == o2.getSuperstepIndex())
					return 0;
				if(o1.getSuperstepIndex() < o2.getSuperstepIndex())
					return -1;
				return 1;
			}
		};
		
		while(it.hasNext()){
			String[] strings = null;
			LocatedFileStatus current = it.next();
			strings = current.getPath().getName().split("-");
			workerIndex = Integer.parseInt(strings[strings.length - 1]);

			InputStream input = null;

			input = new FSDataInputStream(fileSystem.open(current.getPath()).getWrappedStream());
			ExecSuperstepWorkerInfo superstepInfo = ExecSuperstepWorkerInfo.parseFrom(input);

//			if(tempToHost == null){
//				tempToHost = new ArrayList<HashMap<String, double[]>>(); //new Double[superstepInfo.getSuperstepInfoList().size()][hr.getHostToRackMapping().size()][2];
//				tempToRack = new ArrayList<HashMap<String, double[]>>();
//			}

			if(supersteps < 0)
				supersteps = superstepInfo.getSuperstepInfoList().size();
			
			List<SuperstepInfo> sortedInfo = new ArrayList<SuperstepInfo>(superstepInfo.getSuperstepInfoList());
			
			Collections.sort(sortedInfo, ss);
			
			for (SuperstepInfo sInfo : sortedInfo) {

				PreparedStatement ps = null, pt = null; 
				long currentSuperstep = sInfo.getSuperstepIndex();
				
				if(tempToHost.size() <= currentSuperstep) //!tempToHost.contains((int) sInfo.getSuperstepIndex()))
					tempToHost.add((int) currentSuperstep, new HashMap<String, double[]>());

				if(tempToRack.size() <= currentSuperstep) //.contains((int) sInfo.getSuperstepIndex()))
					tempToRack.add((int) currentSuperstep, new HashMap<String, double[]>());

				HashMap<String, double[]> currentHostMap = tempToHost.get((int) sInfo.getSuperstepIndex());
				HashMap<String, double[]> currentRackMap = tempToRack.get((int) sInfo.getSuperstepIndex());

				String currentHost = wToH.get(workerIndex);
				String currentRack = hToR.get(currentHost);

				if(!currentHostMap.containsKey(currentHost))
					currentHostMap.put(currentHost, new double[2]);

				if(!currentRackMap.containsKey(currentRack))
					currentRackMap.put(currentRack, new double[2]);

				currentHostMap.get(currentHost)[0] += sInfo.getComputationSecs();
				currentRackMap.get(currentRack)[0] += sInfo.getComputationSecs();

				currentHostMap.get(currentHost)[1] += sInfo.getProcessedVertices();
				currentRackMap.get(currentRack)[1] += sInfo.getProcessedVertices();

				try{
					ps = ConnectionFactory.prepare(sqlWorkerSuperstepInfoInsertQuery.replace("%SCALE", "worker"));
					ps.setLong(1, currentSuperstep);
					ps.setInt(2, workerIndex);
					ps.setDouble(3,  sInfo.getComputationSecs());
					ps.setLong(4, sInfo.getProcessedVertices()); 

					ps.executeUpdate();


					pt = ConnectionFactory.prepare(insertComputationNameIntoTableQuery);
					pt.setString(1, sInfo.getComputationName());
					pt.setLong(2, sInfo.getSuperstepIndex());

					pt.executeUpdate();

				}catch(SQLException se){
					throw new SQLException(se);
				}finally{
					ps.close();
					pt.close(); 
				}
			}

			input.close();

		}

		PreparedStatement pw = null, pb = null; 

		HashMap<String, HashMap<String, HashSet<Integer>>> hierarchy = hr.getHierarchy();

		for(int i = 0; i<supersteps; i++){

			HashMap<String, double[]> currentHostInfo = tempToHost.get(i);
			HashMap<String, double[]> currentRackInfo = tempToRack.get(i);

			for(Entry<String, double[]> e : currentHostInfo.entrySet()){
				try{

					pw = ConnectionFactory.prepare(sqlWorkerSuperstepInfoInsertQuery.replace("%SCALE", "host"));
					pw.setLong(1, i);
					pw.setString(2, e.getKey());
					pw.setDouble(3,  e.getValue()[0]/hierarchy.get(hToR.get(e.getKey())).get(e.getKey()).size());
					pw.setLong(4, new Double(e.getValue()[1]).longValue()); 

					pw.executeUpdate();

					pw.close();

				}catch(SQLException se){
					try{
						pw.close();
					}catch(Exception ex){}
					throw new SQLException(se);
				}
			}

			for(Entry<String, double[]> er : currentRackInfo.entrySet()){
				try{
					pb = ConnectionFactory.prepare(sqlWorkerSuperstepInfoInsertQuery.replace("%SCALE", "rack"));
					pb.setLong(1, i);
					pb.setString(2, er.getKey());
					pb.setDouble(3,  er.getValue()[0]/hierarchy.get(er.getKey()).size());
					pb.setLong(4, new Double(er.getValue()[1]).longValue()); 

					pb.executeUpdate();

					pb.close();
				}catch(SQLException se){
					try{
						pb.close();
					}catch(Exception ex){}
					throw new SQLException(se);
				}
			}

		}		
	}
}


