/**
 * 
 */
package com.unipg.hdfs2sql.controller;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;

import com.unipg.hdfs2sql.db.ConnectionFactory;
import com.unipg.hdfs2sql.utils.JobInfo;
import com.unipg.hdfs2sql.utils.WorkerIndexMapper;
import com.unipg.profilercommon.protoutils.MessagesProtoBook.ExchangedMessage;
import com.unipg.profilercommon.protoutils.MessagesProtoBook.MessagesBook;

/**
 * @author maria
 *
 */
public class WorkerDataReader/* implements Reader */{

	public static final String workerFolderPrefix = "WorkerData"+File.separator+"WorkerN-";
	public static final String superstepMessagesFilePrefix = "ExchangedMessagesSuperstepN-";

//	public static final String sqlMsgsInsertIntoTempQuery = "INSERT INTO temp_messages_by_%TABLE VALUES(?,?,?,?,?)";
	public static final String sqlMsgsInsertIntoTempQuery = "INSERT INTO messages_by_%TABLE VALUES(?,?,?,?,?)";

	public static final String sqlMsgsInsertQuery = "INSERT INTO messages_by_%TABLE VALUES(?,?,?,?,?)";

	public static final String dropTempTableQuery = "DROP TABLE IF EXISTS temp_messages_by_%TABLE";

	/*	public static final String sqlMsgsUpdateQuery = "UPDATE messages_by_%TABLE SET nMessages = ?, bytes = ? WHERE superstepId = ? AND source = ? AND target = ?";
	 */
	public static final String getAggregatedDataQuery = "SELECT SUM(nMessages), SUM(bytes) FROM temp_messages_by_%TABLE WHERE superstepId = ? AND source = ? AND target = ?";

	public static final String presenceCheckerQuery = "SELECT nMessages, bytes FROM messages_by_%TABLE WHERE superstepId = ? AND source = ? AND target = ?";

	HashMap<String, String> hostToRackMap;
	HashMap<Integer, String> workerToHostMap;

	int fSuperstep;
	HashMap<String, HashSet<String>> workingElements;
	String[] tables = {"worker", "host", "rack"};

	/* 
	 * 
	 */
	public void setHierarchy(HierarchyReader hierarchy) {
		hostToRackMap = hierarchy.getHostToRackMapping();
		workerToHostMap = hierarchy.getWorkerToHostMapping();
		workingElements = new HashMap<String, HashSet<String>> ();
		for(String table : tables)
			workingElements.put(table, new HashSet<String>());
	}

	public void getData(String folder, JobInfo job, FileSystem fileSystem) throws IOException, SQLException, NullPointerException{


		//		Map<Integer,String> workerHostMap = Mapper.mapWorkerHost(folder, jobID, fileSystem);
		//		Map<Integer,String> workerRackMap = Mapper.mapWorkerRack(folder, jobID, fileSystem);

		String[] strings = null;
		String jobID = job.id;

		PreparedStatement ps = null; 
		PreparedStatement sp = null; 

		int workerIndex;
		WorkerIndexMapper mapper = new WorkerIndexMapper();

		RemoteIterator<LocatedFileStatus> it = fileSystem.listLocatedStatus(new Path(folder + File.separator + jobID + File.separator+"WorkerData"));
		while(it.hasNext()){
			LocatedFileStatus current = it.next();
			strings = current.getPath().getName().split("-");
			workerIndex = Integer.parseInt(strings[strings.length - 1]);
			mapper.mapWorker(workerIndex);
			//			//		}
			//
			//			int worker = mapper.getpairWorkerIndex().entrySet().iterator().next().getValue();

			int allSupersteps = job.nSupersteps;

			int superstep = 0;

			Path wPath = new Path(folder + jobID + File.separator+ workerFolderPrefix + workerIndex);

			while(superstep < allSupersteps){

				Path pt = new Path(wPath + File.separator + superstepMessagesFilePrefix + superstep);

				if(!fileSystem.exists(pt)){
					superstep++;
					continue;
				}

				//			FileStatus current = fileSystem.getFileStatus(pt);

				//			while(its.hasNext()){
				System.out.println("Analyzing worker " + workerIndex + " at superstep " + superstep);

				//				LocatedFileStatus current = its.next();

				//			for (Map.Entry<Integer, Integer> entry : mapper.getpairWorkerIndex().entrySet()){


				/*					String name = current.getPath().getName();
				 *///					String[] split = name.split("-");
				//				int superstep = Integer.parseInt(split[1]);
				//					fSuperstep = Math.max(fSuperstep, superstep);

				MessagesBook messagesBook = null;
				InputStream input = null;
				//					Path pt = new Path(folder + jobID + File.separator+ workerFolderPrefix + entry.getKey() + File.separator + current.getPath().getName());

				input = new FSDataInputStream(fileSystem.open(pt).getWrappedStream());
				messagesBook = MessagesBook.parseFrom(input);

				for (ExchangedMessage message : messagesBook.getExchangeMessageList()) { 

					//					ResultSet rs = null;

					for(String table : tables){					
						
						try{
							String source = correctIndex(table, message.getWorkerSourceId());
							String target = correctIndex(table, message.getWorkerDestId());
							workingElements.get(table).add(source);
							workingElements.get(table).add(target);
							int msgsNumber = message.getMessagesNumber();
							double bytesNumber = message.getMessagesByteSize();

							//							ps = ConnectionFactory.prepare(presenceCheckerQuery.replace("%TABLE", table));
							//							ps.setInt(1, superstep);
							//							ps.setString(2, source);
							//							ps.setString(3,  target);
							//
							//							rs = ps.executeQuery();
							//
							//							if(rs.getFetchSize() == 0){						

							sp = ConnectionFactory.prepare(sqlMsgsInsertIntoTempQuery.replace("%TABLE", table));
							sp.setInt(1, superstep);
							sp.setString(2, source);
							sp.setString(3,  target);
							sp.setInt(4, msgsNumber);
							sp.setDouble(5,  bytesNumber);

							sp.executeUpdate();
							//							}else{
							//								int nMessages = 0;
							//								long nBytes = 0;
							//
							//								rs.next();
							//
							//								nMessages = rs.getInt(0);
							//								nBytes = rs.getLong(1);
							//
							//								rs.close();
							//
							//								sp = ConnectionFactory.prepare(sqlMsgsUpdateQuery.replace("%TABLE", table));
							//								sp.setInt(3, superstep);
							//								sp.setString(4, source);
							//								sp.setString(5,  target);
							//								sp.setInt(1, msgsNumber + nMessages); 
							//								sp.setDouble(2, bytesNumber + nBytes);
							//
							//								sp.executeUpdate();							
							//							}

							//							ps.close();
							sp.close();
							//							rs.close();
						}catch(SQLException se){
							throw new SQLException(se);
						}finally{
							ConnectionFactory.closeStatement(ps);
						}
					}
				}

				input.close();

				superstep++;

				//			}
			}
		}
	}

	public void correctTable(JobInfo job){
		PreparedStatement st = null;
		ResultSet rs = null;
		System.out.println("Correcting table");
		for(String s : tables){
			String[] elements = workingElements.get(s).toArray(new String[0]);			
			String insertNewQuery = sqlMsgsInsertQuery.replace("%TABLE", s);
			String groupByQuery = getAggregatedDataQuery.replace("%TABLE", s);
			for(int j=0; j<elements.length; j++){
				for(int i=0; i<job.nSupersteps; i++){
					for(int t=0; t<elements.length; t++){
						try{
							String source = elements[j];
							String target = elements[t];
							
							st = ConnectionFactory.prepare(groupByQuery);
							st.setInt(1, i);
							st.setString(2, source);
							st.setString(3, target);

							rs = st.executeQuery();

							rs.next();

							long sumMessages = rs.getLong(1);
							long sumBytes = rs.getLong(2);

							rs.close();
							st.close();

							st = ConnectionFactory.prepare(insertNewQuery);
							st.setInt(1, i);
							st.setString(2, source);
							st.setString(3, target);
							st.setLong(4, sumMessages);
							st.setLong(5,  sumBytes);

							st.executeUpdate();

							st.close();							

						}catch(Exception e){
							e.printStackTrace();							
							try {
								rs.close();								
								st.close();
							} catch (SQLException e1) {
								e1.printStackTrace();
							}						
						}
					}
				}
			}

			System.out.println("Complete! Dropping support tables");

			try{
				st = ConnectionFactory.prepare(dropTempTableQuery.replace("%TABLE", s));
				st.executeUpdate();

				st.close();
			}catch(Exception e){
				e.printStackTrace();
			}

		}
	}

	private String correctIndex(String scale, int element){
		switch(scale){
		case "worker": return ""+element;
		case "host": return workerToHostMap.get(element);
		case "rack": return hostToRackMap.get(workerToHostMap.get(element));
		}

		return null;
	}
}


