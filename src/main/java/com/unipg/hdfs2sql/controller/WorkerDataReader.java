/**
 * 
 */
package com.unipg.hdfs2sql.controller;

import java.io.File;
import java.io.FileNotFoundException;
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

import com.mysql.jdbc.exceptions.jdbc4.MySQLIntegrityConstraintViolationException;
import com.unipg.givip.common.protoutils.LatenciesProtoBook.LatenciesBook;
import com.unipg.givip.common.protoutils.LatenciesProtoBook.RecordedLatency;
import com.unipg.givip.common.protoutils.MessagesProtoBook.ExchangedMessage;
import com.unipg.givip.common.protoutils.MessagesProtoBook.MessagesBook;
import com.unipg.hdfs2sql.db.ConnectionFactory;
import com.unipg.hdfs2sql.utils.JobInfo;
import com.unipg.hdfs2sql.utils.WorkerIndexMapper;

/**
 * @author maria
 *
 */
public class WorkerDataReader/* implements Reader */{

	public static final String workerFolderPrefix = "WorkerData";

	/*MESSAGES ORIENTED VARIABLES*/

	public static final String workerMessagesFolderName = "MessagesData";
	public static final String workerMessagesFolderPrefix = workerFolderPrefix+File.separator+workerMessagesFolderName+File.separator+"WorkerN-";
	public static final String superstepMessagesFilePrefix = "ExchangedMessagesSuperstepN-";

	public static final String sqlMsgsInsertIntoTempQuery = "INSERT INTO messages_by_%TABLE VALUES(?,?,?,?,?)";
	public static final String sqlMsgsInsertQuery = "INSERT INTO messages_by_%TABLE VALUES(?,?,?,?,?)";
	public static final String dropTempTableQuery = "DROP TABLE IF EXISTS temp_messages_by_%TABLE";

	public static final String getAggregatedDataQuery = "SELECT SUM(nMessages), SUM(bytes) FROM temp_messages_by_%TABLE WHERE superstepId = ? AND source = ? AND target = ?";
	public static final String presenceCheckerQuery = "SELECT nMessages, bytes FROM messages_by_%TABLE WHERE superstepId = ? AND source = ? AND target = ?";

	HashMap<String, String> hostToRackMap;
	HashMap<Integer, String> workerToHostMap;

	/*LATENCIES ORIENTED VARIABLES*/

	public static final String latenciesFolderName = "LatenciesData";
	public static final String latenciesFolderPrefix = workerFolderPrefix+File.separator+latenciesFolderName+File.separator+"WorkerN-";
	public static final String superstepLatenciesFilePrefix = "LatenciesSuperstepN-";

	public static final String sqlLatenciesInsertQuery = "INSERT INTO latencies VALUES(?,?,?,?)";


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

		//Load messages first

		RemoteIterator<LocatedFileStatus> it = fileSystem.listLocatedStatus(new Path(folder + File.separator + jobID + File.separator+ workerFolderPrefix + File.separator + workerMessagesFolderName));
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

			Path msgsFolderPath = new Path(folder + jobID + File.separator + workerMessagesFolderPrefix + workerIndex);
			Path latenciesFolderPath = new Path(folder + jobID + File.separator + latenciesFolderPrefix + workerIndex);

			while(superstep < allSupersteps){

				Path msgsPath = new Path(msgsFolderPath + File.separator + superstepMessagesFilePrefix + superstep);
				Path latenciesPath = new Path(latenciesFolderPath + File.separator + superstepLatenciesFilePrefix + superstep);

				fSuperstep = Math.max(fSuperstep, superstep);

				MessagesBook messagesBook = null;
				InputStream msgsInput = null;

				LatenciesBook latenciesBook = null;
				InputStream latenciesInput = null;
				//					Path pt = new Path(folder + jobID + File.separator+ workerFolderPrefix + entry.getKey() + File.separator + current.getPath().getName());

				try {
					msgsInput = new FSDataInputStream(fileSystem.open(msgsPath).getWrappedStream());
					messagesBook = MessagesBook.parseFrom(msgsInput);

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

								sp = ConnectionFactory.prepare(sqlMsgsInsertIntoTempQuery.replace("%TABLE", table));
								sp.setInt(1, superstep);
								sp.setString(2, source);
								sp.setString(3,  target);
								sp.setInt(4, msgsNumber);
								sp.setDouble(5,  bytesNumber);

								sp.executeUpdate();
								sp.close();

							}catch(SQLException se){
								throw new SQLException(se);
							}finally{
								ConnectionFactory.closeStatement(sp);
							}
						}
					}

				}catch(FileNotFoundException fnfe) {
//					System.out.println("File " + msgsPath + " not found");
				}

				try {

					latenciesInput = new FSDataInputStream(fileSystem.open(latenciesPath).getWrappedStream());
					latenciesBook = LatenciesBook.parseFrom(latenciesInput);

					for (RecordedLatency message : latenciesBook.getRecordedLatencyList()) { 
						
						try{
							String source = message.getPingSource();
							String target = message.getPingTarget();
							long ping = message.getLatencyMs();
							
							ps = ConnectionFactory.prepare(sqlLatenciesInsertQuery);
							ps.setInt(1, superstep);
							ps.setString(2, source);
							ps.setString(3,  target);
							ps.setLong(4, ping);

							ps.executeUpdate();
							ps.close();

						}catch(MySQLIntegrityConstraintViolationException mlce) {
						}catch(SQLException se){
							throw new SQLException(se);
						}finally{
							ConnectionFactory.closeStatement(ps);
						}
					}

				}catch(FileNotFoundException fnfe) {
//					System.out.println("File " + latenciesPath + " not found");
				}

				if(msgsInput != null)
					msgsInput.close();	
				if(latenciesInput != null)				
					latenciesInput.close();

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


