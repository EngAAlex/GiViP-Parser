package com.unipg.hdfs2sql.core;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;

import com.unipg.givip.common.protoutils.HierarchyProtoBook.SingleWorkerTreeHierarchy;
import com.unipg.givip.common.protoutils.HierarchyProtoBook.WorkerHierarchy;
import com.unipg.givip.common.protoutils.SuperstepProtoInfo.SuperstepsInfo;
import com.unipg.hdfs2sql.db.ConnectionFactory;
import com.unipg.hdfs2sql.utils.JobInfo;

/**
 * 
 */

/**
 * @author maria
 *
 */
public class DataExtractor {

	//  public boolean numberOrByte = false; //true = numberMatrix, false=bytesMatrix
	private String folder;
	private FileSystem fileSystem = null;

	public static final String[] scales = {"worker", "host", "rack"};

	public DataExtractor(String folder){
		setFileSystem();
		this.folder = folder;

	}

	private void setFileSystem(){
		Configuration conf=new Configuration();

		String hadoopConfFolder = System.getenv("HADOOP_CONF");

		conf.addResource(new Path(hadoopConfFolder + File.separator +"core-site.xml"));
		conf.addResource(new Path(hadoopConfFolder + File.separator +"hdfs-site.xml"));
		try {
			fileSystem = FileSystem.get(conf);
		} catch (IOException e1) {
			System.out.println("Please set HADOOP_CONF to the Hadoop configuration folder");
			System.exit(1);
			e1.printStackTrace();
		}
	}

	public FileSystem getFileSystem() {
		return fileSystem;
	}

	public void initializeDatabase(String dbName) throws SQLException{
		for(String s : scales)
			createMessageScaleTable(s);
		createSuperstepsInfoTable();
		createHierarchyTable();
		createSuperstepPerScaleInfoTable();
		createLatenciesTable();
	}

	public void getData(Reader r, String jobID, FileSystem fileSystem){
		r.getData(folder, jobID, fileSystem);
	}

	public ArrayList<JobInfo> getHDFSJobsList(){
		InputStream input = null;
		WorkerHierarchy hierarchy = null;
		Path hpt = null;
		SuperstepsInfo superstepInfo = null;
		//		String[] stringsWorker;
		//    String[] jobString;
		ArrayList<String> hostList;
		ArrayList<String> rackList;
		String host;
		String rack;
		String jobId;
		Date date;
		int supersteps = 0;
		int nWorkers = 0;
		ArrayList<JobInfo> jobList = new ArrayList<JobInfo>();
		try {
			RemoteIterator<LocatedFileStatus> fi = fileSystem.listLocatedStatus(new Path(folder));
			//      for(FileStatus jobFolder: fileSystem.listStatus(new Path
			//        (/*fileSystem.getHomeDirectory()+"/profiler/")*/ folder)))
			//for(File f : ){
			while(fi.hasNext()){
				LocatedFileStatus f = fi.next();
				if(!f.isDirectory())
					continue;
				//        jobString = jobFolder.getPath().toString().split("/");
				//jobId = jobString[jobString.length-1];
				//        modificationTime = CalendarUtils.ConvertMilliSecondsToFormattedDate(jobFolder.getModificationTime());    	
				jobId = f.getPath().getName() != null ? f.getPath().getName() : null;
				date = new Date(f.getModificationTime());

				hpt = new Path(folder +jobId+ File.separator + "SuperstepsTimes");
				try {
					input = new FSDataInputStream(fileSystem.open(hpt).getWrappedStream());
				} catch (IOException e) {
					System.out.println("Error while importing SuperstepTimes of " + jobId);
					continue;
				}
				try {
					superstepInfo = SuperstepsInfo.parseFrom(input);
					supersteps = superstepInfo.getSuperstepCount() - 1;
				} catch (IOException e) {
					System.out.println("Error while importing SuperstepInfo of " + jobId);
					continue;
				}

				hpt = new Path(folder + jobId+ File.separator + "JobHierarchy");
				try {
					input = new FSDataInputStream(fileSystem.open(hpt).getWrappedStream());
				} catch (IOException e) {
					System.out.println("Error while importing JobHierarchy of" + jobId);
					continue;
				}
				try {
					hierarchy = WorkerHierarchy.parseFrom(input);
				} catch (IOException e) {
					System.out.println("Error while importing WorkerHierarchy of " + jobId);
					//					e.printStackTrace();
					continue;
				}

				hostList = new ArrayList<String>();
				rackList = new ArrayList<String>();
				for(SingleWorkerTreeHierarchy singleWorker: hierarchy.getSingleWorkerTreeHierarchyList()){
					host = "HostName" + File.separator + singleWorker.getHostName();
					rack = "RackName" + File.separator + singleWorker.getRack();
					if (!hostList.contains(host)){
						hostList.add(host);
					}
					if(!rackList.contains(rack)){
						rackList.add(rack);
					}
				}

				//Check how many workers executed the job
				RemoteIterator<LocatedFileStatus> wf = fileSystem. listLocatedStatus(new Path(folder + jobId + File.separator + "WorkerData"));
				while(wf.hasNext()){
					wf.next();
					nWorkers++;
					//					stringsWorker = cCurrent.getPath().toString().split("/");
					//					stringsWorker = stringsWorker[stringsWorker.length-1].split("-");
				}

				jobList.add(new JobInfo(jobId,date,nWorkers,hostList.size(),rackList.size(),supersteps));
				nWorkers = 0;
			}
		} catch (IllegalArgumentException | IOException e1) {
			e1.printStackTrace();
		}

		return jobList;
	}

	public void createDatabase(String dbName) throws SQLException, NullPointerException{
		String query = "CREATE DATABASE IF NOT EXISTS " + dbName;
		PreparedStatement ps = null;
		try{
			ps = ConnectionFactory.prepare(query);
			ps.executeUpdate();
		}catch(SQLException se){
			//			ConnectionFactory.closeStatement(ps);			
			throw new SQLException(se);
		}finally{
			ConnectionFactory.closeStatement(ps);
		}
		//          statement = connection.createStatement();
		//          statement.executeUpdate(query);
		//          SQLWarning warning = statement.getWarnings();

		//      finally {
		//          DbUtil.close(statement);
		//          DbUtil.close(connection);
		//      }
	}

	private void createMessageScaleTable(String scale) throws SQLException{
		String messagesTableCreationQuery = 
				"CREATE TABLE IF NOT EXISTS messages_by_"+ scale +
				"(superstepId INTEGER not NULL, " +
				" source VARCHAR(255), " + 
				" target VARCHAR(255), " + 
				" nMessages BIGINT, " + 
				" bytes BIGINT)";
		
		String tmpMessagesTableCreationQuery = 
				"CREATE TABLE IF NOT EXISTS temp_messages_by_"+ scale +
				"(superstepId INTEGER not NULL, " +
				" source VARCHAR(255), " + 
				" target VARCHAR(255), " + 
				" nMessages BIGINT, " + 
				" bytes BIGINT)";
		
		PreparedStatement ps = null;
		try{
			ps = ConnectionFactory.prepare(messagesTableCreationQuery);
			ps.executeUpdate();
			
			ps.close();
			
			ps = ConnectionFactory.prepare(tmpMessagesTableCreationQuery);
			ps.executeUpdate();
			
			ps.close();
			
		}catch(SQLException se){
			//			ConnectionFactory.closeStatement(ps);							
			throw new SQLException(se);
		}finally{
			ConnectionFactory.closeStatement(ps);
		}		//      try {
		//        dao.execute(jobId, messagesTable);
		//      } catch (Exception e) {
		//        // TODO Auto-generated catch block
		//        e.printStackTrace();
		//      }
	}
	
	private void createLatenciesTable() throws SQLException{
		String latenciesTableCreationQuery = 
				"CREATE TABLE IF NOT EXISTS latencies" +
				"(id_superstep INTEGER not NULL, " +
				" source VARCHAR(255), " + 
				" target VARCHAR(255), " + 
				" latency BIGINT,"
				+ "PRIMARY KEY (id_superstep, source, target)";
		
		PreparedStatement ps = null;
		try{
			ps = ConnectionFactory.prepare(latenciesTableCreationQuery);
			ps.executeUpdate();
			
			ps.close();
			
		}catch(SQLException se){
			//			ConnectionFactory.closeStatement(ps);							
			throw new SQLException(se);
		}finally{
			ConnectionFactory.closeStatement(ps);
		}		//      try {
		//        dao.execute(jobId, messagesTable);
		//      } catch (Exception e) {
		//        // TODO Auto-generated catch block
		//        e.printStackTrace();
		//      }
	}


	public void createSuperstepPerScaleInfoTable() throws SQLException{

		String superstepPerWorkerTableCreationQuery = 
				"CREATE TABLE  IF NOT EXISTS superstepinfo_per_%SCALE (" +
						"id_superstep INT NOT NULL,"+
						"element_index %INDEX_FORMAT NOT NULL,"+
						"computation_time DOUBLE NULL DEFAULT 0,"+
						"processed_vertices INT NULL DEFAULT 0,"+
						"PRIMARY KEY (id_superstep, element_index))";

		PreparedStatement ps = null;
		try{
			for(String s : scales){
				if(s.equals("worker"))
					ps = ConnectionFactory.prepare(superstepPerWorkerTableCreationQuery.replace("%SCALE", s).replace("%INDEX_FORMAT", "INT"));
				else
					ps = ConnectionFactory.prepare(superstepPerWorkerTableCreationQuery.replace("%SCALE", s).replace("%INDEX_FORMAT", "VARCHAR(100)"));
				ps.executeUpdate();
				ConnectionFactory.closeStatement(ps);	
			}
		}catch(SQLException se){
			//			ConnectionFactory.closeStatement(ps);			
			throw new SQLException(se);
		}finally{
			ConnectionFactory.closeStatement(ps);	
		}

	}

	public void createJobsTable() throws SQLException{
		String messagesTableCreationQuery = 
				"CREATE TABLE IF NOT EXISTS JobsList "+
						"(jobId VARCHAR(255) not NULL PRIMARY KEY, " +
						" date DATE, " + 
						" nWorkers INTEGER, " + 
						" nHost INTEGER, " + 
						" nRack INTEGER, " +
						" nSuperstep INTEGER)";      
		PreparedStatement ps = null;
		try{
			ps = ConnectionFactory.prepare(messagesTableCreationQuery);
			ps.executeUpdate();
		}catch(SQLException se){
			//			ConnectionFactory.closeStatement(ps);			
			throw new SQLException(se);
		}finally{
			ConnectionFactory.closeStatement(ps);	
		}

	}

	private void createSuperstepsInfoTable() throws SQLException{
		String superstepInfoTableQuery = 
				"CREATE TABLE IF NOT EXISTS superstepinfo "+
						"(superstepId INTEGER not NULL, " +
						" millis DOUBLE," +
						" computation VARCHAR(100))"; 
		PreparedStatement ps = null;
		try{
			ps = ConnectionFactory.prepare(superstepInfoTableQuery);
			ps.executeUpdate();
		}catch(SQLException se){
			//			ConnectionFactory.closeStatement(ps);				
			throw new SQLException(se);
		}finally{
			ConnectionFactory.closeStatement(ps);	
		}	
	}

	private void createHierarchyTable() throws SQLException{
		String hierarchyCreationTableQuery = 
				"CREATE TABLE IF NOT EXISTS hierarchy "+
						"(child VARCHAR(255), " + 
						" father VARCHAR(255), " + 
						" typeOfRelation INTEGER, "+
						" PRIMARY KEY(child,father,typeOfRelation))";
		PreparedStatement ps = null;
		try{
			ps = ConnectionFactory.prepare(hierarchyCreationTableQuery);
			ps.executeUpdate();
		}catch(SQLException se){
			//			ConnectionFactory.closeStatement(ps);			
			throw new SQLException(se);
		}finally{
			ConnectionFactory.closeStatement(ps);	
		}		
	}


	public void addJobToTable(JobInfo job) throws Exception{
		String addJobInfo = 
				"INSERT INTO JobsList "+
						"VALUES(?,?,?,?,?,?)";

		PreparedStatement st = null;
		try{
			st = ConnectionFactory.prepare(addJobInfo);		
			st.setString(1, job.id);
			st.setDate(2, job.date); //Date(job.date.getYear(), job.date.getMonthOfYear(), job.date.getDayOfMonth()));// Date.valueOf(job.date.toString()));
			st.setInt(3, job.nWorkers);
			st.setInt(4, job.nHost);
			st.setInt(5, job.nRack);
			st.setInt(6, job.nSupersteps);
			st.executeUpdate();
		}catch(SQLException se){
			//			ConnectionFactory.closeStatement(st);				
			throw new SQLException(se);
		}finally{
			ConnectionFactory.closeStatement(st);	
		}	
	}
}
