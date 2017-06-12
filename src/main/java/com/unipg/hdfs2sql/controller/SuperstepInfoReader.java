/**
 * 
 */
package com.unipg.hdfs2sql.controller;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.unipg.hdfs2sql.db.ConnectionFactory;
import com.unipg.profilercommon.protoutils.SuperstepProtoInfo.Superstep;
import com.unipg.profilercommon.protoutils.SuperstepProtoInfo.SuperstepsInfo;

/**
 * @author maria
 *
 */
public class SuperstepInfoReader/* implements Reader */{

	public static void getData(String folder, String jobID, FileSystem fileSystem) throws IOException, SQLException, NullPointerException{

		InputStream inputFileSuperstepInfo = null;
		SuperstepsInfo superstepInfo = null;

		Path hpt = new Path(folder + File.separator + jobID + File.separator+"/SuperstepsTimes");
		inputFileSuperstepInfo = new FSDataInputStream(fileSystem.open(hpt).getWrappedStream());
		superstepInfo = SuperstepsInfo.parseFrom(inputFileSuperstepInfo);

		for(Superstep singleSuperstep: superstepInfo.getSuperstepList()){

			String insertSuperstepInfoQuery = "INSERT INTO superstepinfo(superstepId, millis) VALUES(?, ?)";

			//+ ""+ singleSuperstep.getNumero() + ","+(singleSuperstep.getDuration()*1000)+")";  //the last value is the relation type --> 0 : worker-host

			//Insert Data Into MySql Database
			PreparedStatement ps = null;

			try{
				ps = ConnectionFactory.prepare(insertSuperstepInfoQuery);
				ps.setLong(1, singleSuperstep.getNumero());
				ps.setDouble(2, singleSuperstep.getDuration());
				ps.executeUpdate();
			}catch(SQLException se){
				throw new SQLException(se);
			}finally{
				ConnectionFactory.closeStatement(ps);
			}

			inputFileSuperstepInfo.close();
		}
	}
}
