/*
 * copyright 2012, gash
 * 
 * Gash licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package poke.server.storage.jdbc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import poke.constants.PokeNettyConstants;
import poke.server.Server;
import poke.server.storage.Storage;

import com.jolbox.bonecp.BoneCP;
import com.jolbox.bonecp.BoneCPConfig;

import eye.Comm.Document;
import eye.Comm.NameSpace;

public class DatabaseStorage implements Storage {
	protected static Logger logger = LoggerFactory.getLogger("database");

	protected Properties cfg;
	protected BoneCP cpool;

	/*protected DatabaseStorage() {
	}*/
	
	String appendPath = null;
	
	//Added for Integration.
	public DatabaseStorage() {
		if (cpool != null)
			return;

		try {
			logger.info("Database properties driver : "+PokeNettyConstants.DB_DRIVER+", url : "+PokeNettyConstants.DB_URL+", "
					+ "user : "+PokeNettyConstants.DB_USER+", pwd : "+PokeNettyConstants.DB_PASS);
			Class.forName(PokeNettyConstants.DB_DRIVER);
			BoneCPConfig config = new BoneCPConfig();
			config.setJdbcUrl(PokeNettyConstants.DB_URL);
			config.setUsername(PokeNettyConstants.DB_USER);
			config.setPassword(PokeNettyConstants.DB_PASS);
			config.setMinConnectionsPerPartition(5);
			config.setMaxConnectionsPerPartition(10);
			config.setPartitionCount(1);

			cpool = new BoneCP(config);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public DatabaseStorage(Properties cfg) {
		init(cfg);
	}

	@Override
	public void init(Properties cfg) {
		if (cpool != null)
			return;

		this.cfg = cfg;

		try {
			Class.forName(PokeNettyConstants.DB_DRIVER);
			BoneCPConfig config = new BoneCPConfig();
			config.setJdbcUrl(PokeNettyConstants.DB_URL);
			config.setUsername(PokeNettyConstants.DB_USER);
			config.setPassword(PokeNettyConstants.DB_PASS);
			config.setMinConnectionsPerPartition(5);
			config.setMaxConnectionsPerPartition(10);
			config.setPartitionCount(1);

			cpool = new BoneCP(config);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	

	/*
	 * (non-Javadoc)
	 * 
	 * @see gash.jdbc.repo.Repository#release()
	 */
	@Override
	public void release() {
		if (cpool == null)
			return;

		cpool.shutdown();
		cpool = null;
	}

	@Override
	public NameSpace getNameSpaceInfo(long spaceId) {
		NameSpace space = null;

		Connection conn = null;
		try {
			conn = cpool.getConnection();
			conn.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
			// TODO complete code to retrieve through JDBC/SQL
			// select * from space where id = spaceId
		} catch (Exception ex) {
			ex.printStackTrace();
			logger.error("failed/exception on looking up space " + spaceId, ex);
			try {
				conn.rollback();
			} catch (SQLException e) {
			}
		} finally {
			if (conn != null) {
				try {
					conn.close();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}

		return space;
	}

	@Override
	public List<NameSpace> findNameSpaces(NameSpace criteria) {
		List<NameSpace> list = null;

		Connection conn = null;
		try {
			conn = cpool.getConnection();
			conn.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
			// TODO complete code to search through JDBC/SQL
		} catch (Exception ex) {
			ex.printStackTrace();
			logger.error("failed/exception on find", ex);
			try {
				conn.rollback();
			} catch (SQLException e) {
			}
		} finally {
			if (conn != null) {
				try {
					conn.close();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}

		return list;
	}

	@Override
	public NameSpace createNameSpace(NameSpace space) {
		if (space == null)
			return space;

		Connection conn = null;
		try {
			conn = cpool.getConnection();
			conn.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
			// TODO complete code to use JDBC
		} catch (Exception ex) {
			ex.printStackTrace();
			logger.error("failed/exception on creating space " + space, ex);
			try {
				conn.rollback();
			} catch (SQLException e) {
			}

			// indicate failure
			return null;
		} finally {
			if (conn != null) {
				try {
					conn.close();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}

		return space;
	}

	@Override
	public boolean removeNameSpace(long spaceId) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean addDocument(String namespace, Document doc) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean removeDocument(String namespace, long docId) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean updateDocument(String namespace, Document doc) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public List<Document> findDocuments(String namespace, Document criteria) {
		// TODO Auto-generated method stub
		return null;
	}
	
	//Added for Integration.
	/**
	 * The method to delete a document from the database based on namespace and filename.
	 * @param pNameSpace
	 * @param pFileName
	 * @return
	 */
	public String deleteDocFromNode(String pNameSpace, String pFileName, String pQuery){
		logger.info("Inside the DatabaseStorage.deleteDocFromNode()...");
		Connection conn = null;
		PreparedStatement pstmt = null;
		String msg = "";
		try{
			conn = cpool.getConnection();
			pstmt = conn.prepareStatement(pQuery);
			pstmt.setString(1, pNameSpace);
			pstmt.setString(2, pFileName);
			
			if(pstmt.executeUpdate() > 0){
				msg = "Success";
			}else{
				msg = "Failure";
			}
		}catch(Exception ex){
			logger.warn("Some exception occured while deleting the file :: "+ex.getMessage());
		}
		
		logger.info("Exiting the DatabaseStorage.deleteDocFromNode()...");
		return msg;
	}
	
	/**
	 * The method to search for a document path if the doc is available.
	 * @param pQuery
	 * @param pNameSpace
	 * @return
	 */
	public String searchForDocument(String pQuery, String pNameSpace, String pFileName){
		logger.info("Inside the DatabaseStorage.searchForDocument()...");
		String path = "";
		Connection conn = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		
		try{
			conn = cpool.getConnection();
			pstmt = conn.prepareStatement(pQuery);
			pstmt.setString(1, pNameSpace);
			pstmt.setString(2, pFileName);
			rs = pstmt.executeQuery();
			
			if(rs != null){
				while(rs.next()){
					path = rs.getString(1);
				}
			}
			return path;
		}catch(Exception ex){
			ex.printStackTrace();
			logger.error("Failed during database fetch :: "+ex.getMessage());
			
		}finally{
			if (conn != null) {
				
				try {
					pstmt.close();
					conn.close();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}
		logger.info("Inside the DatabaseStorage.searchForDocument()...");
		return path;
	}
	
	/**
	 * The database method to add the documents to the database.
	 * @param fileid
	 * @param filename
	 * @param filepath
	 * @param namespace
	 * @param isowner
	 * @return
	 */
	public boolean addDoc(Long fileid,String filename, String filepath, String namespace,boolean isowner, boolean isreplicated)

	{

		Connection conn = null;
		PreparedStatement pstmt = null;
		try {
			conn = cpool.getConnection();
			String query = "insert into fileedgedatbase.fileedge_schema.fed_localfiles(fileid,filename,filepath,namespace,isowner,isreplicated) values(?,?,?,?,?,?)";
			pstmt = conn.prepareStatement(query);
			logger.info( "Running query to insert into localfiles " + query+", fileid : "+fileid+", filename : "+
					filename+", filepath : "+filepath+", namespace : "+namespace+", isowner : "+isowner+", isreplicated : "+isreplicated);
			Integer owner = isowner == true ? 1 : 0;
			Integer replicate = isreplicated == true ? 1 : 0; 
			pstmt.setLong(1, fileid);
			pstmt.setString(2, filename);
			pstmt.setString(3, filepath);
			pstmt.setString(4, namespace);
			pstmt.setInt(5, owner);
			pstmt.setInt(6, replicate);
			pstmt.execute();
			
			return true;
		} catch (Exception ex) {
			ex.printStackTrace();
			logger.error("failed/exception on looking up space " + ex);
			try {
				conn.rollback();
			} catch (SQLException e) {
			}
			
		} finally {
				
			if (conn != null) {
				
				try {
					
					conn.close();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}
		return false;
	}
	
	//Added for Integration.
	/**
	 * The method to add document marked for replication when the node comes up after the failure.
	 * @param fileId
	 * @param replicatedNode
	 * @return
	 */
	public boolean addReplicatedDoc(Long fileId, String replicatedNode)
	{
		Connection conn = null;
		PreparedStatement pstmt = null;
		try {
			conn = cpool.getConnection();

			String query = "insert into fileedgedatbase.fileedge_schema.fed_replication (fileid, replicatednode) values(?,?)";
			
			pstmt = conn.prepareStatement(query);
			logger.info( "Running query for fed_replication insert " + query+", replicatedNode : "+replicatedNode+", fileId : "+fileId);
			
			pstmt.setLong(1, fileId);
			pstmt.setString(2, replicatedNode);
			
			pstmt.execute();

			return true;
		} catch (Exception ex) {
			ex.printStackTrace();
			logger.error("failed/exception on looking up space " + ex);
			try {
				conn.rollback();
			} catch (SQLException e) {
			}
			
			return false;
		} finally {
			if (conn != null) {
				try {
					conn.close();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}
	}
	
	//Added for Integration.
	/**
	 * The method to update the replication status when the pendings docs are replicated across the nodes.
	 * @param fileId
	 * @param replicationStatus
	 * @return
	 */
	public boolean updateReplicationStatus(Long fileId, Boolean replicationStatus)
	{
		Connection conn = null;
		PreparedStatement pstmt = null;
		try {
			conn = cpool.getConnection();

			String query = "update fileedgedatbase.fileedge_schema.fed_localfiles set isReplicated = ? where fileid = ?";
			
			pstmt = conn.prepareStatement(query);
			logger.info( "Running update query for replication status : " + query +", isReplicated : "+replicationStatus+", fileId : "+fileId);
			
			pstmt.setInt(1, replicationStatus == true ? 1 : 0);
			pstmt.setLong(2, fileId);
			
			pstmt.executeUpdate();

			return true;
		} catch (Exception ex) {
			ex.printStackTrace();
			logger.error("failed/exception on looking up space " + ex);
			try {
				conn.rollback();
			} catch (SQLException e) {
			}
			
			return false;
		} finally {
			if (conn != null) {
				try {
					conn.close();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}
	}
	
	//Added for Integration.
	/*
	 * Get map of files along with the neighbors where they are already replicated 
	 * 	for those files which are not completely replicated.
	 */
	public Map<String, Set<String>> getListOfReplicatedNodes()
	{
		Connection conn = null;
		Statement stmt = null;
		try {
			conn = cpool.getConnection();

			String query = "select filename, filepath, namespace, replicatednode, fed_localfiles.fileId "
					+ "from fileedgedatbase.fileedge_schema.fed_localfiles, fileedgedatbase.fileedge_schema.fed_replication "
					+ "where fileedgedatbase.fileedge_schema.fed_localfiles.fileid = fileedgedatbase.fileedge_schema.fed_replication.fileid"
					+ " and isowner = 1 and isReplicated = 0"
					+ " union "
					+ "select filename, filepath, namespace, null, fileId from fileedgedatbase.fileedge_schema.fed_localfiles "
					+ "where isowner = 1 and isReplicated = 0 ";
			
			stmt = conn.createStatement();
			logger.info( "Query to get list of files which are not completely replicated : " + query);
			
			ResultSet rs = stmt.executeQuery(query);
			
			// Here we create a map with fileId+"|"+fileName+"|"+filepath+"|"+namespace as key (String) and value as 
			// 	Set<> of nodes where this file is already replicated.
			// This map is used by Server to determine what all neighboring nodes(where replication failed) 
			// 	a file should be sent to, so that it is fully replicated.
			HashMap<String, Set<String>> fileList = new HashMap<String, Set<String>>();
			while(rs.next())
			{
				String fileName = rs.getString(1);
				String filepath = rs.getString(2);
				String namespace = rs.getString(3);
				String replicatednode = rs.getString(4);
				Long fileId = rs.getLong(5);
				
				String key = fileId+"|"+fileName+"|"+filepath+"|"+namespace;
				
				//This Set contains the places where the file is replicated
				Set<String> set = fileList.get(key);
				
				//If already key exists then no need to create a new set
				if(set != null)
				{
					set.add(replicatednode);
//					fileList.put(key, set);
				}
				else
				{
					set = new HashSet<String>();
					set.add(replicatednode);
					fileList.put(key, set);
				}
			}
			
			return fileList;

		} catch (Exception ex) {
			ex.printStackTrace();
			logger.error("failed/exception on looking up space " + ex);
			try {
				conn.rollback();
			} catch (SQLException e) {
			}
			
			return Collections.emptyMap(); 
		} finally {
			if (conn != null) {
				try {
					conn.close();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}
	}
	
	//Added for Integration.
	/**
	 * Just a test method.
	 */
	public void testQuery ()
	{
		Connection conn = null;
		Statement stmt = null;
		try {
			conn = cpool.getConnection();

			String query = "select * from fileedgedatbase.fileedge_schema.fed_localfiles";
			
			stmt = conn.createStatement();
			ResultSet rs = stmt.executeQuery(query);
			
			System.out.println(rs.getRow());
			
			logger.info( "inside dbstorage" + query);
			
		} catch (Exception ex) {
			ex.printStackTrace();
			logger.error("failed/exception on looking up space " + ex);
			try {
				conn.rollback();
			} catch (SQLException e) {
			}
		} finally {
			if (conn != null) {
				try {
					conn.close();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}
	}
	
	//Added for Integration.
	/**
	 * Just to test.
	 * @param args
	 */
	public static void main(String[] args)
	{
		DatabaseStorage db = new DatabaseStorage();
		db.testQuery();
	}
}
