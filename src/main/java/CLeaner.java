import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.connectionpool.NodeDiscoveryType;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.astyanax.connectionpool.impl.CountingConnectionPoolMonitor;
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.CqlResult;
import com.netflix.astyanax.model.Row;
import com.netflix.astyanax.serializers.StringSerializer;
import com.netflix.astyanax.thrift.ThriftFamilyFactory;

public class CLeaner {

	public static ColumnFamily<String, String> CF_STANDARD1 = ColumnFamily
			.newColumnFamily("Standard1", StringSerializer.get(),
					StringSerializer.get());

	private long cutoff;
	/**
	 * @param args
	 * @throws ConnectionException 
	 * @throws IOException 
	 */
	public void main() throws ConnectionException, IOException {
				
		AstyanaxContext<Keyspace> context = new AstyanaxContext.Builder()
				.forCluster("TestCluster")
				.forKeyspace("taskworker")
				.withAstyanaxConfiguration(
						new AstyanaxConfigurationImpl()
								.setDiscoveryType(
										NodeDiscoveryType.RING_DESCRIBE)
								.setCqlVersion("3.0.0")
								.setTargetCassandraVersion("1.2"))
				.withConnectionPoolConfiguration(
						new ConnectionPoolConfigurationImpl(
								"TaskWorkerConnectionPool").setPort(9160)
								.setMaxConnsPerHost(30)
								.setSeeds("172.16.4.23:9160").setSocketTimeout(100000))
				.withConnectionPoolMonitor(new CountingConnectionPoolMonitor())
				.buildKeyspace(ThriftFamilyFactory.getInstance());

		context.start();
		Keyspace ks = context.getEntity();

		clearPrios(ks);
		clearQueue(ks);
		
	}




	private void clearQueue(Keyspace ks) throws ConnectionException {
		OperationResult<CqlResult<String, String>> queue = ks.prepareQuery(CF_STANDARD1)
		.withCql("select * from task_queue;").execute();
		
		
		for(Row<String,String> row:queue.getResult().getRows()){
			boolean removed = row.getColumns().getBooleanValue("removed", false);
			if(removed){
				String queue_id = row.getColumns().getStringValue("queue_id", "");
				UUID uuid = row.getColumns().getUUIDValue("id", null);
				System.out.println("stale queue item: " + queue_id +" " + uuid);
				ks.prepareQuery(CF_STANDARD1)
						.withCql("delete from task_queue where queue_id=? and id=?;").asPreparedStatement().withStringValue(queue_id).withUUIDValue(uuid).execute();
			}
				
			
		}
		
	}




	private void clearPrios(Keyspace ks) throws ConnectionException {
		OperationResult<CqlResult<String, String>> jobs = ks.prepareQuery(CF_STANDARD1)
		.withCql("select job_id from job where finished = true;").execute();
	
		String[] workers = {"archive","csv-to-task","end","join","template-to-xml","xsl-fo-render","zip-files"};
		
		for(Row<String,String> row:jobs.getResult().getRows()){
			UUID uuid = row.getColumns().getUUIDValue("job_id", null);
			System.out.println(uuid.toString());
			for (String worker : workers) {
				ks.prepareQuery(CF_STANDARD1)
				.withCql("delete from priorities where worker_type=? and job_id=?;").asPreparedStatement().withStringValue(worker).withUUIDValue(uuid).execute();
			}
		}
	}


	

	public static void main(String[] args) throws ConnectionException, IOException {
		(new CLeaner()).main();
	}
}