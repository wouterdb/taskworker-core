import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintStream;
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

public class LineageTool {

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
								.setSeeds("172.16.4.23:9160")
								.setSocketTimeout(100000))
				.withConnectionPoolMonitor(new CountingConnectionPoolMonitor())
				.buildKeyspace(ThriftFamilyFactory.getInstance());

		context.start();
		Keyspace ks = context.getEntity();

		PrintStream p = new PrintStream("lineage.dot");
		printLineage(ks, p,
				UUID.fromString("195d58ff-2278-46bf-a40d-7b7c384a8655"));

	}

	private void printLineage(Keyspace ks, PrintStream out, UUID job)
			throws ConnectionException {
		out.println("digraph lineage{");

		OperationResult<CqlResult<String, String>> queue = ks
				.prepareQuery(CF_STANDARD1)
				.withCql("select id, worker_name from task where job_id = ?;")
				.asPreparedStatement().withUUIDValue(job).execute();
		
		for (Row<String, String> row : queue.getResult().getRows()) {
			UUID id = row.getColumns().getUUIDValue("id", null);
			String worker_name = row.getColumns().getStringValue("worker_name", null);

			out.println(String.format("n%s [label=\"%s\"];", id.toString().replaceAll("-", ""),worker_name));
			// CREATE TABLE task_parent (id uuid, job_id uuid, parent_id uuid,
			// PRIMARY KEY((job_id, id), parent_id))
			OperationResult<CqlResult<String, String>> lineage = ks
					.prepareQuery(CF_STANDARD1)
					.withCql(
							"select parent_id from task_parent where job_id = ? and id= ?;")
					.asPreparedStatement().withUUIDValue(job).withUUIDValue(id).execute();

			for (Row<String, String> rowz : lineage.getResult().getRows()) {

				UUID parent_id = rowz.getColumns().getUUIDValue("parent_id", null);
				out.println(String.format("n%s -> n%s", parent_id.toString().replaceAll("-", ""), id.toString().replaceAll("-", "")));
			}

			
		}
		
		out.println("}");


	}

	public static void main(String[] args) throws ConnectionException,
			IOException {
		(new LineageTool()).main();
	}
}