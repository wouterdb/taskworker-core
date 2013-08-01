/*
    Copyright 2013 KU Leuven Research and Development - iMinds - Distrinet

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.

    Administrative Contact: dnet-project-office@cs.kuleuven.be
    Technical Contact: bart.vanbrabant@cs.kuleuven.be
 */

package drm.taskworker;

import java.io.ByteArrayInputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.logging.Logger;

import org.yaml.snakeyaml.Yaml;

import com.google.common.collect.ImmutableMap;
import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.Serializer;
import com.netflix.astyanax.connectionpool.NodeDiscoveryType;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.astyanax.connectionpool.impl.CountingConnectionPoolMonitor;
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ConsistencyLevel;
import com.netflix.astyanax.serializers.AbstractSerializer;
import com.netflix.astyanax.serializers.ByteBufferOutputStream;
import com.netflix.astyanax.serializers.StringSerializer;
import com.netflix.astyanax.thrift.ThriftFamilyFactory;

import drm.taskworker.monitoring.Statistic;

public class Entities {

	public static final Serializer<List<Statistic>> STATS_SERIALISER;

	static {
		STATS_SERIALISER = new YamlSerialiser<List<Statistic>>();
	}

	private static Logger logger = Logger.getLogger(Entities.class
			.getCanonicalName());

	private static Keyspace cs = null;

	public static ColumnFamily<String, String> CF_STANDARD1 = ColumnFamily
			.newColumnFamily("Standard1", StringSerializer.get(), StringSerializer.get());

	private static void createKeyspace(Keyspace keyspace)
			throws ConnectionException {
		// Using simple strategy
		keyspace.createKeyspace((ImmutableMap.<String, Object>builder()
				.put("strategy_options", ImmutableMap.<String, Object>builder()
						.put("datacenter1", "1")
			            .build())
			        .put("strategy_class",     "NetworkTopologyStrategy")
			        .build())
		);
		
		keyspace.createColumnFamily(CF_STANDARD1, ImmutableMap.<String, Object>builder()
                .put("default_validation_class", "LongType")
                .put("key_validation_class",     "UTF8Type")
                .put("comparator_type",          "UTF8Type")
                .build());
	}

	private static synchronized Keyspace setupCassandra() {
		String seed = System.getProperty("dreamaas.cassandra.seed");
		if (seed == null || seed.isEmpty()) {
			seed = "127.0.0.1";
		}
		AstyanaxContext<Keyspace> context = new AstyanaxContext.Builder()
			.forCluster("Test Cluster").
			forKeyspace("taskworker")
			.withAstyanaxConfiguration(new AstyanaxConfigurationImpl()
				.setDiscoveryType(NodeDiscoveryType.RING_DESCRIBE)
				.setCqlVersion("3.0.0").setTargetCassandraVersion("1.2"))
				.withConnectionPoolConfiguration(
					new ConnectionPoolConfigurationImpl("TaskWorkerConnectionPool")
						.setPort(9160)
						.setMaxConnsPerHost(1)
						.setSeeds(seed))
				.withConnectionPoolMonitor(new CountingConnectionPoolMonitor())
				.buildKeyspace(ThriftFamilyFactory.getInstance());

		context.start();
		@SuppressWarnings("deprecation")
		Keyspace ks = context.getEntity();

		try {
			ks.prepareQuery(CF_STANDARD1)
					.withCql("SELECT COUNT(*) FROM workflow;").execute();
		} catch (ConnectionException e) {
			try {
				createKeyspace(ks);
				ks.prepareQuery(CF_STANDARD1).setConsistencyLevel(ConsistencyLevel.CL_ALL)
						.withCql(
						"CREATE TABLE parameter (task_id uuid, name text, value blob, PRIMARY KEY(task_id, name))")
						.execute();

				/*
				 * type:
				 * 			0 - normal task
				 * 			1 - end task
				 * 			100 - deleted
				 */
				ks.prepareQuery(CF_STANDARD1).setConsistencyLevel(ConsistencyLevel.CL_ALL).withCql(
						"CREATE TABLE task (id uuid, job_id uuid, created_at timestamp, type int, worker_name text, PRIMARY KEY (job_id, id))")
						.execute();
				ks.prepareQuery(CF_STANDARD1).setConsistencyLevel(ConsistencyLevel.CL_ALL).withCql(
						"CREATE INDEX task_worker ON task(worker_name)")
						.execute();
				
				ks.prepareQuery(CF_STANDARD1).setConsistencyLevel(ConsistencyLevel.CL_ALL).withCql(
						"CREATE TABLE task_timing (id uuid, started_at timestamp, finished_at timestamp, PRIMARY KEY (id))")
						.execute();
				
				ks.prepareQuery(CF_STANDARD1).setConsistencyLevel(ConsistencyLevel.CL_ALL).withCql(
						"CREATE TABLE task_queue (id uuid, queue_id text, leased_until timestamp, type int, removed boolean, PRIMARY KEY(queue_id, type, id))")
						.execute();

				ks.prepareQuery(CF_STANDARD1).setConsistencyLevel(ConsistencyLevel.CL_ALL).withCql(
						"CREATE TABLE task_parent (id uuid, job_id uuid, parent_id uuid, PRIMARY KEY(job_id, id))")
						.execute();

				ks.prepareQuery(CF_STANDARD1).setConsistencyLevel(ConsistencyLevel.CL_ALL).withCql(
						"CREATE INDEX task_parent_id ON task_parent (parent_id)")
						.execute();

				ks.prepareQuery(CF_STANDARD1).setConsistencyLevel(ConsistencyLevel.CL_ALL).withCql(
						"CREATE TABLE job (job_id uuid, start_task_id uuid, workflow_name text, start_after timestamp, finish_before timestamp, finished boolean, started boolean, failed boolean, started_at timestamp, finished_at timestamp, stats blob, PRIMARY KEY(job_id, start_after, finish_before))")
						.execute();

				ks.prepareQuery(CF_STANDARD1).setConsistencyLevel(ConsistencyLevel.CL_ALL).withCql(
						"CREATE INDEX job_started ON job (started)")
						.execute();

				ks.prepareQuery(CF_STANDARD1).setConsistencyLevel(ConsistencyLevel.CL_ALL).withCql(
						"CREATE INDEX job_finished ON job (finished)")
						.execute();
				
				ks.prepareQuery(CF_STANDARD1).setConsistencyLevel(ConsistencyLevel.CL_ALL).withCql(
						"CREATE TABLE priorities (job_id uuid, worker_type text, weight float, PRIMARY KEY(worker_type, job_id))")
						.execute();
				
			} catch (ConnectionException ee) {
				logger.warning("Unable to create keyspace and schema");
				throw new IllegalStateException(ee);
			}
		}

		return ks;
	}

	public static Keyspace cs() {
		if (cs == null) {
			cs = setupCassandra();
		}

		return cs;
	}
	
	public static class YamlSerialiser<T> extends AbstractSerializer<T> {

		@Override
		public ByteBuffer toByteBuffer(T obj) {
			Yaml x = new Yaml();

			ByteBufferOutputStream out = new ByteBufferOutputStream();

			Writer w = new OutputStreamWriter(out);
			x.dump(obj, w);

			return out.getByteBuffer();
		}

		@SuppressWarnings("unchecked")
		@Override
		public T fromByteBuffer(ByteBuffer byteBuffer) {

			Yaml x = new Yaml();
			ByteArrayInputStream in = new ByteArrayInputStream(
					byteBuffer.array());
			return (T) x.load(in);
		}
	}

}
