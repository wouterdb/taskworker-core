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

import java.util.logging.Logger;

import com.google.common.collect.ImmutableMap;
import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.connectionpool.NodeDiscoveryType;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.astyanax.connectionpool.impl.CountingConnectionPoolMonitor;
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.serializers.StringSerializer;
import com.netflix.astyanax.thrift.ThriftFamilyFactory;

public class Entities {
	private static Logger logger = Logger.getLogger(Entities.class.getCanonicalName());
	
	private static Keyspace cs = null;
	
	public static ColumnFamily<String, String> CF_STANDARD1 = ColumnFamily
            .newColumnFamily(
                    "Standard1", 
                    StringSerializer.get(),
                    StringSerializer.get());

	private static void createKeyspace(Keyspace keyspace) throws ConnectionException {
		// Using simple strategy
		keyspace.createKeyspace(ImmutableMap.<String, Object>builder()
		    .put("strategy_options", ImmutableMap.<String, Object>builder()
		        .put("replication_factor", "1")
		        .build())
		    .put("strategy_class", "SimpleStrategy")
		        .build()
		);
	}
	
	private static Keyspace setupCassandra() {
		AstyanaxContext<Keyspace> context = new AstyanaxContext.Builder()
		    .forCluster("TestCluster")
		    .forKeyspace("taskworker")
		    .withAstyanaxConfiguration(new AstyanaxConfigurationImpl()   
		        .setDiscoveryType(NodeDiscoveryType.RING_DESCRIBE)
		        .setCqlVersion("3.0.0")
		        .setTargetCassandraVersion("1.2")
		    )
		    .withConnectionPoolConfiguration(new ConnectionPoolConfigurationImpl("TaskWorkerConnectionPool")
		        .setPort(9160)
		        .setMaxConnsPerHost(1)
		        .setSeeds("127.0.0.1:9160")
		    )
		    .withConnectionPoolMonitor(new CountingConnectionPoolMonitor())
		    .buildKeyspace(ThriftFamilyFactory.getInstance());

		context.start();
		Keyspace ks = context.getEntity();
		
		try {
			ks.prepareQuery(CF_STANDARD1)
		    	.withCql("SELECT COUNT(*) FROM workflow;")
		    	.execute();
		} catch (ConnectionException e) {
			try {
				createKeyspace(ks);
				ks.prepareQuery(CF_STANDARD1)
					.withCql("CREATE TABLE parameter (task_id uuid PRIMARY KEY, name text, value blob)")
					.execute();
				
				ks.prepareQuery(CF_STANDARD1)
					.withCql("CREATE TABLE task (id uuid, parent_id uuid, workflow_id uuid, created_at timestamp, started_at timestamp, finished_at timestamp, type text, worker_name text, PRIMARY KEY (workflow_id, id))")
					.execute();
				
				ks.prepareQuery(CF_STANDARD1)
					.withCql("CREATE TABLE workflow (id uuid PRIMARY KEY, workflow_name text)")
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
}