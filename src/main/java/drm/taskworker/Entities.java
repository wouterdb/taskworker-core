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

import java.util.UUID;

import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.connectionpool.NodeDiscoveryType;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.astyanax.connectionpool.impl.CountingConnectionPoolMonitor;
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.serializers.StringSerializer;
import com.netflix.astyanax.serializers.UUIDSerializer;
import com.netflix.astyanax.thrift.ThriftFamilyFactory;


public class Entities {
	private static Keyspace cs = null;
	
	public static ColumnFamily<UUID, String> CQL3_CF = ColumnFamily.
			newColumnFamily(
					"Cql3CF", 
					UUIDSerializer.get(), 
					StringSerializer.get());
	
	public static ColumnFamily<String, String> CF_STANDARD1 = ColumnFamily
            .newColumnFamily(
                    "Standard1", 
                    StringSerializer.get(),
                    StringSerializer.get());

	
	private static Keyspace setupCassandra() {
		AstyanaxContext<Keyspace> context = new AstyanaxContext.Builder()
		    .forCluster("TestCluster")
		    .forKeyspace("test")
		    .withAstyanaxConfiguration(new AstyanaxConfigurationImpl()   
		        .setDiscoveryType(NodeDiscoveryType.RING_DESCRIBE)
		        .setCqlVersion("3.0.0")
		        .setTargetCassandraVersion("1.2")
		    )
		    .withConnectionPoolConfiguration(new ConnectionPoolConfigurationImpl("MyConnectionPool")
		        .setPort(9160)
		        .setMaxConnsPerHost(1)
		        .setSeeds("127.0.0.1:9160")
		    )
		    .withConnectionPoolMonitor(new CountingConnectionPoolMonitor())
		    .buildKeyspace(ThriftFamilyFactory.getInstance());

		context.start();
		return context.getEntity();
	}
	
	public static Keyspace cs() {
		if (cs == null) {
			cs = setupCassandra();
		}
		
		return cs;
	}
}