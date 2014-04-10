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

package drm.taskworker.rest;

import static drm.taskworker.Entities.cs;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import javax.json.JsonValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.model.CqlResult;
import com.netflix.astyanax.model.Row;
import com.netflix.astyanax.serializers.IntegerSerializer;

import drm.taskworker.Entities;
import drm.taskworker.Job;
import drm.taskworker.Service;
import drm.taskworker.tasks.Task;

@Path("livestats")
public class LiveStatsRoot {

	private static Logger logger = Logger.getLogger(LiveStatsRoot.class
			.getCanonicalName());

	/**
	 * Method handling HTTP GET requests.
	 */
	@GET
	@Produces(MediaType.APPLICATION_JSON)
	public String getStats() {
		JsonObjectBuilder builder = Json.createObjectBuilder();
		try {
			OperationResult<CqlResult<String, String>> jobs = cs()
					.prepareQuery(Entities.CF_STANDARD1)
					.withCql("select job_id from job where finished = false;")
					.execute();

			Map<String, Long> workerStats = new HashMap<>();

			builder.add("jobs", jobs.getResult().getRows().size());

			for (Row<String, String> row : jobs.getResult().getRows()) {
				UUID uuid = row.getColumns().getUUIDValue("job_id", null);
				collectStatsForJob(workerStats, uuid);
			}
			JsonObjectBuilder statsBuilder = Json.createObjectBuilder();

			for (Map.Entry<String, Long> entries : workerStats.entrySet()) {
				statsBuilder.add(entries.getKey(), entries.getValue());
			}

			builder.add("stats", statsBuilder);

		} catch (ConnectionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		JsonObject jsonJob = builder.build();
		return jsonJob.toString();
	}

	private void collectStatsForJob(Map<String, Long> workerStats, UUID uuid) {
		Job job = Job.load(uuid);
		Set<String> workertypes = job.getWorkflowConfig().getSteps().keySet();

		for (String workertype : workertypes) {
			try {
				Long i = workerStats.get(workertype);
				if (i == null)
					i = 0L;
				workerStats.put(workertype, i + getQueued(workertype, uuid));
			} catch (Exception e) {
				logger.log(Level.SEVERE, "Unable to load worker stats", e);
				e.printStackTrace();
			}
		}

	}

	private long getQueued(String workertype, UUID jobId)
			throws ConnectionException {

		String qid = workertype + "-" + jobId.toString();

		OperationResult<CqlResult<String, String>> result = cs()
				.prepareQuery(Entities.CF_STANDARD1)
				.withCql(" select count(1) from task_queue where queue_id=?")
				.asPreparedStatement().withStringValue(qid).execute();

		long queucount = result.getResult().getRows().getRowByIndex(0)
				.getColumns().getLongValue("count", 0L);

		return queucount;

	}
}
