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
import com.netflix.astyanax.serializers.IntegerSerializer;

import drm.taskworker.Entities;
import drm.taskworker.Job;
import drm.taskworker.tasks.Task;

@Path("/livestats/{id}")
public class LiveStats {

	private static Logger logger = Logger.getLogger(LiveStats.class
			.getCanonicalName());

	/**
	 * Method handling HTTP GET requests.
	 */
	@GET
	@Produces(MediaType.APPLICATION_JSON)
	public String getStats(@PathParam("id") String jobId) {
		UUID id = UUID.fromString(jobId);

		drm.taskworker.Job job = drm.taskworker.Job.load(id);

		Set<String> workertypes = job.getWorkflowConfig().getSteps().keySet();

		JsonObjectBuilder builder = Json.createObjectBuilder()
				.add("id", job.getJobId().toString())
				.add("workflow", job.getWorkflowName())
				.add("start_after", job.getStartAfter() / 1000)
				.add("finish_before", job.getFinishBefore() / 1000)
				.add("started", job.isStarted())
				.add("finished", job.isFinished())
				.add("failed", job.isFailed());

		if (job.isStarted()) {
			builder.add("started_at", job.getStartAt().getTime() / 1000);
		}
		if (job.isFinished()) {
			builder.add("finished_at", job.getFinishedAt().getTime() / 1000);
		} else {

			JsonObjectBuilder statsBuilder = Json.createObjectBuilder();
			for (String workertype : workertypes) {
				try {
					statsBuilder.add(workertype, getLiveStats(workertype, id));
				} catch (Exception e) {
					logger.log(Level.SEVERE, "Unable to load worker stats", e);
					e.printStackTrace();
				}
			}
			builder.add("stats", statsBuilder);
		}

		JsonObject jsonJob = builder.build();
		return jsonJob.toString();
	}

	private JsonValue getLiveStats(String workertype, UUID jobId)
			throws ConnectionException {
		
		IntegerSerializer x = IntegerSerializer.get();

		String qid = workertype + "-" + jobId.toString();

		OperationResult<CqlResult<String, String>> result = cs()
				.prepareQuery(Entities.CF_STANDARD1)
				.withCql("select count(1) from task where job_id = ?")
				.asPreparedStatement().withUUIDValue(jobId).execute();

		long taskcount = result.getResult().getRows().iterator().next().getColumns().getLongValue("count", 0L);

		result = cs().prepareQuery(Entities.CF_STANDARD1)
				.withCql(" select count(1) from task_queue where queue_id=?")
				.asPreparedStatement().withStringValue(qid).execute();

		long queucount = result.getResult().getRows().getRowByIndex(0).getColumns().getLongValue("count", 0L);
/*
		result = cs()
				.prepareQuery(Entities.CF_STANDARD1)
				.withCql(
						"select count(1) from task_queue where queue_id=? and leased_until is not null")
				.asPreparedStatement().withStringValue(qid).execute();

		long workingcount =  result.getResult().getRows().getRowByIndex(0).getColumns().getLongValue("count", 0L);
*/
		JsonObjectBuilder statsBuilder = Json.createObjectBuilder();
		statsBuilder.add("done", taskcount - queucount);
		statsBuilder.add("waiting", queucount );
		//statsBuilder.add("working", workingcount);
		return statsBuilder.build();

	}
}
