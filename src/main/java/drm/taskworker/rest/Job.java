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

import java.util.UUID;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;


@Path("/job/{id}")
public class Job {
    /**
     * Method handling HTTP GET requests.
     */
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public String newJob(@PathParam("id")String jobId) {
        UUID id = UUID.fromString(jobId);
    	
        drm.taskworker.Job job = drm.taskworker.Job.load(id);
        
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
        }
                
        JsonObject jsonJob = builder.build();
        return jsonJob.toString();
    }
}
