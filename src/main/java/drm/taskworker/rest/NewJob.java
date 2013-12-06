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

import java.util.Date;
import java.util.Map;
import java.util.Map.Entry;

import javax.ws.rs.Consumes;
import javax.ws.rs.FormParam;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.core.MediaType;

import com.google.gson.Gson;

import drm.taskworker.Job;
import drm.taskworker.Service;
import drm.taskworker.tasks.Task;

@Path("/newjob/{workflow}")
public class NewJob {
    /**
     * Method handling HTTP GET requests.
     */
    @POST
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    public String newJob(@PathParam("workflow") String workflowName,
    		@FormParam("start_after") String start_after,
    		@FormParam("finish_before") String finish_before,
    		@FormParam("arg0") String arg0,
    		@FormParam("json_data") String json_data) {
		// get the delay
		int after = Integer.valueOf(start_after);
		int before = Integer.valueOf(finish_before);
		
		// create a workflow and save it
		Job job = new Job(workflowName);
		if (after > 0) {
			job.setStartAfter(new Date(after));	
		}
		if (before > 0) {
			job.setFinishBefore(new Date(before));
		}
		
		Task task = job.newStartTask();
		if (json_data != null) {
			Gson g = new Gson();
			@SuppressWarnings("unchecked")
			Map<String, Object> data = (Map<String, Object>) g.fromJson(json_data, Map.class);
			
			for (Entry<String,Object> entry : data.entrySet()) {
				task.addParam(entry.getKey(), entry.getValue());
			}
		} else if (arg0 != null) {
			task.addParam("arg0", arg0);
		} else {
			throw new IllegalArgumentException("Either arg0 or a json string with arguments should be submitted");
		}
		
		Service.get().addJob(job);
		
		return "[]";
    }
}
