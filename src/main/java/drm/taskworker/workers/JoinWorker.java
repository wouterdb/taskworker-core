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

package drm.taskworker.workers;

import static drm.taskworker.Entities.cs;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;

import com.google.appengine.api.memcache.MemcacheService;
import com.google.appengine.api.memcache.MemcacheServiceFactory;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.CqlResult;
import com.netflix.astyanax.model.Row;

import drm.taskworker.Entities;
import drm.taskworker.Worker;
import drm.taskworker.tasks.AbstractTask;
import drm.taskworker.tasks.EndTask;
import drm.taskworker.tasks.Task;
import drm.taskworker.tasks.TaskResult;

/**
 * A generic worker that joins a workflow by collecting all tasks of a workflow
 * and sending out a new task with all previous tasks in it.
 * 
 * This worker collects all arguments of the joined tasks and sends out a new
 * task with the same arguments in list form.
 * 
 * This worker can only be used once in a workflow with the same worker name.
 * 
 * @author Bart Vanbrabant <bart.vanbrabant@cs.kuleuven.be>
 */
public class JoinWorker extends Worker {
	

	/**
	 * Creates a new work with the name blob-to-cache
	 */
	public JoinWorker(String workerName) {
		super(workerName);
	}

	@Override
	public TaskResult work(Task task) {
		TaskResult result = new TaskResult();
		result.setResult(TaskResult.Result.SUCCESS);
		return result;
	}


	/**
	 * Handle the end of workflow token by sending it to the same next hop.
	 */
	public TaskResult work(EndTask task) {
		logger.info("Joining workflow " + task.getWorkflowId().toString());
		TaskResult result = new TaskResult();

		//Fixme: perhaps write out intermediate table
		CqlResult<String, String> results;
		try {
			results = cs().prepareQuery(Entities.CF_STANDARD1)
					.withCql("SELECT id FROM task WHERE workflow_id = ? AND  worker_name = ? ALLOW FILTERING;").asPreparedStatement().withUUIDValue(task.getWorkflowId()).withStringValue(getName()).execute().getResult();
		} catch (ConnectionException e) {
			
			throw new RuntimeException(e);
		}
		
		// merge the arguments of the all tasks
		List<UUID> parents = new ArrayList<>();
		
		for (Row<String, String> row : results.getRows()) {
			ColumnList<String> c = row.getColumns();
			parents.add(c.getUUIDValue("id", null));
		}
		
		if(parents.isEmpty()){
			return result.setResult(TaskResult.Result.ERROR);
		}
		
		// create a new task with all joined arguments
		Task newTask = new Task(task,parents, this.getNextWorker());
		
		result.addNextTask(newTask);
		
		// also create a new endTask
		result.addNextTask(new EndTask(task, this.getNextWorker()));

		
		return result.setResult(TaskResult.Result.SUCCESS);
	}
}
