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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.google.appengine.api.memcache.MemcacheService;
import com.google.appengine.api.memcache.MemcacheServiceFactory;

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
	private MemcacheService cacheService = MemcacheServiceFactory.getMemcacheService();

	/**
	 * Creates a new work with the name blob-to-cache
	 */
	public JoinWorker(String workerName) {
		super(workerName);
	}

	@Override
	public TaskResult work(Task task) {
		TaskResult result = new TaskResult();
		
		String joinKey = createJoinKey(task);
		
		// increment the join counter to generate a sequence number
		long sequence = cacheService.increment(joinKey, 1, new Long(0));
		
		// store the task
		String taskKey = joinKey + "-" + sequence;
		cacheService.put(taskKey, task);
		
		logger.info("Stored task for " + joinKey + " under key " + taskKey);

		result.setResult(TaskResult.Result.SUCCESS);
		return result;
	}

	public String createJoinKey(AbstractTask task) {
		String wfId = task.getWorkflowId().toString();
		String joinKey = this.getName() + "-" + wfId;
		return joinKey;
	}

	/**
	 * Handle the end of workflow token by sending it to the same next hop.
	 */
	public TaskResult work(EndTask task) {
		logger.info("Joining workflow " + task.getWorkflowId().toString());
		TaskResult result = new TaskResult();
		
		String joinKey = this.createJoinKey(task);
		long sequence = (long)cacheService.get(joinKey);
		
		// generate the list of keys
		List<String> taskKeys = new ArrayList<>();
		for (int i = 1; i <= sequence; i++) {
			taskKeys.add(joinKey + "-" + i);
			logger.info("Joining " + joinKey + "-" + i);
		}
		
		// fetch all tasks
		Map<String, Object> tasks = this.cacheService.getAll(taskKeys);
		
		// merge the arguments of the all tasks
		Map<String, List<Object>> varMap = new HashMap<>();
		List<AbstractTask> parents = new ArrayList<>();
		for (Entry<String, Object> entry: tasks.entrySet()) {
			Task t = (Task)entry.getValue();
			if (t == null) {
				logger.severe("Fetched an empty join tasks from cache! " + entry.getKey());
				continue;
			}
			
			parents.add(t);
			for (String key : t.getParamNames()) {
				if (!varMap.containsKey(key)) {
					varMap.put(key, new ArrayList<Object>());
				}
				varMap.get(key).add(t.getParam(key));
			}
		}
		
		
		if(parents.isEmpty()){
			return result.setResult(TaskResult.Result.ERROR);
		}
		
		// create a new task with all joined arguments
		Task newTask = new Task(parents, this.getNextWorker());
		
		for (String varName : varMap.keySet()) {
			newTask.addParam(varName, varMap.get(varName));
		}
		
		result.addNextTask(newTask);
		
		// also create a new endTask
		result.addNextTask(new EndTask(task, this.getNextWorker()));

		this.cacheService.deleteAll(taskKeys);
		
		return result.setResult(TaskResult.Result.SUCCESS);
	}
}
