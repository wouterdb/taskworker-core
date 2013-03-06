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

import java.io.IOException;
import java.util.logging.Logger;

import drm.taskworker.queue.Queue;
import drm.taskworker.tasks.AbstractTask;
import drm.taskworker.tasks.EndTask;
import drm.taskworker.tasks.Task;
import drm.taskworker.tasks.WorkflowInstance;

/**
 * This class implements a workflow service. This service should be stateless
 * with all state in the queues and storage.
 *
 * @author Bart Vanbrabant <bart.vanbrabant@cs.kuleuven.be>
 */
public class Service {
	private static Logger logger = Logger.getLogger(Worker.class.getCanonicalName());
	
	/*
	 * ThreadLocal instance of the workflow service 
	 */
	private static final ThreadLocal<Service> serviceInstance = new ThreadLocal<Service>() {
		@Override
		protected Service initialValue() {
			return new Service();
		}
	};
	
	/**
	 * Get an instance of the service
	 * 
	 * @return Return a new instace or a cache thread local instance
	 */
	public static Service get() {
		return serviceInstance.get();
	}
	
	private Queue queue = new Queue("pull-queue");
	
	/**
	 * Create a new instance of the workflow service.
	 */
	public Service() {}

	/**
	 * Add a new workflow to the service
	 * 
	 * @param workflow The workflow to start
	 */
	public void startWorkflow(WorkflowInstance workflow, Task start) {
		// save the workflow
		workflow.save();
		
		// save the task
		start.save();
		
		// queue the task
		this.queueTask(start);

		// send end of workflow as the last task
		EndTask endTask = new EndTask(start, start.getWorker());
		endTask.save();
		this.queueTask(endTask);
		
		logger.info("Started workflow. Added task for " + start.getWorker());
	}
	
	/**
	 * Queue a new task
	 * 
	 * @param task The task to queue
	 */
	public void queueTask(AbstractTask task) {
		try {
			// queue the task
			queue.add(task.toTaskOption());
			
			// save the task
			task.save();
		} catch (IOException e) {
			// failed to add the task
			logger.warning("Failed to add task " + task.getId().toString());
		}
	}
}
