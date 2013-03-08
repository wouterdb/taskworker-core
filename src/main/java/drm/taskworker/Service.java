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

import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import com.google.appengine.api.taskqueue.TaskHandle;

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
	
	private Queue queue = new Queue("task-queue");
	
	/**
	 * Create a new instance of the workflow service.
	 */
	public Service() {}
	
	/**
	 * Add a job to the queue
	 */
	public void addJob(Job job) {
		job.create();
		job.getWorkflow().save();
		job.getStartTask().save();
		logger.info("Stored job to start at " + new Date(job.getStartAfter()));
	}
	
	/**
	 * Start all jobs that have a start time after now
	 */
	public void startJobs() {
		List<Job> jobs = Job.getJobsThatShouldStart();
		
		for (Job job : jobs) {
			if (!job.getWorkflow().isStarted()) {
				logger.info("Found a job to start " + job.getJobId());
				this.startWorkflow(job);
			}
		}
	}

	/**
	 * Add a new workflow to the service
	 * 
	 * @param workflow The workflow to start
	 */
	public void startWorkflow(Job job) {
		WorkflowInstance workflow = job.getWorkflow();
		Task start = job.getStartTask();
		
		// save the workflow
		workflow.save();
		
		// save the task
		start.save();
		
		// queue the task
		this.queueTask(start);
		
		// set the start date of the workflow
		workflow.setStartAt(new Date());

		// send end of workflow as the last task
		EndTask endTask = new EndTask(start, start.getWorker());
		endTask.save();
		this.queueTask(endTask);
		
		// mark job as started
		job.markStarted();
		
		logger.info("Started workflow. Added task for " + start.getWorker());
	}
	
	/**
	 * Queue a new task
	 * 
	 * @param task The task to queue
	 */
	public void queueTask(AbstractTask task) {
		// queue the task
		queue.add(task);
			
		// save the task
		task.save();
	}
	
	/**
	 * Get a task from the task queue
	 * 
	 * @param workflowId The workflow id or null for all workflows
	 * @param workerType The type of worker
	 * @return A task or null if no task available
	 */
	public TaskHandle getTask(String workflowId, String workerType) {
		List<TaskHandle> tasks = queue.leaseTasks(10, TimeUnit.SECONDS, 1, workerType, workflowId);
		
		// do work!
		if (!tasks.isEmpty()) {
			return tasks.get(0);
		}
		
		return null;
	}
	
	/**
	 * Remove a task when it is finished
	 * 
	 * @param handle A handle for the task that needs to be removed.
	 */
	public void deleteTask(TaskHandle handle) {
		this.queue.deleteTask(handle);
	}

	/**
	 * Mark the end of a workflow. Only one task can finish a workflow!
	 * 
	 * @param task
	 * @param nextTasks
	 */
	public void workflowFinished(AbstractTask task, List<AbstractTask> nextTasks) {
		WorkflowInstance wf = task.getWorkflow();
		
		logger.info("Workflow " + wf.getWorkflowId() + " was finished");
		
		if (task.getTaskType().equals("end")) {
			wf.setFinishedAt(new Date());
		}
		
		// TODO: store results
		Job job = Job.load(wf.getWorkflowId());
		job.markFinished();
	}
}
