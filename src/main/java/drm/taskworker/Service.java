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
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import org.apache.cassandra.scheduler.RoundRobinScheduler;
import org.apache.cassandra.service.CacheService;

import com.google.appengine.api.memcache.MemcacheService;
import com.google.appengine.api.memcache.MemcacheServiceFactory;
import com.google.appengine.api.taskqueue.TaskHandle;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;

import drm.taskworker.queue.Queue;
import drm.taskworker.schedule.WeightedRoundRobin;
import drm.taskworker.tasks.AbstractTask;
import drm.taskworker.tasks.EndTask;
import drm.taskworker.tasks.Task;
import drm.taskworker.tasks.WorkFlowStateListener;
import drm.taskworker.tasks.WorkflowInstance;
import drm.util.Triplet;

/**
 * This class implements a workflow service. This service should be stateless
 * with all state in the queues and storage.
 * 
 * @author Bart Vanbrabant <bart.vanbrabant@cs.kuleuven.be>
 */
public class Service {
	private static Logger logger = Logger.getLogger(Worker.class
			.getCanonicalName());

	private static Service serviceInstance = new Service();

	/*
	 * ThreadLocal instance of the workflow service
	 */
	// private static final ThreadLocal<Service> serviceInstance = new
	// ThreadLocal<Service>() {
	// @Override
	// protected Service initialValue() {
	// return new Service();
	// }
	// };

	private static final String SCHEDULE = "drm.taskworker.service.scheduler.";

	/**
	 * Get an instance of the service
	 * 
	 * @return Return a new instance or a cache thread local instance
	 */
	public static Service get() {
		return serviceInstance;
	}

	public Queue queue = new Queue("task-queue");

	/**
	 * Create a new instance of the workflow service.
	 */
	public Service() {
	}

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
	 * @param workflow
	 *            The workflow to start
	 */
	public void startWorkflow(Job job) {
		WorkflowInstance workflow = job.getWorkflow();
		Task start = job.getStartTask();

		// save the workflow
		workflow.save();

		// save the task
		start.save();

		// notify others
		synchronized (listeners) {
			for (WorkFlowStateListener wfsl : listeners) {
				wfsl.workflowStarted(workflow);
			}
		}

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
	 * @param task
	 *            The task to queue
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
	 * @param workflowId
	 *            The workflow id or null for all workflows
	 * @param workerType
	 *            The type of worker
	 * @return A task or null if no task available
	 */
	public AbstractTask getTask(UUID workflowId, String workerType) {
		List<Triplet<UUID, UUID, String>> tasks;
		try {
			tasks = queue.leaseTasks(60, TimeUnit.SECONDS, 1, workerType,
					workflowId);

			// do work!
			if (!tasks.isEmpty()) {
				Triplet<UUID, UUID, String> first = tasks.get(0);
				return AbstractTask.load(first.getA(), first.getB());
			}

		} catch (ConnectionException e) {
			throw new IllegalStateException(e);
		}
		return null;
	}

	/**
	 * Get a task from the task queue, using the priorities set with
	 * setPriorities
	 * 
	 * @param workerType
	 *            The type of worker
	 * @return A task or null if no task available
	 */
	public AbstractTask getTask(String workerType) {

		WeightedRoundRobin rrs = getPriorities(workerType);
		if (rrs == null) {
			logger.finest("no scheduler for " + workerType);
			return getTask(null, workerType);
		}

		String workflow = rrs.getNext();

		if (workflow == null) {
			return getTask(null, workerType);
		}
		
		AbstractTask handle = getTask(UUID.fromString(workflow), workerType);

		if (handle == null && workflow != null) {

			logger.finest("scheduler missed (no work for: " + workflow + ", "
					+ workerType + "), taking random");
			// don't go on fishing expedition, just grab work, if any
			return getTask(null, workerType);
		}

		return handle;

	}

	/**
	 * Remove a task when it is finished
	 * 
	 * @param handle
	 *            A handle for the task that needs to be removed.
	 */
	public void deleteTask(AbstractTask handle) {
		try {
			this.queue.deleteTask(handle);
		} catch (ConnectionException e) {
			throw new java.lang.IllegalStateException(e);
		}

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

		wf.calcStats();

		// TODO: store results
		Job job = Job.load(wf.getWorkflowId());
		job.markFinished();

		synchronized (listeners) {
			for (WorkFlowStateListener wfsl : listeners) {
				wfsl.workflowFinished(wf);
			}
		}
	}

	private MemcacheService cacheService = MemcacheServiceFactory
			.getMemcacheService();

	/**
	 * set scheduling priorities for a specific worker type
	 * 
	 * @param workerType
	 * @param rrs
	 */
	public void setPriorities(String workerType, WeightedRoundRobin rrs) {
		cacheService.put(SCHEDULE + workerType, rrs);
	}

	public WeightedRoundRobin getPriorities(String workerType) {
		return (WeightedRoundRobin) cacheService.get(SCHEDULE + workerType);
	}

	private List<WorkFlowStateListener> listeners = new LinkedList<>();

	/**
	 * add a workflow listener
	 * 
	 * !! this is NOT distributed, the listener is local to this machine
	 * 
	 * @param list
	 */
	public void addWorkflowStateListener(WorkFlowStateListener list) {
		synchronized (listeners) {
			listeners.add(list);
		}
	}

	/**
	 * remove a workflow listener
	 * 
	 * @param list
	 */
	public void removeWorkflowStateListener(WorkFlowStateListener list) {
		synchronized (listeners) {
			listeners.remove(list);
		}
	}

}
