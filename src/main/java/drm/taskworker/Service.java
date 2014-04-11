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

import static drm.taskworker.Entities.cs;
import static drm.taskworker.config.Config.cfg;

import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.CqlResult;
import com.netflix.astyanax.model.Row;
import com.netflix.astyanax.model.Rows;

import drm.taskworker.config.WorkflowConfig;
import drm.taskworker.queue.Queue;
import drm.taskworker.queue.TaskHandle;
import drm.taskworker.schedule.FairShare;
import drm.taskworker.schedule.IScheduler;
import drm.taskworker.schedule.WeightedRoundRobin;
import drm.taskworker.tasks.JobStateListener;
import drm.taskworker.tasks.Task;

/**
 * This class implements a workflow service. This service should be stateless
 * with all state in the queues and storage.
 * 
 * @author Bart Vanbrabant <bart.vanbrabant@cs.kuleuven.be>
 */
public class Service {
	private static Logger logger = Logger.getLogger(Service.class.getCanonicalName());
	private static Service serviceInstance = new Service();
	
	private static final long INTERVAL = 60;
	private Map<String, WeightedRoundRobin> priorities = new HashMap<>();
	private Map<String, Long> timeout = new HashMap<>();
	
	private Map<UUID,WorkflowConfig> wfConfig = new HashMap<>();
	

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
	private Service() {
	}

	/**
	 * Add a job to the queue
	 */
	public void addJob(Job job) {
		job.insert();
		job.getStartTask().save();
		logger.info("Stored job to start at " + new Date(job.getStartAfter()));
	}

	/**
	 * Start all jobs that have a start time after now
	 */
	public void startJobs() {
		List<Job> jobs = Job.getJobsThatShouldStart();

		for (Job job : jobs) {
			if (!job.isStarted()) {
				logger.info("Found a job to start " + job.getJobId());
				this.startJob(job);
			}
		}
	}

	/**
	 * Add a new workflow to the service
	 * 
	 * @param workflow
	 *            The workflow to start
	 */
	public void startJob(Job job) {
		Task start = job.getStartTask();

		// notify others
		synchronized (listeners) {
			for (JobStateListener wfsl : listeners) {
				wfsl.jobStarted(job);
			}
		}

		// queue the task
		this.queueTask(start);

		// set the start date of the workflow
		job.setStartAt(new Date());

		logger.info("Started workflow. Added task for " + start.getWorker());
	}

	/**
	 * Queue a new task
	 * 
	 * @param task
	 *            The task to queue
	 */
	public void queueTask(Task task) {
		task.save();
		
		// set the task as scheduled
		this.queue.addTask(task);
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
	public Task getTask(UUID workflowId, String workerType) {
		List<TaskHandle> tasks;
		try {
			tasks = queue.leaseTasks(15, TimeUnit.SECONDS, 1, workerType, workflowId);

			// do work!
			if (!tasks.isEmpty()) {
				TaskHandle first = tasks.get(0);
				return Task.load(first.getJobID(), first.getId());
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
	public Task getTask(String workerType) {

		WeightedRoundRobin rrs = getPriorities(workerType);
		if (rrs == null) {
			logger.finest("no scheduler for " + workerType);
			return getTask(null, workerType);
		}

		String workflow = rrs.getNext();

		if (workflow == null) {
			return getTask(null, workerType);
		}
		
		Task handle = getTask(UUID.fromString(workflow), workerType);

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
	public void deleteTask(Task handle) {
		this.queue.finishTask(handle);
	}

	/**
	 * Mark the end of a job. Only one task can finish a job!
	 */
	public void jobFinished(Job job) {
		logger.info("Job " + job.getJobId() + " was finished");

		job.setFinishedAt(new Date());
		job.calcStats();

		removeJobPriority(job,job.getWorkflowConfig().getSteps().keySet());
		
		synchronized (listeners) {
			for (JobStateListener wfsl : listeners) {
				wfsl.jobFinished(job);
			}
		}
	}
	
	/**
	 * Remove a job from the priorities table
	 */
	public void removeJobPriority(Job job, Collection<String> workers) {
		try {
			for(String worker : workers) {
				cs().prepareQuery(Entities.CF_STANDARD1)
						.withCql("DELETE FROM priorities WHERE job_id = ? AND worker_type = ?")
						.asPreparedStatement()
						.withUUIDValue(job.getJobId())
						.withStringValue(worker)
						.execute();
			}
		} catch (ConnectionException e) {
			logger.severe("Unable to remove priorities from table for job " + job.getJobId());
		}
	}

	/**
	 * set scheduling priorities for a specific worker type
	 * 
	 * @param workerType
	 * @param rrs
	 */
	public void setPriorities(String workerType, WeightedRoundRobin rrs) {
		try {
			for (int i = 0; i < rrs.getLength(); i++) {
				UUID jobId = UUID.fromString(rrs.getName(i));
				float weight = rrs.getWeight(i);
				
				cs().prepareQuery(Entities.CF_STANDARD1)
					.withCql("UPDATE priorities SET weight = ? WHERE job_id = ? AND worker_type = ?")
					.asPreparedStatement()
					.withFloatValue(weight)
					.withUUIDValue(jobId)
					.withStringValue(workerType)
					.execute();
			}
			
		} catch (ConnectionException e) {
			e.printStackTrace();
		}
		this.priorities.put(workerType, rrs);
		this.timeout.put(workerType, System.currentTimeMillis() + INTERVAL);
	}

	public WeightedRoundRobin getPriorities(String workerType) {
		if (this.priorities.containsKey(workerType) && this.timeout.get(workerType) > System.currentTimeMillis()) {
			return this.priorities.get(workerType);
		}
		
		// load the weight from cassandra
		try {
			OperationResult<CqlResult<String, String>> result = cs().prepareQuery(Entities.CF_STANDARD1)
				.withCql("SELECT * FROM priorities WHERE worker_type = ?")
				.asPreparedStatement()
				.withStringValue(workerType)
				.execute();
			
			Rows<String, String> rows = result.getResult().getRows();
			
			float[] weights = new float[rows.size()];
			String[] names = new String[rows.size()];
			
			int i = 0;
			for (Row<String, String> row : rows) {
				ColumnList<String> columns = row.getColumns();
				
				weights[i] = columns.getColumnByName("weight").getFloatValue();
				names[i] = columns.getColumnByName("job_id").getUUIDValue().toString();
				i++;
			}
			
			WeightedRoundRobin rrs = new WeightedRoundRobin(names, weights);
			
			this.priorities.put(workerType, rrs);
			this.timeout.put(workerType, System.currentTimeMillis() + INTERVAL);
			
			return rrs;
		} catch (ConnectionException e) {
			e.printStackTrace();
		}
		
		return new WeightedRoundRobin(new String[]{}, new float[]{});
	}

	private List<JobStateListener> listeners = new LinkedList<>();

	/**
	 * add a workflow listener
	 * 
	 * !! this is NOT distributed, the listener is local to this machine
	 * 
	 * @param list
	 */
	public void addWorkflowStateListener(JobStateListener list) {
		synchronized (listeners) {
			listeners.add(list);
		}
	}

	/**
	 * remove a workflow listener
	 * 
	 * @param list
	 */
	public void removeWorkflowStateListener(JobStateListener list) {
		synchronized (listeners) {
			listeners.remove(list);
		}
	}
	
	/**
	 * Return the next step in the workflow based on the workflow id and the 
	 * current step 
	 */
	public String getNextWorker(UUID jobId, String currentStep, String nextSymbol) {
		if (!this.wfConfig.containsKey(jobId)) {
			synchronized (this.wfConfig) {
				Job job = Job.load(jobId);
				assert(job != null);
				this.wfConfig.put(jobId, job.getWorkflowConfig());
			}
		}
		
		String next = this.wfConfig.get(jobId).getNextStep(currentStep, nextSymbol);
		
		return next;
	}

	/**
	 * Mark the job as failed and stop accepting new tasks for it
	 * 
	 * @param jobId
	 */
	public void killJob(UUID jobId) {
		
	}
}
