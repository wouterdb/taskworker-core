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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.CqlResult;
import com.netflix.astyanax.model.Row;

import drm.taskworker.tasks.AbstractTask;
import drm.taskworker.tasks.Task;
import drm.taskworker.tasks.WorkflowInstance;

/**
 * A job is an instance of a workflow and a first task. A job is either
 * started immediately or at a future point in time.
 *
 * @author Bart Vanbrabant <bart.vanbrabant@cs.kuleuven.be>
 */
public class Job implements Serializable {
	private static Logger logger = Logger.getLogger(Job.class.getCanonicalName());
	
	private transient WorkflowInstance workflow;
	private transient Task startTask;
	private UUID workflowId;
	private UUID startTaskId;
	
	private long startAfter = 0;
	private long finishBefore = 0;
	private boolean isFinished = false;
	private boolean isStarted = false;
	
	/**
	 * Create a new job that is started immediately
	 * 
	 * @param workflow The workflowinstance to start
	 * @param startTask The first task in the workflow
	 */
	public Job(WorkflowInstance workflow, Task startTask) {
		this(workflow, startTask, new Date(), null);
	}

	/**
	 * Create a new job that is started immediately
	 * 
	 * @param workflow The workflowinstance to start
	 * @param startTask The first task in the workflow
	 * @param startAt Start the task at this moment
	 * @param finishAt Finish the task by that time
	 */
	public Job(WorkflowInstance workflow, Task startTask, Date startAt,
			Date finishAt) {
		super();
		this.workflow = workflow;
		this.workflowId = workflow.getWorkflowId();
		this.startTask = startTask;
		this.startTaskId = startTask.getId();
		
		setStartAfter(startAt.getTime());
		
		if (finishAt != null) {
			setFinishBefore(finishAt.getTime());
		}
	}
	

	/**
	 * Create a new job that is started at the given date but does not have
	 * a deadline.
	 * 
	 * @param workflow The workflowinstance to start
	 * @param startTask The first task in the workflow
	 * @param startAt Start the task at this moment
	 */
	public Job(WorkflowInstance workflow, Task startTask, Date startAt) {
		this(workflow, startTask, startAt, null);
	}
	
	private Job() {}
	
	/**
	 * Get the workflow instance
	 */
	public WorkflowInstance getWorkflow() {
		if (this.workflow == null) {
			this.workflow = WorkflowInstance.load(this.workflowId);
		}
		return workflow;
	}

	/**
	 * The first task in the workflow
	 * 
	 * @return
	 */
	public Task getStartTask() {
		if (this.startTask == null) {
			this.startTask = (Task)AbstractTask.load(this.workflowId, this.startTaskId);
		}
		return startTask;
	}

	/**
	 * Get the start time of this job
	 * 
	 * @return
	 */
	public long getStartAfter() {
		return this.startAfter;
	}
	
	/**
	 * Set the start time of this job in a timestamp in milliseconds.
	 * 
	 * @param startAfter
	 */
	public void setStartAfter(long startAfter) {
		this.startAfter = startAfter;
	}

	/**
	 * Finish the workflow by this point in time. This value can be 0 to 
	 * indicate that this job does not have a deadline.
	 * 
	 * @return
	 */
	public long getFinishBefore() {
		return finishBefore;
	}

	/**
	 * Set the time when the job has to finish.
	 * 
	 * @param time
	 */
	public void setFinishBefore(long time) {
		if (time > 0 && time < this.startAfter) {
			throw new IllegalArgumentException("The finish time should be set after the start time of the job.");
		}
		this.finishBefore = time;
	}
	
	/**
	 * The name of a job is the ID of the workflowinstance.
	 * @return
	 */
	public String getName() {
		return this.getWorkflow().getWorkflowId().toString();
	}
	
	/**
	 * Is this job finished. A job is finished when the workflow instance has
	 * been marked as finished.
	 */
	public boolean isFinished() {
		return this.isFinished;
	}
	
	/**
	 * Is this job started. A job is started when the workflow instance has
	 * been marked as started.
	 */
	public boolean isStarted() {
		return this.isStarted;
	}
	
	/**
	 * Mark this job as started
	 */
	public void markStarted() {
		this.isStarted = true;
		this.save();
	}
	
	/**
	 * Mark this job as finished
	 */
	public void markFinished() {
		this.isFinished = true;
		this.save();
	}
	
	/**
	 * Get the job id, this is the same as the id of the workflow instance.
	 */
	public String getJobId() {
		return this.getWorkflow().getWorkflowId().toString();
	}
	
	/**
	 * Create a job in the database
	 */
	public void create() {
		try {
			cs().prepareQuery(Entities.CF_STANDARD1)
				.withCql("INSERT INTO job (workflow_id, start_task_id, start_after, finish_before, started, finished) " + 
								" VALUES (?, ?, ?, ?, false, false);")
				.asPreparedStatement()
				.withUUIDValue(this.getWorkflow().getWorkflowId())
				.withUUIDValue(this.getStartTask().getId())
				.withLongValue(this.startAfter)
				.withLongValue(this.finishBefore)
				.execute();
		} catch (ConnectionException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * Save the state of the job in the database
	 */
	public void save() {
		try {
			cs().prepareQuery(Entities.CF_STANDARD1)
				.withCql("UPDATE job SET started = ?, finished = ? WHERE workflow_id = ? AND start_after = ? AND finish_before = ?")
				.asPreparedStatement()
				.withBooleanValue(this.isStarted)
				.withBooleanValue(this.isFinished)
				.withUUIDValue(this.getWorkflow().getWorkflowId())
				.withLongValue(this.startAfter)
				.withLongValue(this.finishBefore)
				.execute();
		} catch (ConnectionException e) {
			logger.log(Level.WARNING, "Unable to save job", e);
		}
	}
	
	/**
	 * Load the given job from the database
	 */
	public static Job load(UUID jobId) {
		try {
			OperationResult<CqlResult<String, String>> result = cs().prepareQuery(Entities.CF_STANDARD1)
				.withCql("SELECT * FROM job WHERE workflow_id = ?")
				.asPreparedStatement()
				.withUUIDValue(jobId)
				.execute();
			
			for (Row<String, String> row : result.getResult().getRows()) {
				return createJob(row);
			}
		} catch (ConnectionException e) {
			logger.log(Level.WARNING, "Unable to load job", e);
		}
		return null;
	}
	
	/**
	 * Load all jobs that should start and are not finished
	 */
	public static List<Job> getJobsThatShouldStart() {
		List<Job> jobs = new ArrayList<>();
		try {
			OperationResult<CqlResult<String, String>> result = cs().prepareQuery(Entities.CF_STANDARD1)
				.withCql("SELECT * FROM job WHERE started = false AND start_after < ?")
				.asPreparedStatement()
				.withLongValue(System.currentTimeMillis())
				.execute();
		
			for (Row<String, String> row : result.getResult().getRows()) {
				jobs.add(createJob(row));
			}
			
		} catch (ConnectionException e) {
			logger.log(Level.WARNING, "Unable to fetch jobs", e);
		}
		return jobs;
	}

	public static Job createJob(Row<String, String> row) {
		ColumnList<String> columns = row.getColumns();
		
		Job job = new Job();
		job.workflowId = columns.getUUIDValue("workflow_id", null);
		job.startTaskId = columns.getUUIDValue("start_task_id", null);
		job.startAfter = columns.getLongValue("start_after", 0L);
		job.finishBefore = columns.getLongValue("finish_before", 0L);
		job.isStarted = columns.getBooleanValue("started", false);
		job.isFinished = columns.getBooleanValue("finished", false);
		
		return job;
	}
}
