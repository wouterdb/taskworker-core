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

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.CqlResult;
import com.netflix.astyanax.model.Row;

import drm.taskworker.config.Config;
import drm.taskworker.config.WorkflowConfig;
import drm.taskworker.monitoring.Statistic;
import drm.taskworker.tasks.AbstractTask;
import drm.taskworker.tasks.Task;

/**
 * A job is an instance of a workflow and a first task. A job is either
 * started immediately or at a future point in time.
 *
 * @author Bart Vanbrabant <bart.vanbrabant@cs.kuleuven.be>
 */
public class Job {
	private static Logger logger = Logger.getLogger(Job.class.getCanonicalName());
	
	private Task startTask;
	private UUID jobId;
	private UUID startTaskId;
	
	private long startAfter = 0;
	private long finishBefore = 0;
	private boolean isFinished = false;
	private boolean isStarted = false;
	private boolean isFailed = false;

	private Date startedAt;

	private Date finishedAt;

	private List<Statistic> stats;
	
	private String workflowName;

	private WorkflowConfig workflowConfig;
	
	/**
	 * Create a new job instance
	 * 
	 * @param name
	 *            The name of the job to start an instance of
	 */
	public Job(String name) {
		this.jobId = UUID.randomUUID();
		this.workflowName = name;
		this.loadConfig();
	}
	
	private Job() {}

	/**
	 * Load the configuration of this workflow
	 */
	private void loadConfig() {
		Config cfg = Config.getConfig();
		this.setWorkflowConfig(cfg.getWorkflow(this.workflowName));
	}

	/**
	 * @return the workflowConfig
	 */
	public WorkflowConfig getWorkflowConfig() {
		if (this.workflowConfig == null) {
			this.loadConfig();
		}
		return this.workflowConfig;
	}

	/**
	 * @param workflowConfig
	 *            the workflowConfig to set
	 */
	private void setWorkflowConfig(WorkflowConfig workflowConfig) {
		this.workflowConfig = workflowConfig;
	}
	
	/**
	 * The first task in the workflow
	 * 
	 * @return
	 */
	public Task getStartTask() {
		if (this.startTask == null) {
			this.startTask = (Task)AbstractTask.load(this.jobId, this.startTaskId);
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
	public void setStartAfter(Date startAfter) {
		this.startAfter = startAfter.getTime();
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
	public void setFinishBefore(Date finishBefore) {
		long time = finishBefore.getTime();
		if (time > 0 && time < this.startAfter) {
			throw new IllegalArgumentException("The finish time should be set after the start time of the job.");
		}
		this.finishBefore = time;
	}
	
	/**
	 * The name of the workflow. This name is a type of workflow, an instance of
	 * this workflow is identified with the workflowId
	 * 
	 * @return the name
	 */
	public String getWorkflowName() {
		return workflowName;
	}

	/**
	 * @param name
	 *            the name to set
	 */
	public void setWorkflowName(String workflow_name) {
		this.workflowName = workflow_name;
	}
	
	/**
	 * Returns the name of the job. The name is a random UUID.
	 * @return
	 */
	public String getName() {
		return getJobId().toString();
	}
	
	/**
	 * Is this job finished. A job is finished when last task is finished and
	 * the finish time has been set.
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
	 * Get the job id, this is the same as the id of the workflow instance.
	 */
	public UUID getJobId() {
		return this.jobId;
	}
	
	public boolean isFailed() {
		return isFailed;
	}

	public void setFailed() {
		this.isFailed = true;
	}
	
	/**
	 * Create a job in the database
	 */
	public void insert() {
		try {
			cs().prepareQuery(Entities.CF_STANDARD1)
				.withCql("INSERT INTO job (job_id, start_task_id, workflow_name, start_after, finish_before, started, finished, failed) " + 
								" VALUES (?, ?, ?, ?, ?, false, false, false);")
				.asPreparedStatement()
				.withUUIDValue(this.getJobId())
				.withUUIDValue(this.getStartTask().getId())
				.withStringValue(this.workflowName)
				.withLongValue(this.startAfter)
				.withLongValue(this.finishBefore)
				.execute();
		} catch (ConnectionException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * Load the given job from the database
	 */
	public static Job load(UUID jobId) {
		try {
			OperationResult<CqlResult<String, String>> result = cs().prepareQuery(Entities.CF_STANDARD1)
				.withCql("SELECT * FROM job WHERE job_id = ?")
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
		job.jobId = columns.getUUIDValue("job_id", null);
		job.startTaskId = columns.getUUIDValue("start_task_id", null);
		job.workflowName = columns.getStringValue("workflow_name", null);
		job.startAfter = columns.getLongValue("start_after", 0L);
		job.finishBefore = columns.getLongValue("finish_before", 0L);
		job.isStarted = columns.getBooleanValue("started", false);
		job.isFinished = columns.getBooleanValue("finished", false);
		job.isFailed = columns.getBooleanValue("failed", false);
		job.startedAt = columns.getDateValue("started_at", null);
		job.finishedAt = columns.getDateValue("finished_at", null);
		job.stats = columns.getValue("stats", Entities.STATS_SERIALISER,null);
		
		return job;
	}
	
	/**
	 * Get the time when the workflow was started. This is the moment when
	 * the start task was submitted.
	 * 
	 * @return the startAt
	 */
	public Date getStartAt() {
		return startedAt;
	}

	/**
	 * Set the time when the workflow was started. This should be called when
	 * the first task is submitted. When this method is called, the value
	 * is also persisted in the database and the field cannot be changed anymore.
	 * 
	 * @param startAt the startAt to set
	 */
	public void setStartAt(Date startAt) {
		if (this.startedAt != null) {
			throw new IllegalAccessError();
		}
		
		this.startedAt = startAt;
		this.isStarted = true;
		
		try {
			cs().prepareQuery(Entities.CF_STANDARD1)
				.withCql("UPDATE job SET started = ?, started_at = ? WHERE job_id = ? AND start_after = ? AND finish_before = ?")
				.asPreparedStatement()
				.withBooleanValue(this.isStarted)
				.withLongValue(this.startedAt.getTime())
				
				.withUUIDValue(this.getJobId())
				.withLongValue(this.startAfter)
				.withLongValue(this.finishBefore)
				.execute();
		} catch (ConnectionException e) {
			logger.log(Level.WARNING, "Unable to save job", e);
		}
	}

	/**
	 * Get the time when the workflow is finished.
	 * 
	 * @return the finishedAt
	 */
	public Date getFinishedAt() {
		return finishedAt;
	}

	/**
	 * Set the time when the workflow was finished. This should be called when
	 * the last task is ready. When this method is called, the value
	 * is also persisted in the database and the field cannot be changed anymore.
	 * 
	 * @param finishedAt the finishedAt to set
	 */
	public void setFinishedAt(Date finishedAt) {
		if (this.finishedAt != null) {
			throw new IllegalAccessError(this.finishedAt.toString());
		}
		
		this.finishedAt = finishedAt;
		this.isFinished = true;
		try {
			cs().prepareQuery(Entities.CF_STANDARD1)
				.withCql("UPDATE job SET finished = ?, finished_at = ? WHERE job_id = ? AND start_after = ? AND finish_before = ?")
				.asPreparedStatement()
				.withBooleanValue(this.isFinished)
				.withLongValue(this.finishedAt.getTime())

				.withUUIDValue(this.getJobId())
				.withLongValue(this.startAfter)
				.withLongValue(this.finishBefore)
				.execute();
		} catch (ConnectionException e) {
			logger.log(Level.WARNING, "Unable to save job", e);
		}
	}
	
	/**
	 * Get the history of the workflow.
	 */
	public List<AbstractTask> getHistory() {
		try {
			OperationResult<CqlResult<String, String>> result = cs()
					.prepareQuery(Entities.CF_STANDARD1)
					.withCql("SELECT * FROM task WHERE job_id = ?;")
					.asPreparedStatement().withUUIDValue(getJobId())
					.execute();

			List<AbstractTask> tasks = new ArrayList<>();
			for (Row<String, String> row : result.getResult().getRows()) {
				tasks.add(AbstractTask.createTaskFromDB(row));
			}

			return tasks;
		} catch (ConnectionException e) {
			e.printStackTrace();
		}
		return null;
	}
	
	public List<Statistic> getStats() {
		return stats;
	}
	
	/**
	 * Process statistics of tasks in the workflow
	 */
	public void calcStats() {
		Map<String, List<Integer>> samples = new HashMap<String, List<Integer>>();
		
		for (AbstractTask t : getHistory()) {
			if(t.getFinishedAt()==null)
				return;
			String key = t.getTaskType() + "." + t.getWorker();
			List<Integer> sample = samples.get(key);
			if (sample == null) {
				sample = new ArrayList<>();
				samples.put(key, sample);
			}
			sample.add((int) (t.getFinishedAt().getTime() - t.getStartedAt()
					.getTime()));
		}

		List<Statistic> out = new LinkedList<>();

		for (Map.Entry<String, List<Integer>> sample : samples.entrySet()) {
			out.add(new Statistic(sample.getKey(), sample.getValue(), 1000));
		}

		this.stats = out;

		try {
			cs().prepareQuery(Entities.CF_STANDARD1)
					.withCql("UPDATE job SET stats = ? WHERE job_id = ? AND start_after = ? AND finish_before = ?")
					.asPreparedStatement()
					.withByteBufferValue(out, Entities.STATS_SERIALISER)
					.withUUIDValue(getJobId())
					.withLongValue(this.startAfter)
					.withLongValue(this.finishBefore)
					.execute();
		} catch (ConnectionException e) {
			e.printStackTrace();
		}

	}
	
	/**
	 * The string representation of this workflow.
	 */
	@Override
	public String toString() {
		return "job-" + getJobId();
	}
	
	/**
	 * Get a new task in this job
	 * 
	 * @param worker
	 *            The work on which this task should be allocated.
	 * @return A new task that can be added to the queue
	 */
	public Task newTask(Task parent, String worker) {
		return new Task(parent, worker);
	}

	/**
	 * Create a new task that starts the workflow
	 */
	public Task newStartTask() {
		this.startTask = new Task(this, this.getWorkflowConfig().getWorkflowStart());
		this.startTaskId = this.startTask.getId();
		
		return this.startTask;
	}
	
	/**
	 * Get all workflows
	 */
	public static List<Job> getAll() {
		try {
			OperationResult<CqlResult<String, String>> result = cs()
					.prepareQuery(Entities.CF_STANDARD1)
					.withCql("SELECT * FROM job;").asPreparedStatement()
					.execute();

			List<Job> workflows = new ArrayList<>();
			for (Row<String, String> row : result.getResult().getRows()) {
			    Job wf = createJob(row);
			    workflows.add(wf);
			}

			return workflows;
		} catch (ConnectionException e) {
			e.printStackTrace();
		}
		return null;
	}
}
