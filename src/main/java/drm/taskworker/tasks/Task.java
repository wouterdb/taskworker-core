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

package drm.taskworker.tasks;

import static drm.taskworker.Entities.cs;

import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.logging.Logger;

import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.connectionpool.exceptions.SerializationException;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.CqlResult;
import com.netflix.astyanax.model.Row;
import com.netflix.astyanax.model.Rows;
import com.netflix.astyanax.serializers.ObjectSerializer;

import drm.taskworker.Entities;
import drm.taskworker.Job;

/**
 * A baseclass for all tasks.
 * 
 * @author Bart Vanbrabant <bart.vanbrabant@cs.kuleuven.be>
 */
public class Task {
	protected static final Logger logger = 
			Logger.getLogger(Task.class.getCanonicalName());
	public static final String JOIN_PARAM = ".meta.join";
	
	private UUID taskId = null;

	private String worker = null;
	private Date createdAt = null;
	private Date startedAt = null;
	private Date finishedAt = null;

	private UUID jobId = null;

	// WARNING: this list is only saved to the database and not yet loaded from it!
	private List<UUID> parentIds = new LinkedList<>();

	private List<Task> parents = new LinkedList<>();
	
	private Map<String, ValueRef> params = new HashMap<>();
	
	/**
	 * Create a task for a worker
	 * 
	 * @param workflow
	 *            The workflow this task belongs to
	 * @param parent
	 *            The parent of this task
	 * @param worker
	 *            The name of the worker
	 */
	public Task(UUID jobId, Task parentTask, String worker, UUID taskId) {
		this.taskId = taskId;

		this.setJobId(jobId);

		// lookup the next worker
		if (parentTask != null) {
			this.addParent(parentTask);
			this.worker = worker;
		} else {
			this.worker = worker;
		}

		// set the date the task was created
		this.setCreatedAt(new Date());
	}

	Task() {
	}
	
	/**
	 * Create a task for a worker
	 * 
	 * @param parent
	 *            The parent of this task
	 * @param worker
	 *            The name of the worker
	 */
	public Task(Task parent, String worker) {
		this(parent.getJobId(), parent, worker, UUID.randomUUID());
	}
	
	/**
	 * Create a task that starts the job or create a task that has multiple
	 * parents that will be set later on.
	 * 
	 * @param workflow
	 * @param worker
	 */
	public Task(Job job, String worker) {
		this(job.getJobId(), null, worker, UUID.randomUUID());
	}

	/** 
	 * Create a new task in a given job with a given taskid
	 */
	public Task(UUID jobId, UUID taskId, String nextWorker) {
		this.setJobId(jobId);
		this.taskId = taskId;
		this.worker = nextWorker;

		// set the date the task was created
		this.setCreatedAt(new Date());	
	}

	/**
	 * Get the id of this task.
	 */
	public UUID getId() {
		return this.taskId;
	}

	/**
	 * Get the name of the work to send the task to.
	 * 
	 * @return A string that determines the name of the worker.
	 */
	public String getWorker() {
		return this.worker;
	}

	/**
	 * Get the workflow this task belongs to.
	 */
	public Job getJob() {
		Job job = Job.load(getJobId());
		return job;
	}
	
	/**
	 * Get a job wide options
	 */
	public String getJobOption(String key) {
		return this.getJob().getWorkflowConfig().getOption(key);
	}
	
	/**
	 * Check if a job option exists
	 */
	public boolean containsJobOption(String key) {
		return this.getJob().getWorkflowConfig().containsOption(key);
	}

	/**
	 * @return the createdAt
	 */
	public Date getCreatedAt() {
		return createdAt;
	}

	/**
	 * @param createdAt
	 *            the createdAt to set
	 */
	public void setCreatedAt(Date createdAt) {
		this.createdAt = createdAt;
	}

	/**
	 * @return the startedAt
	 */
	public Date getStartedAt() {
		if (startedAt == null) {
			this.loadTiming();
		}
		return startedAt;
	}

	/**
	 * Set the start date to now.
	 */
	public void setStartedAt() {
		this.startedAt = new Date();
	}

	/**
	 * @return the finishedAt
	 */
	public Date getFinishedAt() {
		if (finishedAt == null) {
			this.loadTiming();
		}
		return finishedAt;
	}

	/**
	 * Set the finished date to now
	 */
	public void setFinishedAt() {
		this.finishedAt = new Date();
	}

	/**
	 * Save the task to the datastore
	 */
	public void save() {
		/*
		 * CREATE TABLE task ( id uuid PRIMARY KEY, created_at uuid, finished_at
		 * uuid, parent_id uuid, started_at uuid, type text, worker_name text,
		 * job_id uuid )
		 */
		try {
			cs().prepareQuery(Entities.CF_STANDARD1)
					.withCql("INSERT INTO task (id, created_at, worker_name, job_id) "
									+ " VALUES (?, ?, ?, ?);")
					.asPreparedStatement().withUUIDValue(this.getId()) // id
					.withLongValue(this.createdAt.getTime()) // created_at
					.withStringValue(this.getWorker()) // worker_name
					.withUUIDValue(this.getJobId()) // job_id
					.execute();

			// now save the list of parents
			for (UUID parentId : this.parentIds) {
				saveParent(this.getJobId(), this.getId(), parentId);
			}
			
			// save all value refs that have not been saved
			for (ValueRef ref : params.values()) {
				ref.save();
			}
		} catch (ConnectionException e) {
			e.printStackTrace();
		}
	}

	public static void saveParent(UUID jobId, UUID taskId, UUID parentId) {
		try {
			cs().prepareQuery(Entities.CF_STANDARD1)
					.withCql(
							"INSERT INTO task_parent (id, job_id, parent_id) VALUES (?, ?, ?);")
					.asPreparedStatement().withUUIDValue(taskId) // id
					.withUUIDValue(jobId) // job_id
					.withUUIDValue(parentId) // parent_id
					.execute();
		} catch (ConnectionException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Is this task finished?
	 */
	public boolean isFinshed() {
		return this.getFinishedAt() != null;
	}

	/**
	 * Load a task from the database
	 */
	public static Task load(UUID jobId, UUID id) {
		if (jobId == null) {
			throw new IllegalArgumentException("Job ID is null");
		}
		if (id == null) {
			throw new IllegalArgumentException("Task id is null");
		}
		try {
			OperationResult<CqlResult<	String, String>> result = cs()
					.prepareQuery(Entities.CF_STANDARD1)
					.withCql("SELECT * FROM task WHERE job_id = ? AND id = ?;")
					.asPreparedStatement().withUUIDValue(jobId)
					.withUUIDValue(id).execute();

			for (Row<String, String> row : result.getResult().getRows()) {
				return createTaskFromDB(row);
			}
		} catch (ConnectionException e) {
			
		}
		return null;
	}

	public static Task createTaskFromDB(Row<String, String> row) {
		ColumnList<String> columns = row.getColumns();

		Task task = new Task();
		task.taskId = columns.getUUIDValue("id", null);
		task.createdAt = new Date(columns.getLongValue("created_at", 0L));
		task.setJobId(columns.getUUIDValue("job_id", null));
		task.worker = columns.getStringValue("worker_name", null);

		return task;
	}
	
	/**
	 * Load the timing from the database
	 */
	private void loadTiming() {
		try {
			OperationResult<CqlResult<	String, String>> result = cs()
					.prepareQuery(Entities.CF_STANDARD1)
					.withCql("SELECT * FROM task_timing WHERE id = ?;")
					.asPreparedStatement()
					.withUUIDValue(this.getId())
					.execute();

			for (Row<String, String> row : result.getResult().getRows()) {
				ColumnList<String> c = row.getColumns();
				this.startedAt = new Date(c.getLongValue("started_at", 0L));
				if (this.startedAt.getTime() == 0) {
					this.startedAt = null;
				}
				this.finishedAt = new Date(c.getLongValue("finished_at", 0L));
				if (this.finishedAt.getTime() == 0) {
					this.finishedAt = null;
				}			}
		} catch (ConnectionException e) {
			
		}
	}

	/**
	 * Save the start and finish timings
	 */
	public void saveTiming() {
		try {
			Keyspace cs = cs();
			cs.prepareQuery(Entities.CF_STANDARD1)
					.withCql(
							"INSERT INTO task_timing (id, started_at, finished_at) VALUES (?, ?, ?)")
					.asPreparedStatement()
					.withUUIDValue(this.getId()) // id
					.withLongValue(this.startedAt.getTime()) // started_at
					.withLongValue(this.finishedAt.getTime()) // finished_at
					.execute();
			
		} catch (ConnectionException e) {
			e.printStackTrace();
		}
	}

	/**
	 * @return the workflowId
	 */
	public UUID getJobId() {
		return jobId;
	}

	/**
	 * @param jobId
	 *            the jobId to set
	 */
	private void setJobId(UUID jobId) {
		this.jobId = jobId;
	}

	/**
	 * Get all parents of this task
	 * @return
	 */
	public List<Task> getParents() {
		if (parents.size() == 0) {
			loadParents();
		}
		return parents;
	}
	
	/**
	 * Use the given task as parent and copy over the join list
	 * 
	 * @param parent
	 */
	public void addParent(Task parent) {
		if (this.parents.size() == 0) {
			ValueRef joinRef = parent.getParamRef(JOIN_PARAM);
			try {
				String joinList = (String)joinRef.getValue();
				this.addParam(JOIN_PARAM, joinList);
				logger.info("Added JOIN_PARAM to " + this.getId() + " from " + parent.getId() + " " + joinList);
			} catch (ParameterFoundException e) {
				throw new IllegalArgumentException("A task should have its join parameter set");
			}
		}
		
		this.parentIds.add(parent.getId());
		this.parents.add(parent);
	}

	private void loadParents() {
		try {
			Rows<String, String> rows;

			rows = cs()
					.prepareQuery(Entities.CF_STANDARD1)
					.withCql("SELECT parent_id FROM task_parent WHERE job_id=? and id = ?;")
					.asPreparedStatement()
					.withUUIDValue(this.jobId)
					.withUUIDValue(this.getId())
					.execute().getResult().getRows();

			for (Row<String, String> row : rows) {
				ColumnList<String> c = row.getColumns();
				parentIds.add(c.getUUIDValue("parent_id", null));
			}
			
			for (UUID parentid : parentIds) {
				parents.add(load(jobId, parentid));
			}
		} catch (ConnectionException e) {
			throw new RuntimeException(e);
		}
	}
	
	/**
	 * Add a parameter to the task
	 * 
	 * @param name
	 *            The name of the parameter
	 * @param value
	 *            The value of the parameter
	 */
	public void addParam(String name, Object value) {
		ValueRef ref = new ValueRef(this, name);
		ref.setValue(value);
		this.params.put(name, ref);
	}

	/**
	 * Get a parameter with the given name.
	 * 
	 * @param name
	 *            The name of the parameter
	 * @return The value
	 * @throws ParameterFoundException 
	 */
	public Object getParam(String name) throws ParameterFoundException {
		return getParamRef(name).getValue();
	}
	
	/**
	 * Get a value reference to the parameter
	 */
	public ValueRef getParamRef(String name) {
		if (this.params.containsKey(name)) {
			return this.params.get(name);
		}
		ValueRef ref = new ValueRef(this, name);
		this.params.put(name, ref);
		return ref;
	}
	
	/**
	 * Get param names
	 */
	public Set<String> getParamNames() {
		this.loadParamRefs();
		return this.params.keySet();
	}

	/**
	 * Get a list of value references
	 */
	public void loadParamRefs() {
		try {
			OperationResult<CqlResult<String, String>> result = cs()
					.prepareQuery(Entities.CF_STANDARD1)
					.withCql("SELECT * FROM parameter WHERE job_id = ? AND task_id = ?")
					.asPreparedStatement()
					.withUUIDValue(this.getJobId())
					.withUUIDValue(this.getId())
					.execute();

			for (Row<String, String> row : result.getResult().getRows()) {
				ColumnList<String> columns = row.getColumns();

				UUID jobId = columns.getUUIDValue("job_id", null);
				UUID taskId = columns.getUUIDValue("task_id", null);
				String name = columns.getStringValue("name", null);
				try {
					Object value = ObjectSerializer.get().fromByteBuffer(columns.getByteBufferValue("value", null));
					
					ValueRef ref = new ValueRef(jobId, taskId, name);
					ref.setValue(value);
					
					params.put(name, ref);
				} catch (SerializationException e) {
					logger.warning("Unable to deserialize value");
				}
			}
		} catch (ConnectionException e) {
			e.printStackTrace();
		}
	}
	
	@Override
	public String toString() {
		return String.format("Task [job=%s, id=%s, worker=%s, nparams=%d]",
				getJobId(), getId(), getWorker(), params.size());
	}
	
	/**
	 * Mark that this task splits/forks the workflow and a join will happen
	 * somewhere in the future.
	 * 
	 * This method pushes the id of the join task on the join stack
	 */
	public void markSplit(UUID joinId) {
		ValueRef joinRef = this.getParamRef(JOIN_PARAM);
		
		try {
			String joinList = (String)joinRef.getValue();
			joinRef.setValue(joinList + "|" + joinId);
			joinRef.save();
		} catch (ParameterFoundException e) {
			throw new IllegalArgumentException("Unable to find the join parameter.");
		}

	}

	/**
	 * This method needs to be called on the first task that starts with an
	 * empty join list.
	 */
	public void initJoin() {
		this.addParam(JOIN_PARAM, "");
	}
}
