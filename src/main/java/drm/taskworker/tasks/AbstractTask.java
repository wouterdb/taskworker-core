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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;

import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.CqlResult;
import com.netflix.astyanax.model.Row;
import com.netflix.astyanax.model.Rows;

import drm.taskworker.Entities;

/**
 * A baseclass for all tasks.
 * 
 * @author Bart Vanbrabant <bart.vanbrabant@cs.kuleuven.be>
 */
public abstract class AbstractTask implements Serializable {
	private UUID taskId = null;

	private String worker = null;
	private Date createdAt = null;
	private Date startedAt = null;
	private Date finishedAt = null;

	private UUID workflowId = null;

	// WARNING: this list is only saved to the database and not yet loaded from it!
	private List<UUID> parentIds = null;

	//private Date lease;
	private List<AbstractTask> parents;
	
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
	public AbstractTask(WorkflowInstance workflow, AbstractTask parentTask,
			String worker) {
		this();

		this.setWorkflowId(workflow.getWorkflowId());

		// lookup the next worker
		if (parentTask != null) {
			this.parentIds.add(parentTask.getId());
			this.worker = workflow.resolveStep(parentTask.getWorker(), worker);
		} else {
			this.worker = worker;
		}

		// set the date the task was created
		this.setCreatedAt(new Date());
	}

	/**
	 * Create a task with multiple parents.
	 * 
	 * @param parents
	 * @param worker2
	 */
	public AbstractTask(List<AbstractTask> parents, String worker) {
		this();

		if (parents.size() == 0) {
			throw new IllegalArgumentException(
					"Each task should have at least one parent.");
		}

		AbstractTask parentTask = parents.get(0);
		this.setWorkflowId(parentTask.getWorkflowId());
		for (AbstractTask parent : parents) {
			this.parentIds.add(parent.getId());
		}

		this.worker = parentTask.getWorkflow().resolveStep(
				parentTask.getWorker(), worker);

		// set the date the task was created
		this.setCreatedAt(new Date());
	}

	public AbstractTask(AbstractTask one, List<UUID> parents, String worker) {
		this();
		
		if (parents.size() == 0) {
			throw new IllegalArgumentException(
					"Each task should have at least one parent.");
		}

		this.setWorkflowId(one.getWorkflowId());

		this.parentIds.addAll(parents);

		this.worker = one.getWorkflow().resolveStep(one.getWorker(), worker);

		// set the date the task was created
		this.setCreatedAt(new Date());
	}
	
	public AbstractTask() {
		this.taskId = UUID.randomUUID();
		this.parentIds = new LinkedList<>();
	}

	/**
	 * Get the id of this task.
	 */
	public UUID getId() {
		return this.taskId;
	}

	/**
	 * Returns the string id of this type of task
	 * 
	 * @return A number that can be used to determine the type of task.
	 * 		0 : a normal work item
	 * 		1 : an end task
	 */
	public abstract int getTaskType();

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
	public WorkflowInstance getWorkflow() {
		WorkflowInstance wf = WorkflowInstance.load(this.getWorkflowId());
		assert (wf != null);
		return wf;
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
		 * workflow_id uuid )
		 */
		try {
			cs().prepareQuery(Entities.CF_STANDARD1)
					.withCql(
							"INSERT INTO task (id, created_at, type, worker_name, workflow_id) "
									+ " VALUES (?, ?, ?, ?, ?);")
					.asPreparedStatement().withUUIDValue(this.getId()) // id
					.withLongValue(this.createdAt.getTime()) // created_at
					.withIntegerValue(this.getTaskType()) // type
					.withStringValue(this.getWorker()) // worker_name
					.withUUIDValue(this.getWorkflowId()) // workflow_id
					.execute();

			// now save the list of parents
			for (UUID parentId : this.parentIds) {
				cs().prepareQuery(Entities.CF_STANDARD1)
						.withCql(
								"INSERT INTO task_parent (id, workflow_id, parent_id) VALUES (?, ?, ?);")
						.asPreparedStatement().withUUIDValue(this.getId()) // id
						.withUUIDValue(this.workflowId) // workflow_id
						.withUUIDValue(parentId) // parent_id
						.execute();
			}
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
	public static AbstractTask load(UUID workflowID, UUID id) {
		if (workflowID == null || id == null) {
			throw new IllegalArgumentException();
		}
		try {
			OperationResult<CqlResult<	String, String>> result = cs()
					.prepareQuery(Entities.CF_STANDARD1)
					.withCql(
							"SELECT * FROM task WHERE workflow_id = ? AND id = ?;")
					.asPreparedStatement().withUUIDValue(workflowID)
					.withUUIDValue(id).execute();

			for (Row<String, String> row : result.getResult().getRows()) {
				return createTaskFromDB(row);
			}
		} catch (ConnectionException e) {
			
		}
		return null;
	}

	public static AbstractTask createTaskFromDB(Row<String, String> row) {
		ColumnList<String> columns = row.getColumns();

		AbstractTask task = null;
		int taskType = columns.getIntegerValue("type", 0);
		if (taskType == 0) {
			task = new Task();
		} else if (taskType == 1) {
			task = new EndTask();
		}

		task.taskId = columns.getUUIDValue("id", null);

		task.createdAt = new Date(columns.getLongValue("created_at", 0L));

		task.setWorkflowId(columns.getUUIDValue("workflow_id", null));
		task.worker = columns.getStringValue("worker_name", null);

		
		if (task.getTaskType() == 0) {
			((Task)task).loadParameters(); 
		}

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
	public UUID getWorkflowId() {
		return workflowId;
	}

	/**
	 * @param workflowId
	 *            the workflowId to set
	 */
	private void setWorkflowId(UUID workflowId) {
		this.workflowId = workflowId;
	}

	public List<AbstractTask> getParents() {
		if (parents == null) {
			loadParents();
		}
		return parents;
	}

	private void loadParents() {
		parents = new LinkedList<>();
		parentIds = new LinkedList<>();
		try {
			Rows<String, String> rows;

			rows = cs()
					.prepareQuery(Entities.CF_STANDARD1)
					.withCql("SELECT parent_id FROM task_parent WHERE workflow_id=? and id = ?;")
					.asPreparedStatement()
					.withUUIDValue(this.workflowId)
					.withUUIDValue(this.getId())
					.execute().getResult().getRows();

			for (Row<String, String> row : rows) {
				ColumnList<String> c = row.getColumns();
				parentIds.add(c.getUUIDValue("parent_id", null));
			}
			
			for (UUID parentid : parentIds) {
				parents.add(load(workflowId,parentid));
			}
		} catch (ConnectionException e) {
			throw new RuntimeException(e);
		}
	}
}
