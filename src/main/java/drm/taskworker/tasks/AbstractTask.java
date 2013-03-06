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
import java.util.UUID;

import com.google.appengine.api.taskqueue.TaskOptions;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.CqlResult;
import com.netflix.astyanax.model.Row;

import drm.taskworker.Entities;


/**
 * A baseclass for all tasks.
 *
 * @author Bart Vanbrabant <bart.vanbrabant@cs.kuleuven.be>
 */
public abstract class AbstractTask implements Serializable {
	public static final UUID NONE = UUID.fromString("00000000-0000-0000-0000-000000000000");
	
	private UUID taskId = null;
	
	private String worker = null;
	private String symbolWorker = null;
	private Date createdAt = null;
	private Date startedAt = null;
	private Date finishedAt = null;
	
	private UUID workflowId = null;
	
	/*
	 * Same comment here as for workflow 
	 */
	private UUID parentId = NONE;
	
	/**
	 * Create a task for a worker
	 * 
	 * @param workflow The workflow this task belongs to
	 * @param parent The parent of this task
	 * @param worker The name of the worker
	 */
	public AbstractTask(WorkflowInstance workflow, AbstractTask parentTask, String worker) {
		this();
		
		this.setWorkflowId(workflow.getWorkflowId());
		
		// lookup the next worker
		if (parentTask != null) {
			this.parentId = parentTask.getId();
			this.worker = workflow.resolveStep(parentTask.getWorker(), worker);
		} else {
			this.worker = worker;
		}
		
		// set the date the task was created
		this.setCreatedAt(new Date());
	}
	
	public AbstractTask() {
		this.taskId = UUID.randomUUID();
	}
	
	/**
	 * Get the id of this task.
	 */
	public UUID getId() {
		return this.taskId;
	}
	
	/**
	 * Get the parent of this task.
	 */
	public AbstractTask getParentTask() {
		if (this.parentId.equals(NONE)) {
			// this is a root node
			return null;
		}
		
		return AbstractTask.load(this.getWorkflowId(), this.parentId);
	}
	
	/**
	 * Returns the string id of this type of task
	 * 
	 * @return A string that can be used to determine the type of task.
	 */
	public abstract String getTaskType();
	
	/**
	 * Convert this task to a taskoption object.
	 * 
	 * @return
	 */
	public TaskOptions toTaskOption() throws IOException {
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		ObjectOutputStream oos = new ObjectOutputStream(bos);
		oos.writeObject(this);
		
	    TaskOptions to = TaskOptions.Builder.withMethod(TaskOptions.Method.PULL);
		to.payload(bos.toByteArray());
		
		oos.close();
		bos.close();
		
		to.tag(this.getWorker());
		
		return to;
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
	public WorkflowInstance getWorkflow() {
		WorkflowInstance wf = WorkflowInstance.load(this.getWorkflowId());
		assert(wf != null);
		return wf;
	}

	/**
	 * @return the createdAt
	 */
	public Date getCreatedAt() {
		return createdAt;
	}

	/**
	 * @param createdAt the createdAt to set
	 */
	public void setCreatedAt(Date createdAt) {
		this.createdAt = createdAt;
	}

	/**
	 * @return the startedAt
	 */
	public Date getStartedAt() {
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
		/* CREATE TABLE task (
		  id uuid PRIMARY KEY,
		  created_at uuid,
		  finished_at uuid,
		  parent_id uuid,
		  started_at uuid,
		  type text,
		  worker_name text,
		  workflow_id uuid
		)*/
		try {
			cs().prepareQuery(Entities.CF_STANDARD1)
					.withCql("INSERT INTO task (id, created_at, parent_id, type, worker_name, workflow_id) " + 
							 " VALUES (?, ?, ?, ?, ?, ?);")
					.asPreparedStatement()
		            .withUUIDValue(this.getId())					// id
		            .withLongValue(this.createdAt.getTime())		// created_at
		            .withUUIDValue(this.parentId)					// parent_id
		            .withStringValue(this.getTaskType())			// type
		            .withStringValue(this.getWorker())				// worker_name
		            .withUUIDValue(this.getWorkflowId())					// workflow_id
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
	public static AbstractTask load(UUID workflowID, UUID id) {
		try {
			OperationResult<CqlResult<String, String>> result = cs().prepareQuery(Entities.CF_STANDARD1)
				.withCql("SELECT * FROM task WHERE workflow_id = ? AND id = ?;")
				.asPreparedStatement()
				.withUUIDValue(workflowID)
				.withUUIDValue(id)
				.execute();
			
			for (Row<String, String> row : result.getResult().getRows()) {
			    return createTaskFromDB(row);
			}
		} catch (ConnectionException e) {
			e.printStackTrace();
		}
		return null;
	}

	public static AbstractTask createTaskFromDB(Row<String, String> row) {
		ColumnList<String> columns = row.getColumns();
		
		AbstractTask task = null;
		String taskType = columns.getStringValue("type", "task");
		if (taskType.equals("work")) {
			task = new Task();
		} else if (taskType.equals("end")) {
			task = new EndTask();
		}
		
		task.taskId = columns.getUUIDValue("id", null);
		
		task.createdAt = new Date(columns.getLongValue("created_at", 0L));
		task.startedAt = new Date(columns.getLongValue("finished_at", 0L));
		if (task.startedAt.getTime() == 0) {
			task.startedAt = null;
		}
		task.finishedAt = new Date(columns.getLongValue("started_at", 0L));
		if (task.finishedAt.getTime() == 0) {
			task.finishedAt = null;
		}
		
		task.parentId = columns.getUUIDValue("parent_id", null);
		task.setWorkflowId(columns.getUUIDValue("workflow_id", null));
		
		task.worker = columns.getStringValue("worker_name", null);
		
		return task;
	}

	/**
	 * Save the start and finish timings
	 */
	public void saveTiming() {
		try {
			Keyspace cs = cs();
			cs.prepareQuery(Entities.CF_STANDARD1)
					.withCql("UPDATE task SET started_at = ?, finished_at = ? WHERE workflow_id = ? AND id = ?;")
					.asPreparedStatement()
					.withLongValue(this.startedAt.getTime())		// started_at
					.withLongValue(this.finishedAt.getTime())		// finished_at
					.withUUIDValue(this.getWorkflowId()) 				// workflow_id
		            .withUUIDValue(this.getId())					// id
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
	 * @param workflowId the workflowId to set
	 */
	private void setWorkflowId(UUID workflowId) {
		this.workflowId = workflowId;
	}
}