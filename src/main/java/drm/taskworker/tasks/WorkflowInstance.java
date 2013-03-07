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

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import java.util.logging.Logger;

import com.google.appengine.api.taskqueue.Queue;
import com.google.appengine.api.taskqueue.QueueFactory;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.CqlResult;
import com.netflix.astyanax.model.Row;

import drm.taskworker.Entities;
import drm.taskworker.Worker;
import drm.taskworker.config.Config;
import drm.taskworker.config.WorkflowConfig;

/**
 * This class manages a workflow
 * 
 * @author Bart Vanbrabant <bart.vanbrabant@cs.kuleuven.be>
 */
public class WorkflowInstance implements Serializable {
	private static Logger logger = Logger.getLogger(Worker.class.getCanonicalName());
	private drm.taskworker.config.WorkflowConfig workflowConfig = null;

	private String name = null;
	private UUID workflowId = null;
	
	private Date startAt = null;
	private Date deadline = null;

	/**
	 * Create a new workflow instance
	 * 
	 * @param name
	 *            The name of the workflow to start an instance of
	 */
	public WorkflowInstance(String name) {
		this();
		this.setName(name);
		this.loadConfig();
	}

	/**
	 * Default constructor, that only sets a UUID
	 */
	public WorkflowInstance() {
		this.workflowId = UUID.randomUUID();
	}

	/**
	 * Load the configuration of this workflow
	 */
	private void loadConfig() {
		Config cfg = Config.getConfig();
		this.setWorkflowConfig(cfg.getWorkflow(this.name));
	}

	/**
	 * The name of the workflow. This name is a type of workflow, an instance of
	 * this workflow is identified with the workflowId
	 * 
	 * @return the name
	 */
	public String getName() {
		return name;
	}

	/**
	 * @param name
	 *            the name to set
	 */
	public void setName(String name) {
		this.name = name;
	}

	/**
	 * Resolve a symbolic next step for the given current step. If the next
	 * symbol is not defined, the next symbol parameter itself will be returned.
	 * 
	 * @param stepName
	 *            The step to lookup the next step for
	 * @param nextSymbol
	 *            The next symbol that needs to be resolved.
	 * @return The actual next step.
	 */
	public String resolveStep(String stepName, String nextSymbol) {
		return this.getWorkflowConfig().getNextStep(stepName, nextSymbol);
	}

	/**
	 * Get a new task in this workflow
	 * 
	 * @param worker
	 *            The work on which this task should be allocated.
	 * @return A new task that can be added to the queue
	 */
	public Task newTask(Task parent, String worker) {
		return new Task(parent, worker);
	}

	/**
	 * The string representation of this workflow.
	 */
	@Override
	public String toString() {
		return "workflow-" + this.workflowId;
	}

	/**
	 * Get the unique id of this workflow.
	 * 
	 * @return
	 */
	public UUID getWorkflowId() {
		return this.workflowId;
	}

	/**
	 * Create a new task that starts the workflow
	 */
	public Task newStartTask() {
		return new Task(this, this.getWorkflowConfig().getWorkflowStart());
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
	 * Save the workflow to the datastore
	 */
	public void save() {
		try {
			cs().prepareQuery(Entities.CF_STANDARD1)
					.withCql("INSERT INTO workflow (id, workflow_name) VALUES (?, ?);")
					.asPreparedStatement()
		            .withUUIDValue(this.workflowId)
		            .withStringValue(this.name)
		            .execute();
		} catch (ConnectionException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * Load a workflow from the database
	 */
	public static WorkflowInstance load(UUID id) {
		try {
			OperationResult<CqlResult<String, String>> result = cs().prepareQuery(Entities.CF_STANDARD1)
				.withCql("SELECT id, workflow_name FROM workflow WHERE id = ?;")
				.asPreparedStatement()
				.withUUIDValue(id)
				.execute();
			
			for (Row<String, String> row : result.getResult().getRows()) {
			    ColumnList<String> columns = row.getColumns();
			    
			    WorkflowInstance wf = new WorkflowInstance();
			    wf.workflowId = columns.getUUIDValue("id", null);
			    wf.name = columns.getStringValue("workflow_name", null);
			    
			    return wf;
			}
		} catch (ConnectionException e) {
			e.printStackTrace();
		}
		return null;
	}

	/**
	 * Get the history of the workflow.
	 */
	public List<AbstractTask> getHistory() {
		try {
			OperationResult<CqlResult<String, String>> result = cs().prepareQuery(Entities.CF_STANDARD1)
					.withCql("SELECT * FROM task WHERE workflow_id = ?;")
					.asPreparedStatement()
					.withUUIDValue(this.workflowId)
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
	
	/**
	 * Get all workflows 
	 */
	public static List<WorkflowInstance> getAll() {
		try {
			OperationResult<CqlResult<String, String>> result = cs().prepareQuery(Entities.CF_STANDARD1)
					.withCql("SELECT * FROM workflow;")
					.asPreparedStatement()
					.execute();
			
			List<WorkflowInstance> workflows = new ArrayList<>();
			for (Row<String, String> row : result.getResult().getRows()) {
			    ColumnList<String> columns = row.getColumns();
			    
			    WorkflowInstance wf = new WorkflowInstance();
			    wf.workflowId = columns.getUUIDValue("id", null);
			    wf.name = columns.getStringValue("workflow_name", null);
			    
			    workflows.add(wf);
			}
			
			return workflows;
		} catch (ConnectionException e) {
			e.printStackTrace();
		}
		return null;
	}
}
