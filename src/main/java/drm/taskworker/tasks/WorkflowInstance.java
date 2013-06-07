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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.logging.Logger;

import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.CqlResult;
import com.netflix.astyanax.model.Row;

import drm.taskworker.Entities;
import drm.taskworker.Worker;
import drm.taskworker.config.Config;
import drm.taskworker.config.WorkflowConfig;
import drm.taskworker.monitoring.Statistic;

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

	private Date startedAt = null;
	private Date finishedAt = null;

	private List<Statistic> stats = null;

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
	private String resolveStep(String stepName, String nextSymbol) {
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
					.withCql(
							"INSERT INTO workflow (id, workflow_name) VALUES (?, ?);")
					.asPreparedStatement().withUUIDValue(this.workflowId)
					.withStringValue(this.name).execute();
		} catch (ConnectionException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Load a workflow from the database
	 */
	public static WorkflowInstance load(UUID id) {
		try {
			OperationResult<CqlResult<String, String>> result = cs()
					.prepareQuery(Entities.CF_STANDARD1)
					.withCql("SELECT * FROM workflow WHERE id = ?;")
					.asPreparedStatement()
					.withUUIDValue(id).execute();

			for (Row<String, String> row : result.getResult().getRows()) {
			    WorkflowInstance wf = createWorkflow(row);

				return wf;
			}
		} catch (ConnectionException e) {
			e.printStackTrace();
		}
		return null;
	}

	public static WorkflowInstance createWorkflow(Row<String, String> row) {
		ColumnList<String> columns = row.getColumns();
		
		WorkflowInstance wf = new WorkflowInstance();
		wf.workflowId = columns.getUUIDValue("id", null);
		wf.name = columns.getStringValue("workflow_name", null);
		wf.startedAt = columns.getDateValue("started_at", null);
		wf.finishedAt = columns.getDateValue("finished_at", null);
		wf.stats = columns.getValue("stats", Entities.STATS_SERIALISER,null);
		return wf;
	}

	public List<Statistic> getStats() {
		return stats;
	}
	
	/**
	 * Get the history of the workflow.
	 */
	public List<AbstractTask> getHistory() {
		try {
			OperationResult<CqlResult<String, String>> result = cs()
					.prepareQuery(Entities.CF_STANDARD1)
					.withCql("SELECT * FROM task WHERE workflow_id = ?;")
					.asPreparedStatement().withUUIDValue(this.workflowId)
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
					.withCql("UPDATE workflow set stats = ? where id = ?;")
					.asPreparedStatement()
					.withByteBufferValue(out, Entities.STATS_SERIALISER)
					.withUUIDValue(this.workflowId).execute();
		} catch (ConnectionException e) {
			e.printStackTrace();
		}

	}

	/**
	 * Get all workflows
	 */
	public static List<WorkflowInstance> getAll() {
		try {
			OperationResult<CqlResult<String, String>> result = cs()
					.prepareQuery(Entities.CF_STANDARD1)
					.withCql("SELECT * FROM workflow;").asPreparedStatement()
					.execute();

			List<WorkflowInstance> workflows = new ArrayList<>();
			for (Row<String, String> row : result.getResult().getRows()) {
			    WorkflowInstance wf = createWorkflow(row);
			    workflows.add(wf);
			}

			return workflows;
		} catch (ConnectionException e) {
			e.printStackTrace();
		}
		return null;
	}
	

	/**
	 * Has this workflow finished?
	 * @return
	 */
	public boolean isFinished() {
		if (finishedAt != null) {
			return true;
		}
		return false;
	}
	
	/**
	 * Has this workflow started?
	 */
	public boolean isStarted() {
		if (this.startedAt != null) {
			return true;
		}
		return false;
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
		
		try {
			Keyspace cs = cs();
			cs.prepareQuery(Entities.CF_STANDARD1)
					.withCql("UPDATE workflow SET started_at = ? WHERE id = ?;")
					.asPreparedStatement()
					.withLongValue(this.startedAt.getTime())		// started_at
					.withUUIDValue(this.getWorkflowId()) 			// workflow_id
		            .execute();
		} catch (ConnectionException e) {
			e.printStackTrace();
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
		
		try {
			Keyspace cs = cs();
			cs.prepareQuery(Entities.CF_STANDARD1)
					.withCql("UPDATE workflow SET finished_at = ? WHERE id = ?;")
					.asPreparedStatement()
					.withLongValue(this.finishedAt.getTime())		// finished_at
					.withUUIDValue(this.getWorkflowId()) 			// workflow_id
		            .execute();
		} catch (ConnectionException e) {
			e.printStackTrace();
		}
	}
	
}
