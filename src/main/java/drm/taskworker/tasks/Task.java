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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;

import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.CqlResult;
import com.netflix.astyanax.model.Row;
import com.netflix.astyanax.serializers.ObjectSerializer;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.TimerContext;

import drm.taskworker.Entities;

/**
 * A task that needs to be executed by a worker
 * 
 * @author Bart Vanbrabant <bart.vanbrabant@cs.kuleuven.be>
 */
public class Task extends AbstractTask {
	private Map<String, Object> params = new HashMap<String, Object>();
	private transient TimerContext timer;

	/**
	 * Create a task for a worker
	 * 
	 * @param parent
	 *            The parent of this task
	 * @param worker
	 *            The name of the worker
	 */
	public Task(AbstractTask parent, String worker) {
		super(parent.getWorkflow(), parent, worker);
	}

	/**
	 * Create a task for a worker
	 * 
	 * @param parents
	 *            The parents of this task, this collection should contain at
	 *            least one parent.
	 * @param worker
	 *            The name of the worker
	 */
	public Task(List<AbstractTask> parents, String worker) {
		super(parents, worker);
	}

	/**
	 * Constructor for persistence
	 */
	Task() {
		super();
	}

	/**
	 * Create a task that starts the workflow.
	 * 
	 * @param workflow
	 * @param worker
	 */
	public Task(WorkflowInstance workflow, String worker) {
		this(workflow, null, worker);
	}

	/**
	 * Create a task for a worker
	 * 
	 * @param parent
	 *            The parent of this task
	 * @param worker
	 *            The name of the worker
	 */
	protected Task(WorkflowInstance workflow, AbstractTask parent, String worker) {
		super(workflow, parent, worker);
	}


	public Task(AbstractTask one, List<UUID> parents, String worker) {
		super(one,parents,worker);
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
		if(params == null) {
			loadParameters();
		}
		this.params.put(name, value);
	}

	/**
	 * Get a parameter with the given name.
	 * 
	 * @param name
	 *            The name of the parameter
	 * @return The value
	 */
	public Object getParam(String name) {
		if(params == null) {
			loadParameters();
		}
		return this.params.get(name);
	}

	/**
	 * Check if a parameter exists
	 * 
	 * @param name
	 * @return
	 */
	public boolean hasParam(String name) {
		return this.params.containsKey(name);
	}

	/**
	 * Get a set of parameter names.
	 * 
	 * @return A set of names
	 */
	public Set<String> getParamNames() {
		return this.params.keySet();
	}

	@Override
	public int getTaskType() {
		return 0;
	}

	public void setStartedAt() {
		timer = Metrics.newTimer(getClass(), getWorker()).time();
		super.setStartedAt();
	}

	public void setFinishedAt() {
		timer.stop();
		super.setFinishedAt();
	}

	/**
	 * Save the task to the database
	 */
	public void save() {
		super.save();

		try {
			for (Entry<String, Object> param : params.entrySet()) {
				cs().prepareQuery(Entities.CF_STANDARD1)
						.withCql("INSERT INTO parameter (task_id, name, value) VALUES (?, ?, ?);")
						.asPreparedStatement()
						.withUUIDValue(this.getId())
						.withStringValue(param.getKey())
						.withByteBufferValue(param.getValue(),
								ObjectSerializer.get()).execute();
			}
		} catch (ConnectionException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Load the parameters of this task from the database
	 */
	public void loadParameters() {
		try {
			OperationResult<CqlResult<String, String>> result = cs()
					.prepareQuery(Entities.CF_STANDARD1)
					.withCql("SELECT * FROM parameter WHERE task_id = ?")
					.asPreparedStatement().withUUIDValue(this.getId())
					.execute();

			for (Row<String, String> row : result.getResult().getRows()) {
				ColumnList<String> columns = row.getColumns();

				String name = columns.getStringValue("name", null);
				Object value = ObjectSerializer.get().fromByteBuffer(
						columns.getByteBufferValue("value", null));

				this.addParam(name, value);
			}
		} catch (ConnectionException e) {
			e.printStackTrace();
		}
	}

	@Override
	public String toString() {
		return String.format("Task [workflow=%s, id=%s, worker=%s, params=%s]",
				getWorkflowId(), getId(), getWorker(), params);
	}

	

}
