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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.CqlResult;
import com.netflix.astyanax.model.Row;
import com.netflix.astyanax.serializers.ObjectSerializer;

import drm.taskworker.Entities;
import drm.taskworker.Job;

/**
 * A task that needs to be executed by a worker
 * 
 * @author Bart Vanbrabant <bart.vanbrabant@cs.kuleuven.be>
 */
public class Task extends AbstractTask {
	private Map<String, ValueRef> params = new HashMap<>();

	/**
	 * Create a task for a worker
	 * 
	 * @param parent
	 *            The parent of this task
	 * @param worker
	 *            The name of the worker
	 */
	public Task(AbstractTask parent, String worker) {
		super(parent.getJobId(), parent, worker);
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
	public Task(Job job, String worker) {
		super(job.getJobId(), null, worker);
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
	 * @return The valuee
	 * @throws ParameterFoundException 
	 */
	public Object getParam(String name) throws ParameterFoundException {
		return getParamRef(name).getValue();
	}
	
	/**
	 * Get a value reference to the parameter
	 */
	public ValueRef getParamRef(String name) {
		return new ValueRef(this, name);
	}

	/**
	 * Get a list of value references
	 */
	public List<ValueRef> getParamRefs() {
		List<ValueRef> refs = new ArrayList<>();
		
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
				Object value = ObjectSerializer.get().fromByteBuffer(columns.getByteBufferValue("value", null));

				ValueRef ref = new ValueRef(jobId, taskId, name);
				ref.setValue(value);
				
				refs.add(ref);
			}
		} catch (ConnectionException e) {
			e.printStackTrace();
		}
		
		return refs;
	}

	@Override
	public int getTaskType() {
		return 0;
	}

	public void setStartedAt() {
		super.setStartedAt();
	}

	public void setFinishedAt() {
		super.setFinishedAt();
	}

	/**
	 * Save the task to the database
	 */
	public void save() {
		super.save();

		// save all value refs that have not been saved
		for (ValueRef ref : params.values()) {
			ref.save();
		}
	}

	@Override
	public String toString() {
		return String.format("Task [job=%s, id=%s, worker=%s, nparams=%d]",
				getJobId(), getId(), getWorker(), params.size());
	}

}
