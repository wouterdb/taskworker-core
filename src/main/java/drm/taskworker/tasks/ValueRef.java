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
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;

import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.CqlResult;
import com.netflix.astyanax.model.Row;
import com.netflix.astyanax.serializers.ObjectSerializer;

import drm.taskworker.Entities;

/**
 * A reference to a value in the store 
 */
@SuppressWarnings("serial")
public class ValueRef implements Serializable {
	private UUID jobId;
	private UUID taskId;
	private String keyName;
	private transient Object value;
	
	public ValueRef(Task task, String keyName) {
		this.jobId = task.getJobId();
		this.taskId = task.getId();
		this.keyName = keyName;
	}
	
	public ValueRef(UUID jobId, UUID taskId, String keyString) {
		this.jobId = jobId;
		this.taskId = taskId;
		this.keyName = keyString;
	}
	
	public UUID getTaskId() {
		return taskId;
	}
	
	public UUID getJobId() {
		return jobId;
	}

	public String getKeyName() {
		return keyName;
	}
	
	public Object getValue() throws ParameterFoundException {
		if (this.value == null) {
			this.loadValue();
			
			if (this.value == null) {
				throw new ParameterFoundException();
			}
		}
		if (this.value instanceof ValueRef) {
			return ((ValueRef)this.value).getValue();
		}
		
		return this.value;
	}
	
	/**
	 * Save the task to the database
	 */
	public void save() {
		try {
			cs().prepareQuery(Entities.CF_STANDARD1)
				.withCql("INSERT INTO parameter (job_id, task_id, name, value) VALUES (?, ?, ?, ?);")
				.asPreparedStatement()
				.withUUIDValue(this.getJobId())
				.withUUIDValue(this.getTaskId())
				.withStringValue(this.getKeyName())
				.withByteBufferValue(this.value, ObjectSerializer.get())
				.execute();
		} catch (ConnectionException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Load the parameters of this task from the database
	 */
	private void loadValue() {
		try {
			OperationResult<CqlResult<String, String>> result = cs()
					.prepareQuery(Entities.CF_STANDARD1)
					.withCql("SELECT value FROM parameter WHERE job_id = ? AND task_id = ? AND name = ?")
					.asPreparedStatement()
					.withUUIDValue(this.getJobId())
					.withUUIDValue(this.getTaskId())
					.withStringValue(this.keyName)
					.execute();

			for (Row<String, String> row : result.getResult().getRows()) {
				ColumnList<String> columns = row.getColumns();

				Object value = ObjectSerializer.get().fromByteBuffer(
						columns.getByteBufferValue("value", null));

				this.value = value;
			}
		} catch (ConnectionException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Set the value, the data will not yet be saved.
	 *  
	 * @param val
	 */
	public void setValue(Object val) {
		this.value = val;
	}

	public void flatten() throws ParameterFoundException {
		value = getValue();
		
		if(value instanceof Collection){
			Collection c = (Collection) value;
			List newvalue = new LinkedList<>();
			value = newvalue;
			for(Object o:c){
				if(o instanceof ValueRef){
					ValueRef child = (ValueRef)o;
					child.flatten();
					newvalue.add(child.getValue());
				}else
					newvalue.add(o);
			}
		}
		
	}
	
}
