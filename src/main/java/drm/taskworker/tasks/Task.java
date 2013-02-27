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

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import com.googlecode.objectify.annotation.EntitySubclass;

import drm.taskworker.Workflow;

/**
 * A task that needs to be executed by a worker
 *
 * TODO Add the history of the task
 * 
 * @author Bart Vanbrabant <bart.vanbrabant@cs.kuleuven.be>
 */
@EntitySubclass(index=true)
public class Task extends AbstractTask {
	private Map<String,Object> params = new HashMap<String, Object>();

	/**
	 * Create a task for a worker
	 * 
	 * @param parent The parent of this task
	 * @param worker The name of the worker
	 */
	public Task(AbstractTask parent, String worker) {
		super(parent.getWorkflow(), parent, worker);
	}
	
	public Task() {
		super();
	}
	
	/**
	 * Create a task for a worker
	 * 
	 * @param parent The parent of this task
	 * @param worker The name of the worker
	 */
	protected Task(Workflow workflow, AbstractTask parent, String worker) {
		super(workflow, parent, worker);
	}
	
	/**
	 * Add a parameter to the task
	 * 
	 * @param name The name of the parameter
	 * @param value The value of the parameter
	 */
	public void addParam(String name, Object value) {
		this.params.put(name, value);
	}
	
	/**
	 * Get a parameter with the given name. 
	 * 
	 * @param name The name of the parameter
	 * @return The value
	 */
	public Object getParam(String name) {
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
	public String getTaskType() {
		return "work";
	}

}
