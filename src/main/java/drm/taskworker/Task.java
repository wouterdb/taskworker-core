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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import com.google.appengine.api.taskqueue.TaskOptions;

/**
 * A task that needs to be executed by a worker
 *
 * TODO Add the history of the task
 * 
 * @author bart
 */
@SuppressWarnings("serial")
public class Task implements Serializable {
	private Map<String,Object> params = new HashMap<String, Object>();
	private String worker = null;;
	
	/**
	 * Create a task for a worker
	 * 
	 * @param worker The name of the worker
	 */
	public Task(String worker) {
		this.worker = worker;
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
	
	public String getWorker() {
		return this.worker;
	}
	
	public Set<String> getParamNames() {
		return this.params.keySet();
	}
	
	/**
	 * Convert this task to a taskoption object.
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
}
