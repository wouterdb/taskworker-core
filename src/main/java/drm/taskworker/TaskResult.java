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

import java.util.ArrayList;
import java.util.List;

/**
 * Instances of this class are used by workers to return a result. 
 * 
 * @author bart
 *
 */
public class TaskResult {
	enum Result {SUCCESS, ERROR};
	
	private Result result = null;
	private List<Task> tasks = null;

	public TaskResult() {
		this.tasks = new ArrayList<Task>();
	}
	
	/**
	 * @return the result
	 */
	public Result getResult() {
		return result;
	}

	/**
	 * @param result the result to set
	 */
	public TaskResult setResult(Result result) {
		this.result = result;
		return this;
	}
	
	public void addNextTask(Task task) {
		System.err.println("new task for " + task.getWorker());
		this.tasks.add(task);
	}
	
	/**
	 * Does this task fork the workflow?
	 */
	public boolean isFork() {
		return this.tasks.size() > 1;
	}
	
	public List<Task> getNextTasks() {
		return this.tasks;
	}
}
