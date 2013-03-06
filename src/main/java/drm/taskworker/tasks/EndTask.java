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




/**
 * A task used to signal that this is the end of a workflow
 * 
 * @author Bart Vanbrabant <bart.vanbrabant@cs.kuleuven.be>
 */
public class EndTask extends AbstractTask {
	/**
	 * Signal the end of a workflow to the next step in the workflow 
	 * 
	 * @param parent The parent of this task
	 * @param worker The name of the worker
	 */
	public EndTask(AbstractTask parent, String worker) {
		super(parent.getWorkflow(), parent, worker);
	}
	
	public EndTask() {
		super();
	}

	@Override
	public String getTaskType() {
		return "end";
	}
}
