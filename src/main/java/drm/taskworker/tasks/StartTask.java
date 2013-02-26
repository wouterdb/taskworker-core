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

import drm.taskworker.Workflow;

/**
 * The root task of a workflow
 *
 * @author Bart Vanbrabant <bart.vanbrabant@cs.kuleuven.be>
 */
public class StartTask extends Task {
	/**
	 * Create a task that starts the workflow.
	 * @param workflow
	 * @param worker
	 */
	public StartTask(Workflow workflow, String worker) {
		super(workflow, null, worker);
	}

	@Override
	public String getTaskType() {
		return "start";
	}
}
