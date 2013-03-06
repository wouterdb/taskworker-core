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

import drm.taskworker.tasks.WorkflowInstance;

/**
 * This class implements a workflow service. This service should be stateless
 * with all state in the queues and storage.
 *
 * @author Bart Vanbrabant <bart.vanbrabant@cs.kuleuven.be>
 */
public class Service {
	private static final ThreadLocal<Service> serviceInstance = new ThreadLocal<Service>() {
		@Override
		protected Service initialValue() {
			return new Service();
		}
	};
	
	public static Service get() {
		return serviceInstance.get();
	}
	
	/**
	 * Create a new instance of the workflow service.
	 */
	public Service() {
	}

	/**
	 * Add a new workflow to the service and start it immediately
	 * 
	 * @param workflow The workflow to start
	 */
	public void startWorkflow(WorkflowInstance workflow) {
		
	}
}
