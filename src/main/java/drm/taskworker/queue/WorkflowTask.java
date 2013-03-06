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

package drm.taskworker.queue;

import org.hibernate.search.annotations.Analyze;
import org.hibernate.search.annotations.Field;
import org.hibernate.search.annotations.Indexed;
import org.hibernate.search.annotations.Norms;
import org.hibernate.search.annotations.ProvidedId;
import org.hibernate.search.annotations.TermVector;

import com.google.appengine.api.taskqueue.RetryOptions;
import com.google.appengine.api.taskqueue.TaskOptions;

@Indexed
@ProvidedId
public class WorkflowTask extends org.jboss.capedwarf.tasks.Task {
    public static final String WORKFLOW = "workflow";
    public static final String TYPE = "type";

    private String workflowID;
    private String type;

	
    public WorkflowTask() {
    	super();
    }

    public WorkflowTask(String name, String queue, String tag, Long eta, TaskOptions options, RetryOptions retry) {
        super(name, queue, tag, eta, options, retry);
    }

	/**
	 * @return the workflowID
	 */
    @Field(name = WORKFLOW, analyze = Analyze.NO, norms = Norms.NO, termVector = TermVector.NO)
	public String getWorkflowID() {
		return workflowID;
	}

	/**
	 * @param workflowID the workflowID to set
	 */
	public void setWorkflowID(String workflowID) {
		this.workflowID = workflowID;
	}

	/**
	 * @return the type
	 */
	@Field(name = TYPE, analyze = Analyze.NO, norms = Norms.NO, termVector = TermVector.NO)
	public String getType() {
		return type;
	}

	/**
	 * @param type the type to set
	 */
	public void setType(String type) {
		this.type = type;
	}
}
