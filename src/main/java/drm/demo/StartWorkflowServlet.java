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

package drm.demo;

import static com.googlecode.objectify.ObjectifyService.ofy;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.google.appengine.api.blobstore.BlobKey;
import com.google.appengine.api.blobstore.BlobstoreService;
import com.google.appengine.api.blobstore.BlobstoreServiceFactory;
import com.googlecode.objectify.ObjectifyService;

import drm.taskworker.Workflow;
import drm.taskworker.tasks.AbstractTask;
import drm.taskworker.tasks.EndTask;
import drm.taskworker.tasks.StartTask;
import drm.taskworker.tasks.Task;

/**
 * Servlet implementation class StartWorkflowServlet
 * 
 * @author Bart Vanbrabant <bart.vanbrabant@cs.kuleuven.be>
 */
public class StartWorkflowServlet extends HttpServlet {
	private BlobstoreService blobstoreService = BlobstoreServiceFactory
			.getBlobstoreService();

	/**
	 * @see HttpServlet#HttpServlet()
	 */
	public StartWorkflowServlet() {
		super();
	}

	/**
	 * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse
	 *      response)
	 */
	@Override
	protected void doGet(HttpServletRequest request,
			HttpServletResponse response) throws ServletException, IOException {
	}

	/**
	 * @see HttpServlet#doPost(HttpServletRequest request, HttpServletResponse
	 *      response)
	 */
	@Override
	protected void doPost(HttpServletRequest request,
			HttpServletResponse response) throws ServletException, IOException {
		
//		/ObjectifyService.register(Workflow.class);
		Map<String, List<BlobKey>> blobKeys = 
				blobstoreService.getUploads(request);

		String id = "";
		if (blobKeys.containsKey("file")) {
			List<BlobKey> keys = blobKeys.get("file");

			if (keys.size() != 1) {
				throw new IllegalArgumentException(
						"Exactly one input file is expected!");
			}

			BlobKey key = keys.get(0);
			
			// create a workflow and save it
			Workflow workflow = new Workflow("invoices");
			ofy().save().entities(workflow);
			
			StartTask task = workflow.newStartTask();
			task.addParam("arg0", key);
			
			workflow.startNewWorkflow(task, true);
			id = workflow.getWorkflowId();
			
			ofy().save().entities(workflow);
		}

		response.sendRedirect("/invoices.jsp?id=workflow-" + id);
	}
	
	static {
		ObjectifyService.register(Workflow.class);
		ObjectifyService.register(AbstractTask.class);
		ObjectifyService.register(Task.class);
		ObjectifyService.register(StartTask.class);
		ObjectifyService.register(EndTask.class);
	}
}

