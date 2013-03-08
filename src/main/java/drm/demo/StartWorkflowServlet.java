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

import java.io.IOException;
import java.util.Date;
import java.util.List;
import java.util.Map;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.google.appengine.api.blobstore.BlobKey;
import com.google.appengine.api.blobstore.BlobstoreService;
import com.google.appengine.api.blobstore.BlobstoreServiceFactory;

import drm.taskworker.Job;
import drm.taskworker.Service;
import drm.taskworker.config.Config;
import drm.taskworker.tasks.Task;
import drm.taskworker.tasks.WorkflowInstance;
// import this here so entities are always loaded

/**
 * Servlet implementation class StartWorkflowServlet
 * 
 * @author Bart Vanbrabant <bart.vanbrabant@cs.kuleuven.be>
 */
@WebServlet("/start")
public class StartWorkflowServlet extends HttpServlet {
	private BlobstoreService blobstoreService = BlobstoreServiceFactory
			.getBlobstoreService();

	/**
	 * @see HttpServlet#HttpServlet()
	 */
	public StartWorkflowServlet() {
		super();
	}
	
	private String[] getWorkflows() {
		// load the configuration
		Config cfg = Config.loadConfig(this.getServletContext().getResourceAsStream("/WEB-INF/workers.yaml"));
		String[] workflowNames = cfg.getWorkflows().keySet().toArray(new String[0]);
		
		return workflowNames;
	}
	

	
	/**
	 * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse
	 *      response)
	 */
	@Override
	protected void doGet(HttpServletRequest request,
			HttpServletResponse response) throws ServletException, IOException {
		
		request.setAttribute("workflows", this.getWorkflows());
		request.getRequestDispatcher("/start.jsp").forward(request, response);
	}
	
	
	
	/**
	 * @see HttpServlet#doPost(HttpServletRequest request, HttpServletResponse
	 *      response)
	 */
	@Override
	protected void doPost(HttpServletRequest request,
			HttpServletResponse response) throws ServletException, IOException {
		Map<String, List<BlobKey>> blobKeys = 
				blobstoreService.getUploads(request);
		
		Object data = null;
		String textField = request.getParameter("text");
		if (blobKeys.size() == 1 && blobKeys.containsKey("file")) {
			List<BlobKey> keys = blobKeys.get("file");
			data = keys.get(0);
		} else if (textField != null && textField.length() > 0) {
			data = request.getParameter("text");
		} else {
			request.setAttribute("error", "Either file or text should be provided.");
		}
		if (data != null) {
			// get the delay
			int delay = Integer.valueOf(request.getParameter("date"));
			Date when = new Date(System.currentTimeMillis() + (delay * 1000));
			
			// create a workflow and save it
			WorkflowInstance workflow = new WorkflowInstance(request.getParameter("workflow"));
			Task task = workflow.newStartTask();
			task.addParam("arg0", data);
						
			Job newJob = new Job(workflow, task, when);
			Service.get().addJob(newJob);
			
			String id = workflow.getWorkflowId().toString();
			request.setAttribute("workflowId", id);
			request.setAttribute("info", "Started workflow with id <a href=\"/workflow?workflowId=" + id + "\">" + id + "</a>");
		}

		request.setAttribute("workflows", this.getWorkflows());
		request.getRequestDispatcher("/start.jsp").forward(request, response);
	}
	

}
