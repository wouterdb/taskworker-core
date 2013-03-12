/**
 *
 *     Copyright 2013 KU Leuven Research and Development - iMinds - Distrinet
 *
 *     Licensed under the Apache License, Version 2.0 (the "License");
 *     you may not use this file except in compliance with the License.
 *     You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 *
 *     Administrative Contact: dnet-project-office@cs.kuleuven.be
 *     Technical Contact: bart.vanbrabant@cs.kuleuven.be
 */
package drm.demo;


import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import drm.taskworker.tasks.AbstractTask;
import drm.taskworker.tasks.WorkflowInstance;

/**
 * Servlet implementation class Workflow
 */
@WebServlet("/workflow")
public class WorkflowServlet extends HttpServlet {
	private static final long serialVersionUID = 1L;

	/**
	 * @see HttpServlet#HttpServlet()
	 */
	public WorkflowServlet() {
		super();
	}

	/**
	 * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse
	 *      response)
	 */
	protected void doGet(HttpServletRequest request,
			HttpServletResponse response) throws ServletException, IOException {
		WorkflowInstance workflow = null;

		List<drm.taskworker.tasks.WorkflowInstance> workflows = new ArrayList<drm.taskworker.tasks.WorkflowInstance>();
		for (drm.taskworker.tasks.WorkflowInstance wf : WorkflowInstance.getAll()) {
			workflows.add(wf);
		}

		request.setAttribute("workflows", workflows);

		Map<String, Integer> stats = new HashMap<>();
		Date started = new Date();
		Date finished = new Date(0);
		if (request.getParameter("workflowId") != null) {
			// load the workflow
			workflow = WorkflowInstance.load(UUID.fromString(request.getParameter("workflowId")));
			
			if (workflow != null) {
				for (AbstractTask task : workflow.getHistory()) {
					if (!stats.containsKey(task.getWorker())) {
						stats.put(task.getWorker(), 0);
					}
					int value = stats.get(task.getWorker()) + 1;
					stats.put(task.getWorker(), value);
					
					if (task.getStartedAt() != null && task.getStartedAt().before(started)) {
						started = task.getStartedAt();
					}
					
					if (task.getFinishedAt() != null && task.getFinishedAt().after(finished)) {
						finished = task.getFinishedAt();
					}
				}
			}
		}
		
		request.setAttribute("started", started);
		request.setAttribute("finished", finished);
		request.setAttribute("stats", stats);
		request.setAttribute("workflow", workflow);
		request.getRequestDispatcher("/workflow.jsp").forward(request, response);
	}
}
