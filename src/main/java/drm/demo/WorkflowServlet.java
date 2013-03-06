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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import drm.taskworker.Workflow;
import drm.taskworker.tasks.AbstractTask;

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
		Workflow workflow = null;

		List<drm.taskworker.Workflow> workflows = new ArrayList<drm.taskworker.Workflow>();
		for (drm.taskworker.Workflow wf : Workflow.getAll()) {
			workflows.add(wf);
		}

		request.setAttribute("workflows", workflows);

		AbstractTask root = null;
		Map<AbstractTask, List<AbstractTask>> graph = new HashMap<AbstractTask, List<AbstractTask>>();
		
		if (request.getParameter("workflowId") != null) {
			// load the workflow
			workflow = Workflow.load(UUID.fromString(request.getParameter("workflowId")));
		
			// create a graph

			int max = 0;
			for (AbstractTask task : workflow.getHistory()) {
				if (task.getParentTask().equals(AbstractTask.NONE)) {
					root = task;
				} else {
					if (!graph.containsKey(task.getParentTask())) {
						graph.put(task.getParentTask(), new ArrayList<AbstractTask>());
					}
					graph.get(task.getParentTask()).add(task);
					
					if (graph.get(task.getParentTask()).size() > max) {
						max = graph.get(task.getParentTask()).size();
					}
				}
			}

		}
		
		request.setAttribute("graph", graph);
		request.setAttribute("root", root);
		request.setAttribute("workflow", workflow);
		request.getRequestDispatcher("/workflow.jsp").forward(request, response);
	}
}
