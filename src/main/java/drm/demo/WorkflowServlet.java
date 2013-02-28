package drm.demo;

import static com.googlecode.objectify.ObjectifyService.ofy;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.google.appengine.api.datastore.QueryResultIterable;
import com.googlecode.objectify.NotFoundException;

import drm.taskworker.Entities;
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

		QueryResultIterable<drm.taskworker.Workflow> workflowList = ofy()
				.load().type(drm.taskworker.Workflow.class).iterable();

		List<drm.taskworker.Workflow> workflows = new ArrayList<drm.taskworker.Workflow>();
		for (drm.taskworker.Workflow wf : workflowList) {
			workflows.add(wf);
		}

		request.setAttribute("workflows", workflows);

		AbstractTask root = null;
		Map<AbstractTask, List<AbstractTask>> graph = new HashMap<AbstractTask, List<AbstractTask>>();
		
		if (request.getParameter("workflowId") != null) {
			// load the workflow
			try { 
				workflow = ofy().load().type(Workflow.class).id(request.getParameter("workflowId")).safeGet();
			} catch (NotFoundException e) {
				// do nothing
			}
		
			// create a graph

			int max = 0;
			for (AbstractTask task : workflow.getHistory()) {
				if (task.getParentTask() == null) {
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

	static {
		Entities.register();
	}
}
