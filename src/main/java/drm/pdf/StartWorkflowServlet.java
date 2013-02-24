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

package drm.pdf;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.ObjectOutputStream;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.fileupload.FileItemIterator;
import org.apache.commons.fileupload.FileItemStream;
import org.apache.commons.fileupload.FileUploadException;
import org.apache.commons.fileupload.servlet.ServletFileUpload;

import com.google.appengine.api.taskqueue.Queue;
import com.google.appengine.api.taskqueue.QueueFactory;
import com.google.appengine.api.taskqueue.TaskOptions;

/**
 * Servlet implementation class StartWorkflowServlet
 */
@SuppressWarnings("serial")
public class StartWorkflowServlet extends HttpServlet {

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
	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        createTask(request);
        response.sendRedirect("index.jsp");
    }

	private void createTask(HttpServletRequest request) throws IOException,
			ServletException {
		try {
			ServletFileUpload upload = new ServletFileUpload();
		    FileItemIterator iter = upload.getItemIterator(request);
		    
		    while (iter.hasNext()) {
		    	FileItemStream item = iter.next();

		    	if (!item.isFormField()) {
                    InputStream filecontent = item.openStream();
                    BufferedReader br = new BufferedReader(new InputStreamReader(filecontent));
                    
                    StringBuffer data = new StringBuffer();
                    String line = null;
                    while ((line = br.readLine()) != null) {
                    	data.append(line);
                    	data.append('\n');
                    }
                    
                    // create a workflow task
                    Task task = new Task("csv-invoice");
                    task.addParam("csv-input", data.toString());
                    
					ByteArrayOutputStream bos = new ByteArrayOutputStream();
					ObjectOutputStream oos = new ObjectOutputStream(bos);
					oos.writeObject(task);
					
				    TaskOptions to = TaskOptions.Builder.withMethod(TaskOptions.Method.PULL);
					to.payload(bos.toByteArray());
					
					oos.close();
					bos.close();
					
					to.tag(task.getWorker());
				    
					Queue q = QueueFactory.getQueue("pull-queue");
				    q.add(to);
				    
				    System.out.println("Added a task to the queue");
                }
            }
        } catch (FileUploadException e) {
            throw new ServletException("Cannot parse multipart request.", e);
        }
	}
}
