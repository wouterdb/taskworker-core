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

package drm.taskworker.workers;

import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.io.StringReader;
import java.util.UUID;

import javax.xml.transform.Templates;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.sax.SAXResult;
import javax.xml.transform.stream.StreamSource;

import org.apache.fop.apps.FOUserAgent;
import org.apache.fop.apps.Fop;
import org.apache.fop.apps.FopFactory;
import org.apache.fop.apps.MimeConstants;

import com.google.appengine.api.memcache.MemcacheService;
import com.google.appengine.api.memcache.MemcacheServiceFactory;

import drm.taskworker.Worker;
import drm.taskworker.tasks.Task;
import drm.taskworker.tasks.TaskResult;

/**
 * A worker that renders an invoice based
 * 
 * @author Bart Vanbrabant <bart.vanbrabant@cs.kuleuven.be>
 */
public class XslFoRenderWorker extends Worker {
	private MemcacheService cacheService = MemcacheServiceFactory.getMemcacheService();

	public XslFoRenderWorker(String workerName) {
		super(workerName);
	}

	@Override
	public TaskResult work(Task task) {
		TaskResult result = new TaskResult();
		if (!task.hasParam("arg0") || !cacheService.contains(task.getParam("arg0"))) {
			return result.setResult(TaskResult.Result.ARGUMENT_ERROR);
		}

		String invoice_source = (String)cacheService.get((String) task.getParam("arg0"));

		try {
			TransformerFactory tFactory = TransformerFactory.newInstance();
			FopFactory fopFactory = FopFactory.newInstance();

			// Load the stylesheet
			Templates templates = tFactory.newTemplates(new StreamSource(
					new StringReader(invoice_source)));

			// Second run (the real thing)
			ByteArrayOutputStream boas = new ByteArrayOutputStream();
			OutputStream out = new java.io.BufferedOutputStream(boas);
			try {
				FOUserAgent foUserAgent = fopFactory.newFOUserAgent();
				foUserAgent.setURIResolver(new ClassPathURIResolver());
				Fop fop = fopFactory.newFop(MimeConstants.MIME_PDF,
						foUserAgent, out);

				Transformer transformer = templates.newTransformer();
				transformer.transform(new StreamSource(new StringReader(
						"<invoice></invoice>")),
						new SAXResult(fop.getDefaultHandler()));

			} finally {
				out.close();
			}

			// store the fileData
			String cacheKey = UUID.randomUUID().toString();
			this.cacheService.put(cacheKey, boas.toByteArray());

			Task newTask = new Task(task, this.getNextWorker());
			newTask.addParam("arg0", cacheKey);
			result.addNextTask(newTask);
			result.setResult(TaskResult.Result.SUCCESS);
		} catch (Exception e) {
			result.setResult(TaskResult.Result.EXCEPTION);
			result.setException(e);
		}
		return result;
	}
}
