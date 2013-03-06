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

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import com.google.appengine.api.memcache.MemcacheService;
import com.google.appengine.api.memcache.MemcacheServiceFactory;

import drm.taskworker.Worker;
import drm.taskworker.tasks.EndTask;
import drm.taskworker.tasks.Task;
import drm.taskworker.tasks.TaskResult;

/**
 * This worker parses html pages and creates new fetch tasks for the urls in the
 * pages if they have not been fetched yet. (Stored in memcache)
 * 
 * @author Bart Vanbrabant <bart.vanbrabant@cs.kuleuven.be>
 */
public class ExtractUrlWorker extends Worker {
	private MemcacheService cacheService = MemcacheServiceFactory.getMemcacheService();

	/**
	 * @param name
	 */
	public ExtractUrlWorker(String name) {
		super(name);
	}

	/* (non-Javadoc)
	 * @see drm.taskworker.Worker#work(drm.taskworker.tasks.Task)
	 */
	@Override
	public TaskResult work(Task task) {
		TaskResult result = new TaskResult();
		if (!task.hasParam("arg0")) {
			return result.setResult(TaskResult.Result.ARGUMENT_ERROR);
		}
		
		String html = (String)task.getParam("arg0");

		System.out.println(html);
		Document doc = Jsoup.parse(html);
		Elements links = doc.select("a[href]"); // a with href

		for (int i = 0; i < links.size(); i++) {
			Element el = links.get(i);
			
			String href = el.attr("href");
			System.err.println(href);
			if (href.startsWith("http://") && !this.cacheService.contains(href)) {
				Task newTask = new Task(task, this.getNextWorker());
				newTask.addParam("arg0", href);
		        result.addNextTask(newTask);
			}
		}
		
        result.setResult(TaskResult.Result.SUCCESS);
		return result;
	}
	
	@Override
	public TaskResult work(EndTask task) {
		// dont stop  and fork:)
		TaskResult result = new TaskResult();
		result.setResult(TaskResult.Result.SUCCESS);
		return result;
	}

}
