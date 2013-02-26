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
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import com.google.appengine.api.memcache.MemcacheService;
import com.google.appengine.api.memcache.MemcacheServiceFactory;

import drm.taskworker.Worker;
import drm.taskworker.tasks.EndTask;
import drm.taskworker.tasks.Task;
import drm.taskworker.tasks.TaskResult;

/**
 * Collect all files in a workflow and zip them when an end of workflow task
 * is received.
 * 
 * @author Bart Vanbrabant <bart.vanbrabant@cs.kuleuven.be>
 */
public class ZipWorker extends Worker {
	private MemcacheService cacheService = MemcacheServiceFactory.getMemcacheService();

	/**
	 * Creates a new work with the name blob-to-cache
	 */
	public ZipWorker(String workerName) {
		super(workerName);
	}

	/**
	 * Retrieves the filedata and stores it in a list associated with the 
	 * workflow. When the end of workflow token is received the zip file is
	 * created.
	 */
	@SuppressWarnings("unchecked")
	@Override
	public TaskResult work(Task task) {
		TaskResult result = new TaskResult();
		if (!task.hasParam("arg0")) {
			return result.setResult(TaskResult.Result.ARGUMENT_ERROR);
		}

		// get the list of files to put in the zip, if it does not exist yet
		// create it.
		String zipKey = "workflow-files-" + task.getWorkflow().getWorkflowId().toString();
		List<byte[]> zipList = null;
		if (this.cacheService.contains(zipKey)) {
			zipList = (List<byte[]>)this.cacheService.get(zipKey);
		} else {
			zipList = new ArrayList<byte[]>();
		}
		
		// get the file that is sent to this worker
		byte[] fileData = (byte[])this.cacheService.get(task.getParam("arg0"));
		zipList.add(fileData);
		
		// save the list again
		this.cacheService.put(zipKey, zipList);
		
		// delete the file from the cache
		this.cacheService.delete(task.getParam("arg0"));

		result.setResult(TaskResult.Result.SUCCESS);
		return result;
	}

	/**
	 * Handle the end of workflow token by sending it to the same next hop.
	 */
	@SuppressWarnings("unchecked")
	public TaskResult work(EndTask task) {
		logger.info("Building zip file and ending workflow.");
		TaskResult result = new TaskResult();
		
		try {
			// get all zip files
			String zipKey = "workflow-files-" + task.getWorkflow().getWorkflowId().toString();
			List<byte[]> zipList = null;
			if (!this.cacheService.contains(zipKey)) {
				logger.warning("empty zip file");
				zipList = new ArrayList<byte[]>();
			} else {
				zipList = (List<byte[]>)this.cacheService.get(zipKey);
			}
			
			// create the zip stream
			ByteArrayOutputStream boas = new ByteArrayOutputStream();
			ZipOutputStream out = new ZipOutputStream(boas);

			// save the files in the zip
			int i = 0;
			for (byte[] fileData : zipList) {
				out.putNextEntry(new ZipEntry(++i + ".pdf"));
				out.write(fileData);
			}
			out.close();
			boas.flush();
			
			byte[] zipData = boas.toByteArray();
			boas.close();
			
			this.cacheService.put("workflow-" + task.getWorkflow().getWorkflowId().toString(), zipData);
			logger.info("Stored zip file in cache under " + zipKey);
		} catch (FileNotFoundException e) {
			result.setResult(TaskResult.Result.EXCEPTION);
			result.setException(e);
		} catch (IOException e) {
			result.setResult(TaskResult.Result.EXCEPTION);
			result.setException(e);
		}

		return result.setResult(TaskResult.Result.SUCCESS);
	}
}
