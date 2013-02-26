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

import com.google.appengine.api.blobstore.BlobKey;
import com.google.appengine.api.blobstore.BlobstoreService;
import com.google.appengine.api.blobstore.BlobstoreServiceFactory;
import com.google.appengine.api.memcache.MemcacheService;
import com.google.appengine.api.memcache.MemcacheServiceFactory;

import drm.taskworker.EndTask;
import drm.taskworker.Task;
import drm.taskworker.TaskResult;
import drm.taskworker.Worker;

/**
 * Retrieves a blob from the blobstore and puts it in the cache service.
 * 
 * @author Bart Vanbrabant <bart.vanbrabant@cs.kuleuven.be>
 */
public class BlobWorker extends Worker {
	public static final String NEXT_TASK = "csv-invoice";
	public static final String WORKER_NAME = "blob-to-cache";

	private MemcacheService cacheService = MemcacheServiceFactory
			.getMemcacheService();
	private BlobstoreService blobstoreService = BlobstoreServiceFactory
			.getBlobstoreService();

	/**
	 * Creates a new work with the name blob-to-cache
	 */
	public BlobWorker() {
		super(WORKER_NAME);
	}

	/**
	 * Retrieves the blob from the blobstore and puts it in the memcache service
	 * with the same key as the blob.
	 * 
	 * @in blob:BlobKey The info of the blob to put into the cache
	 * 
	 * @out key:String The key used to save the blob to the cache service
	 * 
	 * @next "csv-invoice"
	 */
	@Override
	public TaskResult work(Task task) {
		TaskResult result = new TaskResult();
		if (!task.hasParam("blob")) {
			return result.setResult(TaskResult.Result.ARGUMENT_ERROR);
		}

		BlobKey blobKey = (BlobKey) task.getParam("blob");
		if (blobKey == null) {
			return result.setResult(TaskResult.Result.ARGUMENT_ERROR);
		}
		
		String fileContent = new String(blobstoreService.fetchData(blobKey, 0,
				BlobstoreService.MAX_BLOB_FETCH_SIZE - 1));

		if (!cacheService.contains(blobKey.getKeyString())) {
			logger.info("Putting blob into cache with key "
					+ blobKey.getKeyString());
			cacheService.put(blobKey.getKeyString(), fileContent);
		} else {
			// this really should not happen that a key is duplicate
			return result.setResult(TaskResult.Result.ERROR);
		}

		Task newTask = new Task(NEXT_TASK);
		newTask.addParam("cacheKey", blobKey.getKeyString());

		result.addNextTask(newTask);
		result.setResult(TaskResult.Result.SUCCESS);

		return result;
	}

	/**
	 * Handle the end of workflow token by sending it to the same next hop.
	 */
	public TaskResult work(EndTask task) {
		TaskResult result = new TaskResult();
		result.addNextTask(new EndTask(NEXT_TASK));

		return result.setResult(TaskResult.Result.SUCCESS);
	}

}
