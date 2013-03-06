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

import drm.taskworker.Worker;
import drm.taskworker.tasks.Task;
import drm.taskworker.tasks.TaskResult;

/**
 * Retrieves a blob from the blobstore and puts it in the cache service.
 * 
 * @author Bart Vanbrabant <bart.vanbrabant@cs.kuleuven.be>
 */
public class BlobWorker extends Worker {
	private MemcacheService cacheService = MemcacheServiceFactory
			.getMemcacheService();
	private BlobstoreService blobstoreService = BlobstoreServiceFactory
			.getBlobstoreService();

	/**
	 * Creates a new work with the name blob-to-cache
	 */
	public BlobWorker(String workerName) {
		super(workerName);
	}

	/**
	 * Retrieves the blob from the blobstore and puts it in the memcache service
	 * with the same key as the blob.
	 * 
	 * @in arg0 The info of the blob to put into the cache
	 * 
	 * @out key:String The key used to save the blob to the cache service
	 * 
	 * @next "csv-invoice"
	 */
	@Override
	public TaskResult work(Task task) {
		TaskResult result = new TaskResult();
		if (!task.hasParam("arg0")) {
			logger.warning("Worker requires arg0 argument");
			return result.setResult(TaskResult.Result.ARGUMENT_ERROR);
		}

		BlobKey blobKey = (BlobKey) task.getParam("arg0");
		if (blobKey == null) {
			logger.warning("The argument arg0 should have a value");
			return result.setResult(TaskResult.Result.ARGUMENT_ERROR);
		}
		
		String fileContent = new String(blobstoreService.fetchData(blobKey, 0,
				BlobstoreService.MAX_BLOB_FETCH_SIZE - 1));

		if (!cacheService.contains(blobKey.getKeyString())) {
			logger.info("Putting blob into cache with key " + blobKey.getKeyString());
			cacheService.put(blobKey.getKeyString(), fileContent);
		} else {
			// this really should not happen that a key is duplicate
			return result.setResult(TaskResult.Result.ERROR);
		}

		Task newTask = new Task(task, this.getNextWorker());
		newTask.addParam("arg0", blobKey.getKeyString());

		result.addNextTask(newTask);
		result.setResult(TaskResult.Result.SUCCESS);

		return result;
	}

}
