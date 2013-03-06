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

package drm.taskworker.queue;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.hibernate.search.query.dsl.QueryBuilder;
import org.hibernate.search.query.dsl.TermTermination;
import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.query.CacheQuery;
import org.infinispan.query.Search;
import org.infinispan.query.SearchManager;
import org.jboss.capedwarf.common.app.Application;
import org.jboss.capedwarf.common.infinispan.CacheName;
import org.jboss.capedwarf.common.infinispan.InfinispanUtils;
import org.jboss.capedwarf.tasks.Task;
import org.jboss.capedwarf.tasks.TaskOptionsHelper;

import com.google.appengine.api.taskqueue.InvalidQueueModeException;
import com.google.appengine.api.taskqueue.QueueConstants;
import com.google.appengine.api.taskqueue.QueueFactory;
import com.google.appengine.api.taskqueue.RetryOptions;
import com.google.appengine.api.taskqueue.TaskHandle;
import com.google.appengine.api.taskqueue.TaskOptions;

public class Queue {
    private static final Sort SORT = new Sort(new SortField("eta", SortField.LONG));
	
	private String queueName;
	private AdvancedCache<String, Object> tasks;
	private SearchManager searchManager;
	
	public Queue(String queueName) {
		this.queueName = queueName;
        this.tasks = getCache().getAdvancedCache().with(Application.getAppClassloader());
        this.searchManager = Search.getSearchManager(tasks);
	}
	
	/**
	 * Get an instance to the queue
	 */
	com.google.appengine.api.taskqueue.Queue getGAEQueue() {
		return QueueFactory.getQueue(this.queueName);
	}
	
    private Cache<String, Object> getCache() {
        return InfinispanUtils.getCache(Application.getAppId(), CacheName.TASKS);
    }
    
    public TaskHandle add(TaskOptions taskOptions) {
        TaskOptionsHelper helper = new TaskOptionsHelper(taskOptions);
        TaskOptions.Method method = helper.getMethod();
        if (method != TaskOptions.Method.PULL) {
            throw new InvalidQueueModeException("Target queue mode does not support this operation");
        }
        
        TaskOptionsHelper options = new TaskOptionsHelper(taskOptions);
        try {
        	TaskOptions copy = new TaskOptions(options.getTaskOptions());
			String taskName = options.getTaskName();
			if (taskName == null) {
			    taskName = UUID.randomUUID().toString(); // TODO -- unique enough?
			    copy.taskName(taskName);
			}
			Long lifespan = options.getEtaMillis();
			RetryOptions retryOptions = options.getRetryOptions();
			Task task = new Task(taskName, this.queueName, copy.getTag(), lifespan, copy, retryOptions);
			storeTask(task);
			TaskHandle handle = new TaskHandle(copy, this.queueName);
        	return handle;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    
    private void storeTask(Task task) {
    	tasks.put(task.getName(), task);
    }
    
    public List<TaskHandle> leaseTasksByTag(long lease, TimeUnit unit, long countLimit, String tag) {
    	long leaseMillis = unit.toMillis(lease);
        long now = System.currentTimeMillis();
    	
        List<TaskHandle> handles = new ArrayList<TaskHandle>();
        for (Task task : findTasks(tag, countLimit)) {
            task.setLastLeaseTimestamp(now);
            task.setLeasedUntil(now + leaseMillis);
            storeTask(task);
            handles.add(new TaskHandle(task.getOptions(), queueName));
        }
        return handles;
    }
    
    static TermTermination toTerm(QueryBuilder builder, String field, Object value) {
        return builder.keyword().onField(field).ignoreAnalyzer().ignoreFieldBridge().matching(value);
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
	private List<Task> findTasks(String tag, long countLimit) {
        QueryBuilder builder = searchManager.buildQueryBuilderForClass(Task.class).get();

        Query luceneQuery = builder.bool()
            .must(toTerm(builder, Task.QUEUE, queueName).createQuery())
            .must(builder.range().onField(Task.LEASED_UNTIL).below(System.currentTimeMillis()).createQuery())
            .createQuery();

        if (tag != null) {
            Query tagQuery = toTerm(builder, Task.TAG, tag).createQuery();
            luceneQuery = builder.bool().must(luceneQuery).must(tagQuery).createQuery();
        }

        CacheQuery query = searchManager.getQuery(luceneQuery, Task.class)
            .maxResults((int)countLimit).sort(SORT);

        //noinspection unchecked
        return (List<Task>) (List)query.list();
    }

    public boolean deleteTask(String taskName) {
        validateTaskName(taskName);
        return tasks.remove(taskName) != null;
    }

    public boolean deleteTask(TaskHandle taskHandle) {
        return deleteTask(taskHandle.getName());
    }
    
    static void validateTaskName(String taskName) {
        if (taskName == null || taskName.length() == 0 || QueueConstants.TASK_NAME_PATTERN.matcher(taskName).matches() == false) {
            throw new IllegalArgumentException("Task name does not match expression " + QueueConstants.TASK_NAME_REGEX + "; given taskname: '" + taskName + "'");
        }
    }
}
