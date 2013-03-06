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
import java.util.logging.Logger;

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

import drm.taskworker.tasks.AbstractTask;

/**
 * A wrapper around the CapeDwarf queue. A lot of code is copy pasted and 
 * adapted from CapeDwarf. 
 *
 * @author Bart Vanbrabant <bart.vanbrabant@cs.kuleuven.be>
 */
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
	
    private Cache<String, Object> getCache() {
        return InfinispanUtils.getCache(Application.getAppId(), CacheName.TASKS);
    }
    
    /**
     * Add a new task to the queue.
     */
    public TaskHandle add(AbstractTask task) {
    	TaskOptions taskOptions = null;
    	try {
    		taskOptions = task.toTaskOption();
    	} catch (Exception e) {
            throw new RuntimeException(e);
        }
    	
        TaskOptionsHelper helper = new TaskOptionsHelper(taskOptions);
        TaskOptions.Method method = helper.getMethod();
        if (method != TaskOptions.Method.PULL) {
            throw new InvalidQueueModeException("Target queue mode does not support this operation");
        }
        
        try {
        	TaskOptions copy = new TaskOptions(helper.getTaskOptions());
			String taskName = helper.getTaskName();
			if (taskName == null) {
			    taskName = UUID.randomUUID().toString(); // TODO -- unique enough?
			    copy.taskName(taskName);
			}
			Long lifespan = helper.getEtaMillis();
			RetryOptions retryOptions = helper.getRetryOptions();
			
			WorkflowTask queueTask = new WorkflowTask(taskName, this.queueName, 
					copy.getTag(), lifespan, copy, retryOptions);
			queueTask.setWorkflowID(task.getWorkflowId().toString());
			queueTask.setType(task.getTaskType());
			
			storeTask(queueTask);

			return new TaskHandle(copy, this.queueName);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    
    private void storeTask(Task task) {
    	tasks.put(task.getName(), task);
    }

    /**
     * Lease a task from the queue
     * 
     * @param lease How long is the task leased
     * @param unit The timeunit of the lease time
     * @param limit How many task are to be leased
     * @param taskType The type of task (the name / type of the worker)
     * @param workflowId The workflow the task belongs to
     * @return A list of taskhandles
     */
    public List<TaskHandle> leaseTasks(long lease, TimeUnit unit, long limit, String taskType, String workflowId) {
    	long leaseMillis = unit.toMillis(lease);
        long now = System.currentTimeMillis();
        List<TaskHandle> handles = new ArrayList<TaskHandle>();

        for (Task task : findTasks(taskType, workflowId, limit)) {
            task.setLastLeaseTimestamp(now);
            task.setLeasedUntil(now + leaseMillis);
            storeTask(task);

            handles.add(new TaskHandle(task.getOptions(), queueName));
        }
        return handles;
    }
    
    @SuppressWarnings({ "unchecked", "rawtypes" })
	private List<Task> findTasks(String taskType, String workflowId, long countLimit) {
        QueryBuilder builder = searchManager.buildQueryBuilderForClass(Task.class).get();

        Query luceneQuery = builder.bool()
            .must(toTerm(builder, Task.QUEUE, queueName).createQuery())
            .must(builder.range().onField(Task.LEASED_UNTIL).below(System.currentTimeMillis()).createQuery())
            .createQuery();
        
        // the type of task
        if (taskType != null) {
        	Query tagQuery = toTerm(builder, Task.TAG, taskType).createQuery();
        	luceneQuery = builder.bool().must(luceneQuery).must(tagQuery).createQuery();
        }
        
        // constrain to the given workflowId
        if (workflowId != null) {
        	Query workflowQuery = toTerm(builder, WorkflowTask.WORKFLOW, taskType).createQuery();
        	luceneQuery = builder.bool().must(luceneQuery).must(workflowQuery).createQuery();
        }

        // query the cache
        CacheQuery query = searchManager.getQuery(luceneQuery, Task.class)
            .maxResults((int)countLimit).sort(SORT);

        //noinspection unchecked
        return (List<Task>) (List)query.list();
    }

    public boolean deleteTask(TaskHandle taskHandle) {
        String taskName = taskHandle.getName();
        validateTaskName(taskName);
        return tasks.remove(taskName) != null;
    }

    static TermTermination toTerm(QueryBuilder builder, String field, Object value) {
        return builder.keyword().onField(field).ignoreAnalyzer().ignoreFieldBridge().matching(value);
    }
    
    static void validateTaskName(String taskName) {
        if (taskName == null || taskName.length() == 0 || QueueConstants.TASK_NAME_PATTERN.matcher(taskName).matches() == false) {
            throw new IllegalArgumentException("Task name does not match expression " + QueueConstants.TASK_NAME_REGEX + "; given taskname: '" + taskName + "'");
        }
    }
}
