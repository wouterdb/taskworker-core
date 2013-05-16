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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import org.jboss.capedwarf.common.app.Application;
import org.jboss.capedwarf.common.infinispan.CacheName;
import org.jboss.capedwarf.common.infinispan.InfinispanUtils;
import org.jboss.capedwarf.tasks.Task;
import org.jboss.capedwarf.tasks.TaskOptionsHelper;
import org.jgroups.util.Triple;

import com.google.appengine.api.socket.SocketServicePb.ReceiveRequest.Flags;
import com.google.appengine.api.taskqueue.InvalidQueueModeException;
import com.google.appengine.api.taskqueue.QueueConstants;
import com.google.appengine.api.taskqueue.RetryOptions;
import com.google.appengine.api.taskqueue.TaskHandle;
import com.google.appengine.api.taskqueue.TaskOptions;
import com.google.common.util.concurrent.ListenableFuture;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.ConsistencyLevel;
import com.netflix.astyanax.model.CqlResult;
import com.netflix.astyanax.model.Row;
import com.netflix.astyanax.query.CqlQuery;
import com.netflix.astyanax.query.PreparedCqlQuery;
import com.netflix.astyanax.recipes.locks.ColumnPrefixDistributedRowLock;

import drm.taskworker.Entities;
import drm.taskworker.Job;
import drm.taskworker.tasks.AbstractTask;
import drm.taskworker.tasks.EndTask;
import drm.util.Triplet;

/**
 * A wrapper around the CapeDwarf queue. A lot of code is copy pasted from CD
 * and then heavily changed.
 * 
 * @author Bart Vanbrabant <bart.vanbrabant@cs.kuleuven.be>
 */
public class Queue {

	private Keyspace cs;
	private UUID lockId;

	public Queue(String queueName) {
		cs = Entities.cs();
		this.lockId = UUID.randomUUID();
	}

	/**
	 * assume saved
	 * 
	 * Add a new task to the queue.
	 */
	public void add(AbstractTask task) {
		try {
			queue(task);

		} catch (ConnectionException e) {
			throw new RuntimeException(e);
		}
	}

	private void queue(AbstractTask task) throws ConnectionException {
		PreparedCqlQuery<String, String> doQueue = cs
				.prepareQuery(Entities.CF_STANDARD1)
				.withCql(
						"INSERT INTO taskqueue (id, workflow_id, worker_name, end) VALUES (?, ?,?,?);")
				.asPreparedStatement();
		doQueue.withUUIDValue(task.getId()).withUUIDValue(task.getWorkflowId())
				.withStringValue(task.getWorker())
				.withBooleanValue(task instanceof EndTask).execute();
	}

	/**
	 * Lease a task from the queue
	 * 
	 * @param lease
	 *            How long is the task leased
	 * @param unit
	 *            The timeunit of the lease time
	 * @param limit
	 *            How many task are to be leased
	 * @param taskType
	 *            The type of task (the name / type of the worker)
	 * @param workflowId
	 *            The workflow the task belongs to
	 * @return A list of taskhandles
	 * @throws ConnectionException
	 */
	public List<Triplet<UUID, UUID, String>> leaseTasks(long lease,
			TimeUnit unit, long limit, String taskType, UUID workflowId)
			throws ConnectionException {
		long leaseMillis = unit.toMillis(lease);
		long now = System.currentTimeMillis();
		long leasetime = now + leaseMillis;

		List<Triplet<UUID, UUID, String>> handles = new LinkedList<Triplet<UUID, UUID, String>>();
		List<Triplet<UUID, UUID, String>> candidates = findTasks(taskType,
				workflowId, limit);

		for (Triplet<UUID, UUID, String> task : candidates) {
			requestlease(task, leasetime);
		}

		// fixme: retry if failed?
		for (Triplet<UUID, UUID, String> task : candidates) {
			if (verifyLease(now, task))
				handles.add(task);
		}

		return handles;
	}

	/**
	 * @param taskType
	 * @param workflowId
	 *            can be null
	 * @param limit
	 * @return
	 * @throws ConnectionException
	 */
	private List<Triplet<UUID, UUID, String>> findTasks(String taskType,
			UUID workflowId, long limit) throws ConnectionException {

		CqlResult<String, String> result;
		if (workflowId == null) {
			PreparedCqlQuery<String, String> findTasksShort = cs
					.prepareQuery(Entities.CF_STANDARD1)
					.withCql(
							"SELECT * FROM taskqueue WHERE worker_name=? ALLOW FILTERING")
					.asPreparedStatement();

			result = findTasksShort.withStringValue(taskType).execute()
					.getResult();
		}

		else {
			PreparedCqlQuery<String, String> findTasks = cs
					.prepareQuery(Entities.CF_STANDARD1)
					.withCql(
							"SELECT * FROM taskqueue WHERE workflow_id=? and worker_name=? ALLOW FILTERING")
					.asPreparedStatement();
			result = findTasks.withUUIDValue(workflowId)
					.withStringValue(taskType).execute().getResult();
		}

		List<Triplet<UUID, UUID, String>> out = new LinkedList<>();

		long now = System.currentTimeMillis();

		Set<UUID> seen = new HashSet<>();
		Map<UUID, Triplet<UUID, UUID, String>> endtasks = new HashMap<UUID, Triplet<UUID, UUID, String>>();

		for (Row<String, String> row : result.getRows()) {
			ColumnList<String> c = row.getColumns();

			UUID workflow = c.getUUIDValue("workflow_id", null);
			boolean end = c.getBooleanValue("end", false);
			
			if(!end) 
				seen.add(workflow);

			long lease = c.getLongValue("lease", Long.valueOf(Long.MAX_VALUE));
			if (lease <= now)
				continue;

			Triplet<UUID, UUID, String> record = new Triplet<UUID, UUID, String>(
					workflow, c.getUUIDValue("id", null), taskType);

			if (end) {
				endtasks.put(workflow, record);
			} else {
				out.add(record);
			}
		}

		for (Entry<UUID, Triplet<UUID, UUID, String>> end : endtasks.entrySet()) {
			if (!seen.contains(end.getKey()))
				out.add(0, end.getValue());
		}
		return out.subList(0, Math.min((int) limit,out.size()));
	}

	private boolean verifyLease(long now, Triplet<UUID, UUID, String> task)
			throws ConnectionException {

		PreparedCqlQuery<String, String> verifylease = cs
				.prepareQuery(Entities.CF_STANDARD1)
				.setConsistencyLevel(ConsistencyLevel.CL_QUORUM)
				.withCql(
						"SELECT * FROM taskqueue WHERE workflow_id=? and id=? and worker_name=?")
				.asPreparedStatement();

		CqlResult<String, String> result = verifylease
				.withUUIDValue(task.getA()).withUUIDValue(task.getB())
				.withStringValue(task.getC()).execute().getResult();
		if (result.getRows().size() > 1)
			throw new IllegalStateException(
					"multi lock records, figure it out sucker");
		for (Row<String, String> row : result.getRows()) {
			ColumnList<String> c = row.getColumns();

			if (lockId.equals(c.getUUIDValue("lockid", null)))
				return true;

		}
		return false;
	}

	private void requestlease(Triplet<UUID, UUID, String> task, long leasetime)
			throws ConnectionException {
		PreparedCqlQuery<String, String> requestlease = cs
				.prepareQuery(Entities.CF_STANDARD1)
				.setConsistencyLevel(ConsistencyLevel.CL_QUORUM)
				.withCql(
						"update taskqueue set lease=?, lockid=? where workflow_id=? and id=? and worker_name=?;")
				.asPreparedStatement();
		requestlease.withLongValue(leasetime).withUUIDValue(lockId)
				.withUUIDValue(task.getA()).withUUIDValue(task.getB())
				.withStringValue(task.getC()).execute();
	}

	public void deleteTask(AbstractTask task) throws ConnectionException {
		PreparedCqlQuery<String, String> delete = cs
				.prepareQuery(Entities.CF_STANDARD1)
				.setConsistencyLevel(ConsistencyLevel.CL_QUORUM)
				.withCql(
						"DELETE FROM taskqueue WHERE workflow_id=? and id=? and worker_name=?")
				.asPreparedStatement();
		delete.withUUIDValue(task.getWorkflowId()).withUUIDValue(task.getId())
				.withStringValue(task.getWorker()).execute();
	}

	/*
	 * @SuppressWarnings({ "unchecked", "rawtypes" }) public List<WorkflowTask>
	 * dumpTasks() {
	 * 
	 * QueryBuilder builder = searchManager.buildQueryBuilderForClass(
	 * WorkflowTask.class).get();
	 * 
	 * Query luceneQuery = builder.bool() .must(toTerm(builder, Task.QUEUE,
	 * queueName).createQuery()) //
	 * .must(builder.range().onField(Task.LEASED_UNTIL
	 * ).below(System.currentTimeMillis()).createQuery()) .createQuery();
	 * 
	 * // query the cache CacheQuery query = searchManager.getQuery(luceneQuery,
	 * WorkflowTask.class);
	 * 
	 * return (List<WorkflowTask>) (List) query.list(); }
	 * 
	 * @SuppressWarnings({ "unchecked", "rawtypes" }) private List<Task>
	 * findTasks(String taskType, String workflowId, long countLimit) {
	 * 
	 * QueryBuilder builder = searchManager.buildQueryBuilderForClass(
	 * WorkflowTask.class).get();
	 * 
	 * Query luceneQuery = builder.bool() .must(toTerm(builder, Task.QUEUE,
	 * queueName).createQuery()) //
	 * .must(builder.range().onField(Task.LEASED_UNTIL
	 * ).below(System.currentTimeMillis()).createQuery()) .createQuery();
	 * 
	 * // the type of task if (taskType != null) { Query tagQuery =
	 * toTerm(builder, Task.TAG, taskType).createQuery(); luceneQuery =
	 * builder.bool().must(luceneQuery).must(tagQuery) .createQuery(); }
	 * 
	 * // constrain to the given workflowId if (workflowId != null) { Query
	 * workflowQuery = toTerm(builder, WorkflowTask.WORKFLOW,
	 * workflowId).createQuery(); luceneQuery =
	 * builder.bool().must(luceneQuery).must(workflowQuery) .createQuery(); }
	 * 
	 * // query the cache CacheQuery query = searchManager
	 * .getQuery(luceneQuery, WorkflowTask.class) .maxResults((int)
	 * countLimit).sort(SORT);
	 * 
	 * // noinspection unchecked List<Task> out = new LinkedList<>(); long now =
	 * System.currentTimeMillis(); Task end = null; boolean leases = false; for
	 * (WorkflowTask t : (List<WorkflowTask>) (List) query.list()) { if
	 * (t.getType().equals("end")) end = t; else { if (t.getLeasedUntil() < now)
	 * { if (failcache.containsKey(t.getName()))
	 * Logger.getLogger(getClass().getName()).severe( "bad object in cache");
	 * else out.add(t); }
	 * 
	 * else leases = true; }
	 * 
	 * }
	 * 
	 * if (!out.isEmpty()) return out; if (!leases && end != null) return
	 * Collections.singletonList(end); return Collections.EMPTY_LIST; }
	 */

	/*
	 * static TermTermination toTerm(QueryBuilder builder, String field, Object
	 * value) { return builder.keyword().onField(field).ignoreAnalyzer()
	 * .ignoreFieldBridge().matching(value); }
	 * 
	 * static void validateTaskName(String taskName) { if (taskName == null ||
	 * taskName.length() == 0 ||
	 * QueueConstants.TASK_NAME_PATTERN.matcher(taskName).matches() == false) {
	 * throw new IllegalArgumentException(
	 * "Task name does not match expression " + QueueConstants.TASK_NAME_REGEX +
	 * "; given taskname: '" + taskName + "'"); } }
	 * 
	 * public String getName() { return queueName; }
	 */
}
