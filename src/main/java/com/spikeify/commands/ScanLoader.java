package com.spikeify.commands;

import com.aerospike.client.*;
import com.aerospike.client.async.IAsyncClient;
import com.aerospike.client.policy.ScanPolicy;
import com.spikeify.*;
import com.spikeify.annotations.Namespace;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * A command chain for getting all records from database.
 *
 * @param <T>
 */
public class ScanLoader<T> {

	public ScanLoader(Class<T> type,
	                  IAsyncClient asynClient,
	                  ClassConstructor classConstructor,
	                  RecordsCache recordsCache,
	                  String namespace) {

		this.asynClient = asynClient;
		this.classConstructor = classConstructor;
		this.recordsCache = recordsCache;
		this.namespace = namespace;
		this.setName = null;

		this.maxRecords = 0;

		this.mapper = MapperService.getMapper(type);
		this.type = type;

		// must be set in order for later queries to return record keys
		this.asynClient.getReadPolicyDefault().sendKey = true;
	}

	protected String namespace;
	protected String setName;

	protected final IAsyncClient asynClient;
	protected final ClassConstructor classConstructor;
	protected final RecordsCache recordsCache;

	protected ScanPolicy overridePolicy;
	protected long maxRecords;

	protected final ClassMapper<T> mapper;
	protected final Class<T> type;
	private AcceptFilter<T> acceptFilter;

	/**
	 * Sets the Namespace. Overrides the default namespace and the namespace defined on the Class via {@link Namespace} annotation.
	 *
	 * @param namespace The namespace.
	 */
	public ScanLoader<T> namespace(String namespace) {

		this.namespace = namespace;
		return this;
	}

	/**
	 * Manually sets the set name. Overrides the default setName resolving mechanics on the Class via {@link com.spikeify.annotations.SetName} annotation.
	 *
	 * @param setName set name.
	 */
	public ScanLoader<T> setName(String setName) {

		this.setName = setName;
		return this;
	}

	/**
	 * Sets the {@link ScanPolicy} to be used when getting the record from the database.
	 *
	 * @param policy The policy.
	 */
	public ScanLoader<T> policy(ScanPolicy policy) {

		this.overridePolicy = policy;
		this.overridePolicy.sendKey = true;
		return this;
	}

	/**
	 * Sets the maximum number of records to be retrieved
	 *
	 * @param max number of records before scan stops execution
	 */
	public ScanLoader<T> maxRecords(long max) {

		this.maxRecords = max;
		return this;
	}

	public ScanLoader<T> filter(AcceptFilter<T> filter) {

		acceptFilter = filter;
		return this;
	}

	protected String getNamespace() {

		String useNamespace = namespace != null ? namespace : mapper.getNamespace();
		if (useNamespace == null) {
			throw new SpikeifyError("Namespace not set.");
		}
		return useNamespace;
	}

	protected String getSetName() {

		return setName == null ? IndexingService.getSetName(type) : setName;
	}

	/**
	 * Synchronously executes multiple get commands.
	 *
	 * @return List of Java objects mapped from records
	 */
	public List<T> now() {

		final List<T> list = Collections.synchronizedList(new ArrayList<T>());

		try {

			ScanPolicy policy = overridePolicy != null ? overridePolicy : copyScanPolicy(asynClient.getScanPolicyDefault());

			asynClient.scanAll(policy, getNamespace(), getSetName(), new ScanCallback() {
				@Override
				public void scanCallback(Key key, Record record) throws AerospikeException {

					T object = classConstructor.construct(type);

					recordsCache.insert(key, record.bins);

					MapperService.map(mapper, key, record, object);

					// set LDT fields
					mapper.setBigDatatypeFields(object, asynClient, key);

					// if filter is given then check if item fits, otherwise simple add
					boolean add = (acceptFilter == null || acceptFilter.accept(object));

					if (add) {
						// save record hash into cache - used later for differential updating

						list.add(object);

						if (maxRecords > 0 && list.size() >= maxRecords) {
							// quit scanning if we have enough
							throw new AerospikeException.ScanTerminated();
						}
					}
				}
			});
		} catch (AerospikeException.ScanTerminated e) {
			// this is not the best way to do this ...
			// check if exception was thrown from ScanLoader ... if not propagate
			if (list.size() < maxRecords) {
				throw e;
			}
		}

		return list;
	}

	private ScanPolicy copyScanPolicy(ScanPolicy original) {
		ScanPolicy copy = new ScanPolicy();
		copy.scanPercent = original.scanPercent;
		copy.maxConcurrentNodes = original.maxConcurrentNodes;
		copy.concurrentNodes = original.concurrentNodes;
		copy.includeBinData = original.includeBinData;
		copy.includeLDT = original.includeLDT;
		copy.failOnClusterChange = original.failOnClusterChange;

		return copy;
	}

	private ScanPolicy getPolicy(){
		ScanPolicy policy = overridePolicy != null ? overridePolicy : copyScanPolicy(asynClient.getScanPolicyDefault());
		// make sure only keys get retrieved
		policy.includeBinData = false;
		policy.includeLDT = false;
		return policy;
	}

	/**
	 * Synchronously executes multiple get commands.
	 *
	 * @return List of record Keys only
	 */
	public List<Value> keys() {

		final List<Value> list = Collections.synchronizedList(new ArrayList<Value>());

		try {

			asynClient.scanAll(getPolicy(), getNamespace(), getSetName(), new ScanCallback() {
				@Override
				public void scanCallback(Key key, Record record) throws AerospikeException {

					list.add(key.userKey);

					if (maxRecords > 0 && list.size() >= maxRecords) {
						// quit scanning if we have enough
						throw new AerospikeException.ScanTerminated();
					}
				}
			});
		} catch (AerospikeException.ScanTerminated e) {
			// this is not the best way to do this ...
			// check if exception was thrown from ScanLoader ... if not propagate
			if (list.size() < maxRecords) {
				throw e;
			}
		}

		return list;
	}

}
