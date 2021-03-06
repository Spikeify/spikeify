package com.spikeify.commands;

import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.async.IAsyncClient;
import com.aerospike.client.policy.Policy;
import com.spikeify.*;
import com.spikeify.annotations.Namespace;
import com.spikeify.annotations.SetName;

import java.util.ArrayList;
import java.util.List;

/**
 * A command chain for getting a single object from database.
 * This class is not intended to be instantiated by user.
 *
 * @param <T>
 */
@SuppressWarnings("WeakerAccess")
public class SingleLoader<T> {

	/**
	 * Used internally to create a command chain. Not intended to be used by the user directly. Use {@link Spikeify#get(Class)} instead.
	 */
	public SingleLoader(Class<T> type,
	                    IAsyncClient asynClient,
	                    ClassConstructor classConstructor,
	                    RecordsCache recordsCache,
	                    String namespace) {
		this.asynClient = asynClient;
		this.classConstructor = classConstructor;
		this.recordsCache = recordsCache;
		this.namespace = namespace;
		this.mapper = MapperService.getMapper(type);
		this.type = type;
	}

	protected String namespace;
	protected String setName;
	protected final List<String> stringKeys = new ArrayList<>();
	protected final List<Long> longKeys = new ArrayList<>();
	protected final List<Key> keys = new ArrayList<>(10);
	protected final IAsyncClient asynClient;
	protected final ClassConstructor classConstructor;
	protected final RecordsCache recordsCache;
	protected final ClassMapper<T> mapper;
	protected final Class<T> type;
	protected Policy overridePolicy;

	/**
	 * Sets the Namespace. Overrides the default namespace and the namespace defined on the Class via {@link Namespace} annotation.
	 *
	 * @param namespace The namespace.
	 */
	public SingleLoader<T> namespace(String namespace) {
		this.namespace = namespace;
		return this;
	}

	/**
	 * Sets the SetName. Overrides any SetName defined on the Class via {@link SetName} annotation.
	 *
	 * @param setName The name of the set.
	 */
	public SingleLoader<T> setName(String setName) {
		this.setName = setName;
		return this;
	}

	/**
	 * Sets the key of the record to be loaded.
	 *
	 * @param userKey A user key of the record to loaded.
	 */
	public SingleLoader<T> key(String userKey) {
		this.stringKeys.add(userKey);
		return this;
	}

	/**
	 * Sets the user key of the record to be loaded.
	 *
	 * @param userKey User key of tye Long.
	 */
	public SingleLoader<T> key(Long userKey) {
		this.longKeys.add(userKey);
		return this;
	}

	/**
	 * Sets the user key of the record to be loaded.
	 *
	 * @param userKey User key of tye String.
	 */
	public SingleLoader<T> key(Key userKey) {
		this.keys.add(userKey);
		return this;
	}

	/**
	 * Sets the {@link Policy} to be used when getting the record from the database.
	 * Internally the 'sendKey' property of the policy will always be set to true.
	 *
	 * @param policy The policy.
	 */
	public SingleLoader<T> policy(Policy policy) {
		this.overridePolicy = policy;
		this.overridePolicy.sendKey = true;
		return this;
	}

	private Policy getPolicy(){
		return  overridePolicy != null ? overridePolicy : new Policy(asynClient.getReadPolicyDefault());
	}

	protected void collectKeys() {

		if (!stringKeys.isEmpty()) {
			for (String stringKey : stringKeys) {
				keys.add(new Key(getNamespace(), getSetName(), stringKey));
			}
		} else if (!longKeys.isEmpty()) {
			for (long longKey : longKeys) {
				keys.add(new Key(getNamespace(), getSetName(), longKey));
			}
		} else if (keys.isEmpty()) {
			throw new SpikeifyError("Error: missing parameter 'key'");
		}
	}

	protected String getNamespace() {
		String useNamespace = namespace != null ? namespace : mapper.getNamespace();
		if (useNamespace == null) {
			throw new SpikeifyError("Namespace not set.");
		}
		return useNamespace;
	}

	protected String getSetName() {
		return setName != null ? setName : mapper.getSetName();
	}

	/**
	 * Synchronously executes a single get command and returns the java object.
	 *
	 * @return The Java object mapped from record
	 */
	public T now() {

		collectKeys();

		// this should be a one-key operation
		// if multiple keys - use the first key
		Key key = keys.get(0);

		Record record = asynClient.get(getPolicy(), key);

		if (record == null) {
			return null;
		}

		T object = classConstructor.construct(type);

		// save raw records into cache - used later for differential updating
		recordsCache.insert(key, record.bins);

		MapperService.map(mapper, key, record, object);

		// set LDT fields
		mapper.setBigDatatypeFields(object, asynClient, key);

		return object;
	}
}
