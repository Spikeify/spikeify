package com.spikeify.commands;

import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.async.IAsyncClient;
import com.aerospike.client.command.ParticleType;
import com.aerospike.client.policy.BatchPolicy;
import com.spikeify.*;
import com.spikeify.annotations.Namespace;
import com.spikeify.annotations.SetName;

import java.util.ArrayList;
import java.util.List;

/**
 * A command chain for getting a single object from database.
 * This class is not intended to be instantiated by user.
 * @param <T>
 */
public class SingleLoader<T> {

	/**
	 * Used internally to create a command chain. Not intended to be used by the user directly. Use {@link Spikeify#get(Class)} instead.
	 */
	public SingleLoader(Class<T> type, IAerospikeClient synClient, IAsyncClient asyncClient, ClassConstructor classConstructor,
	                    RecordsCache recordsCache, String namespace) {
		this.synClient = synClient;
		this.asyncClient = asyncClient;
		this.classConstructor = classConstructor;
		this.recordsCache = recordsCache;
		this.namespace = namespace;
		this.policy = new BatchPolicy();
		this.policy.sendKey = true;
		this.mapper = MapperService.getMapper(type);
		this.type = type;
	}

	protected String namespace;
	protected String setName;
	protected List<String> stringKeys = new ArrayList<>();
	protected List<Long> longKeys = new ArrayList<>();
	protected List<Key> keys = new ArrayList<>(10);
	protected final IAerospikeClient synClient;
	protected final IAsyncClient asyncClient;
	protected final ClassConstructor classConstructor;
	protected final RecordsCache recordsCache;
	protected BatchPolicy policy;
	protected ClassMapper<T> mapper;
	protected Class<T> type;

	/**
	 * Sets the Namespace. Overrides the default namespace and the namespace defined on the Class via {@link Namespace} annotation.
	 * @param namespace The namespace.
	 * @return
	 */
	public SingleLoader<T> namespace(String namespace) {
		this.namespace = namespace;
		return this;
	}

	/**
	 * Sets the SetName. Overrides any SetName defined on the Class via {@link SetName} annotation.
	 * @param setName The name of the set.
	 * @return
	 */
	public SingleLoader<T> set(String setName) {
		this.setName = setName;
		return this;
	}

	/**
	 * Sets the key of the record to be loaded.
	 * @param key
	 * @return
	 */
	public SingleLoader<T> key(String key) {
		this.stringKeys.add(key);
		return this;
	}

	/**
	 * Sets the user key of the record to be loaded.
	 * @param userKey User key of tye Long.
	 * @return
	 */
	public SingleLoader<T> key(Long userKey) {
		this.longKeys.add(userKey);
		return this;
	}

	/**
	 * Sets the user key of the record to be loaded.
	 * @param userKey User key of tye String.
	 * @return
	 */
	public SingleLoader<T> key(Key userKey) {
		this.keys.add(userKey);
		return this;
	}

	/**
	 * Sets the {@link BatchPolicy} to be used when getting the record from the database.
	 * Internally the 'sendKey' property of the policy will always be set to true.
	 * @param policy The policy.
	 * @return
	 */
	public SingleLoader<T> policy(BatchPolicy policy) {
		this.policy = policy;
		this.policy.sendKey = true;
		return this;
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

		Record record = synClient.get(policy, key);

		if (record == null) {
			return null;
		}

		T object = classConstructor.construct(type);

		// save rew records into cache - used later for differential updating
		recordsCache.insert(key, record.bins);

		// set UserKey field
		switch (key.userKey.getType()) {
			case ParticleType.STRING:
				mapper.setUserKey(object, key.userKey.toString());
				break;
			case ParticleType.INTEGER:
				mapper.setUserKey(object, key.userKey.toLong());
				break;
		}

		// set metafields on the entity: @Namespace, @SetName, @Expiration..
		mapper.setMetaFieldValues(object, key.namespace, key.setName, record.generation, record.expiration);

		// set field values
		mapper.setFieldValues(object, record.bins);

		return object;
	}

}
