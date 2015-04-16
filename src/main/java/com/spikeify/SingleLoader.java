package com.spikeify;

import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.async.IAsyncClient;
import com.aerospike.client.command.ParticleType;
import com.aerospike.client.policy.BatchPolicy;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class SingleLoader<T> {

	public SingleLoader(Class<T> type, IAerospikeClient synClient, IAsyncClient asyncClient, ClassConstructor classConstructor, RecordsCache recordsCache) {
		this.synClient = synClient;
		this.asyncClient = asyncClient;
		this.classConstructor = classConstructor;
		this.recordsCache = recordsCache;
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

	public SingleLoader<T> namespace(String namespace) {
		this.namespace = namespace;
		return this;
	}

	public SingleLoader<T> set(String setName) {
		this.setName = setName;
		return this;
	}

	public SingleLoader<T> key(String key) {
		this.stringKeys.add(key);
		return this;
	}

	public SingleLoader<T> key(Long key) {
		this.longKeys.add(key);
		return this;
	}

	public SingleLoader<T> key(Key key) {
		this.keys.add(key);
		return this;
	}

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
			throw new IllegalStateException("Error: missing parameter 'key'");
		}
	}

	protected String getNamespace() {
		String useNamespace = namespace != null ? namespace : mapper.getNamespace();
		if (useNamespace == null) {
			throw new IllegalStateException("Namespace not set.");
		}
		return useNamespace;
	}

	protected String getSetName() {
		return setName != null ? setName : mapper.getSetName();
	}

	/**
	 * Executes a single get command.
	 *
	 * @return The Java object mapped from record
	 */
	public T now() {

		collectKeys();

		// this should be a one-key operation
		// if multiple keys - use the first key
		Key key = keys.get(0);

		Record record = synClient.get(policy, key);
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
