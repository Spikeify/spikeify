package com.spikeify.commands;

import com.aerospike.client.Bin;
import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.async.IAsyncClient;
import com.aerospike.client.policy.RecordExistsAction;
import com.aerospike.client.policy.WritePolicy;
import com.spikeify.*;

import java.util.Arrays;
import java.util.Map;
import java.util.Set;

public class SingleKeyUpdater<T, K> {

	public SingleKeyUpdater(IAerospikeClient synClient, IAsyncClient asyncClient,
	                        RecordsCache recordsCache, boolean create, String defaultNamespace, T object, K key) {
		this.synClient = synClient;
		this.asyncClient = asyncClient;
		this.recordsCache = recordsCache;
		this.create = create;
		this.namespace = defaultNamespace;
		this.object = object;
		this.policy = new WritePolicy();
		this.policy.sendKey = true;
		this.mapper = MapperService.getMapper((Class<T>)object.getClass());
		if(key.getClass().equals(Key.class)){
			this.key = (Key) key;
			this.keyType = KeyType.KEY;
		} else if(key.getClass().equals(Long.class)){
			this.longKey = (Long) key;
			this.keyType = KeyType.LONG;
		} else if(key.getClass().equals(String.class)){
			this.stringKey = (String) key;
			this.keyType = KeyType.STRING;
		} else {
			throw new IllegalArgumentException("Error: unsupported key type. " +
					"SingleKeyUpdater constructor can only ba called wit K as : Key, Long or String");
		}
	}

	private T object;
	protected KeyType keyType;
	protected String namespace;
	protected String setName;
	protected String stringKey;
	protected Long longKey;
	protected Key key;
	protected IAerospikeClient synClient;
	protected IAsyncClient asyncClient;
	protected RecordsCache recordsCache;
	protected final boolean create;
	protected WritePolicy policy;
	protected ClassMapper<T> mapper;

	public SingleKeyUpdater<T, K> namespace(String namespace) {
		this.namespace = namespace;
		return this;
	}

	public SingleKeyUpdater<T, K> set(String setName) {
		this.setName = setName;
		return this;
	}

	public SingleKeyUpdater<T, K> policy(WritePolicy policy) {
		this.policy = policy;
		this.policy.sendKey = true;
		if (create) {
			this.policy.recordExistsAction = RecordExistsAction.CREATE_ONLY;
		} else {
			this.policy.recordExistsAction = RecordExistsAction.UPDATE_ONLY;
		}
		return this;
	}

	protected void collectKeys() {

		// check if any Long or String keys were provided
		if (stringKey != null) {
			key = new Key(getNamespace(), getSetName(), stringKey);
		} else if (longKey != null) {
			key = new Key(getNamespace(), getSetName(), longKey);
		}
	}

	protected String getNamespace() {
		if (namespace == null) {
			throw new SpikeifyError("Namespace not set.");
		}
		return namespace;
	}

	protected String getSetName() {
		return setName != null ? setName : mapper.getSetName();
	}

	public K now() {

		if (create) {
			policy.recordExistsAction = RecordExistsAction.CREATE_ONLY;
		} else {
			policy.recordExistsAction = RecordExistsAction.UPDATE;
		}

		collectKeys();

		if (object == null) {
			throw new SpikeifyError("Error: parameter 'objects' must not be null or empty array");
		}

		Map<String, Object> props = mapper.getProperties(object);
		Set<String> changedProps = recordsCache.update(key, props);

		Bin[] bins = new Bin[changedProps.size()];
		int position = 0;
		for (String propName : changedProps) {
			bins[position++] = new Bin(propName, props.get(propName));
		}

		Long expiration = mapper.getExpiration(object);
		if (expiration != null) {
			// Entities expiration:  Java time in milliseconds
			// Aerospike expiration: seconds from 1.1.2010 = 1262304000s.
			policy.expiration = (int) (expiration / 1000) - 1262304000;
		}

		synClient.put(policy, key, bins);

		switch (keyType){
			case KEY:
				return  (K) key;
			case LONG:
				Long longKey = key.userKey.toLong();
				return (K) longKey;
			case STRING:
				return (K) key.userKey.toString();
			default:
				throw new IllegalStateException("Error: unsupported ket type. Must be one of: Key, Long or String"); // should not happen
		}
	}
}
