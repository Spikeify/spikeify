package com.spikeify;

import com.aerospike.client.Bin;
import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.async.IAsyncClient;
import com.aerospike.client.policy.RecordExistsAction;
import com.aerospike.client.policy.WritePolicy;

import java.util.*;

public class SingleUpdater<T> {

	private final T object;

	public SingleUpdater(Class type, IAerospikeClient synClient, IAsyncClient asyncClient,
	                     RecordsCache recordsCache, boolean create, T object) {
		this.synClient = synClient;
		this.asyncClient = asyncClient;
		this.recordsCache = recordsCache;
		this.create = create;
		this.policy = new WritePolicy();
		this.policy.sendKey = true;
		this.mapper = MapperService.getMapper(type);
		this.object = object;
	}

	protected String namespace;
	protected String setName;
	protected List<String> stringKeys = new ArrayList<>();
	protected List<Long> longKeys = new ArrayList<>();
	protected List<Key> keys = new ArrayList<>(10);
	protected IAerospikeClient synClient;
	protected IAsyncClient asyncClient;
	protected RecordsCache recordsCache;
	protected final boolean create;
	protected WritePolicy policy;
	protected ClassMapper<T> mapper;

	public SingleUpdater<T> namespace(String namespace) {
		this.namespace = namespace;
		return this;
	}

	public SingleUpdater<T> set(String setName) {
		this.setName = setName;
		return this;
	}

	public SingleUpdater<T> key(String key) {
		this.stringKeys.add(key);
		return this;
	}

	public SingleUpdater<T> key(Long key) {
		this.longKeys.add(key);
		return this;
	}

	public SingleUpdater<T> key(Key key) {
		this.keys.add(key);
		return this;
	}

	public SingleUpdater<T> policy(WritePolicy policy) {
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

	public Key now() {

		collectKeys();

		// this should be a one-key operation
		// if multiple keys - use the first key
		Key key = keys.get(0);

		if (object == null) {
			throw new IllegalStateException("Error: parameter 'objects' must not be null or empty array");
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

		return key;
	}
}
