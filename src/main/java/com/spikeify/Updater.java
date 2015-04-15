package com.spikeify;

import com.aerospike.client.Bin;
import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.async.IAsyncClient;
import com.aerospike.client.policy.RecordExistsAction;
import com.aerospike.client.policy.WritePolicy;

import java.util.*;

public class Updater<T> {

	private final T[] objects;

	public Updater(Class type, IAerospikeClient synClient, IAsyncClient asyncClient,
	               RecordsCache recordsCache, boolean create, T... objects) {
		this.synClient = synClient;
		this.asyncClient = asyncClient;
		this.recordsCache = recordsCache;
		this.create = create;
		this.policy = new WritePolicy();
		this.policy.sendKey = true;
		this.mapper = MapperService.getMapper(type);
		this.objects = objects;
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

	public Updater<T> namespace(String namespace) {
		this.namespace = namespace;
		return this;
	}

	public Updater<T> set(String setName) {
		this.setName = setName;
		return this;
	}

	public Updater<T> key(String... keys) {
		this.stringKeys.addAll(Arrays.asList(keys));
		return this;
	}

	public Updater<T> key(Long... keys) {
		this.longKeys.addAll(Arrays.asList(keys));
		return this;
	}

	public Updater<T> key(Key... keys) {
		this.keys.addAll(Arrays.asList(keys));
		return this;
	}

	public Updater<T> policy(WritePolicy policy) {
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

	public Key put() {

		collectKeys();

		// this should be a one-key operation
		// if multiple keys - use the first key
		Key key = keys.get(0);

		if (objects == null || objects.length == 0) {
			throw new IllegalStateException("Error: parameter 'objects' must not be null or empty array");
		}
		T object = objects[0];

		Map<String, Object> props = mapper.getProperties(objects[0]);
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

	public void putAll() {

		collectKeys();

		if (objects.length != keys.size()) {
			throw new IllegalStateException("Error: with multi-put you need to provide equal number of objects and keys");
		}

		for (int i = 0; i < objects.length; i++) {

			T object = objects[i];
			Key key = keys.get(i);

			if (key == null || object == null) {
				throw new IllegalStateException("Error: with multi-put all objects and keys must NOT be null");
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
				policy.expiration = (int) (expiration / 1000) - 1262304000; // todo fix Expiration
			}

			synClient.put(policy, key, bins);
		}
	}

}
