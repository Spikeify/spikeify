package com.spikeify;

import com.aerospike.client.Bin;
import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.async.IAsyncClient;
import com.aerospike.client.policy.RecordExistsAction;
import com.aerospike.client.policy.WritePolicy;

import java.util.*;

public class MultiUpdater<T> {

	private final T[] objects;

	public MultiUpdater(Class<T> type, IAerospikeClient synClient, IAsyncClient asyncClient,
	                    RecordsCache recordsCache, boolean create, String namespace, T... objects) {
		this.synClient = synClient;
		this.asyncClient = asyncClient;
		this.recordsCache = recordsCache;
		this.create = create;
		this.namespace = namespace;
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

	public MultiUpdater<T> namespace(String namespace) {
		this.namespace = namespace;
		return this;
	}

	public MultiUpdater<T> set(String setName) {
		this.setName = setName;
		return this;
	}

	public MultiUpdater<T> key(String... keys) {
		if (keys.length != objects.length) {
			throw new IllegalStateException("Number of keys does not match number of objects.");
		}
		this.stringKeys = Arrays.asList(keys);
		this.longKeys.clear();
		this.keys.clear();
		return this;
	}

	public MultiUpdater<T> key(Long... keys) {
		if (keys.length != objects.length) {
			throw new IllegalStateException("Number of keys does not match number of objects.");
		}
		this.longKeys = Arrays.asList(keys);
		this.stringKeys.clear();
		this.keys.clear();
		return this;
	}

	public MultiUpdater<T> key(Key... keys) {
		if (keys.length != objects.length) {
			throw new IllegalStateException("Number of keys does not match number of objects.");
		}
		this.keys = Arrays.asList(keys);
		this.stringKeys.clear();
		this.longKeys.clear();
		return this;
	}

	public MultiUpdater<T> policy(WritePolicy policy) {
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
		if (!stringKeys.isEmpty()) {
			for (String stringKey : stringKeys) {
				keys.add(new Key(getNamespace(), getSetName(), stringKey));
			}
		} else if (!longKeys.isEmpty()) {
			for (long longKey : longKeys) {
				keys.add(new Key(getNamespace(), getSetName(), longKey));
			}
		} else if (keys.isEmpty()) {

			// check if entities have @UserKey entity
			keys.clear();
			for (T object : objects) {
				Object userKey = mapper.getUserKey(object);
				if (userKey != null) {
					if (userKey.getClass() == String.class) {
						Key objectKey = new Key(getNamespace(), getSetName(), (String) userKey);
						keys.add(objectKey);
					} else if (userKey.getClass() == Long.class || userKey.getClass() == long.class) {
						Key objectKey = new Key(getNamespace(), getSetName(), (Long) userKey);
						keys.add(objectKey);
					}
				}
			}
		}

		if (keys.isEmpty()) {
			throw new IllegalStateException("Error: missing parameter 'key'");
		}
		if (keys.size() != objects.length) {
			throw new IllegalStateException("Error scanning @UserKey annotation on objects: " +
					"not all provided objects have @UserKey annotation provided on a field.");
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

	public Map<Key, T> now() {

		collectKeys();

		if (objects.length != keys.size()) {
			throw new IllegalStateException("Error: with multi-put you need to provide equal number of objects and keys");
		}

		Map<Key, T> result = new HashMap<>(objects.length);

		for (int i = 0; i < objects.length; i++) {

			T object = objects[i];
			Key key = keys.get(i);

			if (key == null || object == null) {
				throw new IllegalStateException("Error: with multi-put all objects and keys must NOT be null");
			}

			result.put(key, object);

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

		return result;
	}

}
