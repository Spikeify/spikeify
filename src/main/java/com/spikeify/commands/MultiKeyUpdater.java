package com.spikeify.commands;

import com.aerospike.client.Bin;
import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.async.IAsyncClient;
import com.aerospike.client.policy.RecordExistsAction;
import com.aerospike.client.policy.WritePolicy;
import com.spikeify.ClassMapper;
import com.spikeify.MapperService;
import com.spikeify.ObjectMetadata;
import com.spikeify.RecordsCache;

import java.util.*;

public class MultiKeyUpdater {


	public MultiKeyUpdater(IAerospikeClient synClient, IAsyncClient asyncClient,
	                       RecordsCache recordsCache, boolean create, String namespace,
	                       Key[] keys, Object[] objects) {
		this.synClient = synClient;
		this.asyncClient = asyncClient;
		this.recordsCache = recordsCache;
		this.create = create;
		this.namespace = namespace;
		this.policy = new WritePolicy();
		this.policy.sendKey = true;
		if (keys.length != objects.length) {
			throw new IllegalStateException("Error: keys and objects arrays must be of the same size");
		}
		this.keys = Arrays.asList(keys);
		this.objects = objects;
	}

	public MultiKeyUpdater(IAerospikeClient synClient, IAsyncClient asyncClient,
	                       RecordsCache recordsCache, boolean create, String namespace,
	                       Long[] keys, Object[] objects) {
		this.synClient = synClient;
		this.asyncClient = asyncClient;
		this.recordsCache = recordsCache;
		this.create = create;
		this.namespace = namespace;
		this.policy = new WritePolicy();
		this.policy.sendKey = true;
		if (keys.length != objects.length) {
			throw new IllegalStateException("Error: keys and objects arrays must be of the same size");
		}
		this.longKeys = Arrays.asList(keys);
		this.objects = objects;
	}

	public MultiKeyUpdater(IAerospikeClient synClient, IAsyncClient asyncClient,
	                       RecordsCache recordsCache, boolean create, String namespace,
	                       String[] keys, Object[] objects) {
		this.synClient = synClient;
		this.asyncClient = asyncClient;
		this.recordsCache = recordsCache;
		this.create = create;
		this.namespace = namespace;
		this.policy = new WritePolicy();
		this.policy.sendKey = true;
		if (keys.length != objects.length) {
			throw new IllegalStateException("Error: keys and objects arrays must be of the same size");
		}
		this.stringKeys = Arrays.asList(keys);
		this.objects = objects;
	}

	private final Object[] objects;
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

	public MultiKeyUpdater namespace(String namespace) {
		this.namespace = namespace;
		return this;
	}

	public MultiKeyUpdater set(String setName) {
		this.setName = setName;
		return this;
	}

	public MultiKeyUpdater key(String... keys) {
		if (keys.length != objects.length) {
			throw new IllegalStateException("Number of keys does not match number of objects.");
		}
		this.stringKeys = Arrays.asList(keys);
		this.longKeys.clear();
		this.keys.clear();
		return this;
	}

	public MultiKeyUpdater key(Long... keys) {
		if (keys.length != objects.length) {
			throw new IllegalStateException("Number of keys does not match number of objects.");
		}
		this.longKeys = Arrays.asList(keys);
		this.stringKeys.clear();
		this.keys.clear();
		return this;
	}

	public MultiKeyUpdater key(Key... keys) {
		if (keys.length != objects.length) {
			throw new IllegalStateException("Number of keys does not match number of objects.");
		}
		this.keys = Arrays.asList(keys);
		this.stringKeys.clear();
		this.longKeys.clear();
		return this;
	}

	public MultiKeyUpdater policy(WritePolicy policy) {
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

		if (namespace == null) {
			throw new IllegalStateException("Namespace not set.");
		}

		// check if any Long or String keys were provided
		if (!stringKeys.isEmpty()) {
			for (String stringKey : stringKeys) {
				keys.add(new Key(namespace, setName, stringKey));
			}
		} else if (!longKeys.isEmpty()) {
			for (long longKey : longKeys) {
				keys.add(new Key(namespace, setName, longKey));
			}
		}

		if (keys.isEmpty()) {
			throw new IllegalStateException("Error: missing parameter 'key'");
		}
	}

	public Map<Key, Object> now() {

		collectKeys();

		if (objects.length != keys.size()) {
			throw new IllegalStateException("Error: with multi-put you need to provide equal number of objects and keys");
		}

		Map<Key, Object> result = new HashMap<>(objects.length);

		for (int i = 0; i < objects.length; i++) {

			Object object = objects[i];
			Key key = keys.get(i);

			if (key == null || object == null) {
				throw new IllegalStateException("Error: with multi-put all objects and keys must NOT be null");
			}

			result.put(key, object);

			ClassMapper mapper = MapperService.getMapper(object.getClass());

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
