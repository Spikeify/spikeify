package com.spikeify;

import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.async.IAsyncClient;
import com.aerospike.client.policy.BatchPolicy;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Loader<T> {

	protected String namespace;
	protected String setName;
	protected String[] stringKeys;
	protected long[] longKeys;
	protected List<Key> keys = new ArrayList<>(10);
	protected IAerospikeClient synClient;
	protected IAsyncClient asyncClient;
	protected ClassConstructor classConstructor;
	private RecordsCache recordsCache;
	protected BatchPolicy policy;
	protected ClassMapper<T> mapper;
	protected Class<T> type;

	public Loader(Class<T> type, IAerospikeClient synClient, IAsyncClient asyncClient, ClassConstructor classConstructor, RecordsCache recordsCache) {
		this.synClient = synClient;
		this.asyncClient = asyncClient;
		this.classConstructor = classConstructor;
		this.recordsCache = recordsCache;
		this.policy = new BatchPolicy();
		this.policy.sendKey = true;
		this.mapper = MapperService.getMapper(type);
		this.type = type;
	}

	public Loader<T> namespace(String namespace) {
		this.namespace = namespace;
		return this;
	}

	public Loader<T> set(String setName) {
		this.setName = setName;
		return this;
	}

	public Loader<T> key(String... keys) {
		this.stringKeys = keys;
		return this;
	}

	public Loader<T> key(long... keys) {
		this.longKeys = keys;
		return this;
	}

	public Loader<T> key(Key... keys) {
		for (Key key : keys) {
			this.keys.add(key);
		}
		return this;
	}

	public Loader<T> policy(BatchPolicy policy) {
		this.policy = policy;
		this.policy.sendKey = true;
		return this;
	}

	protected void collectKeys() {

		Key key;
		if (stringKeys != null) {
			for (String stringKey : stringKeys) {
				keys.add(new Key(getNamespace(), getSetName(), stringKey));
			}
		} else if (longKeys != null) {
			for (long longKey : longKeys) {
				keys.add(new Key(getNamespace(), getSetName(), longKey));
			}
		} else if (keys == null) {
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

	public T get() {

		collectKeys();
		String useNamespace = getNamespace();
		String useSetName = getSetName();

		// this should be a one-key operation
		// if multiple keys - use the first key
		Key key = keys.get(0);

		Record record = synClient.get(policy, key);
		T object = classConstructor.construct(type);

		// save rew records into cache - used later for differential updating
		recordsCache.insert(key, record.bins);

		mapper.setMetaFieldValues(object, useNamespace, useSetName, record.generation, record.expiration);
		mapper.setFieldValues(object, record.bins);

		return object;
	}

	public Map<Key, T> getAll() {

		collectKeys();

		Key[] keysArray = keys.toArray(new Key[keys.size()]);
		Record[] records = synClient.get(policy, keysArray);

		Map<Key, T> result = new HashMap<>(keys.size());

		Record record;
		for (int i = 0; i < records.length; i++) {
			record = records[i];
			if (record != null) {
				Key key = keysArray[i];

				// construct the entity object via provided ClassConstructor
				T object = classConstructor.construct(type);

				// save record hash into cache - used later for differential updating
				recordsCache.insert(keysArray[i], record.bins);

				// set metafields on the entity: @Namespace, @SetName, @Expiration..
				mapper.setMetaFieldValues(object, key.namespace, key.setName, record.generation, record.expiration);

				// set field values
				mapper.setFieldValues(object, record.bins);

				result.put(key, object);
			}
		}

		return result;
	}

}
