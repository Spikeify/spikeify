package com.spikeify;

import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.async.IAsyncClient;
import com.aerospike.client.command.ParticleType;
import com.aerospike.client.policy.BatchPolicy;

import java.util.*;

public class MultiLoader<T> {

	public MultiLoader(Class<T> type, IAerospikeClient synClient, IAsyncClient asyncClient, ClassConstructor classConstructor, RecordsCache recordsCache) {
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

	public MultiLoader<T> namespace(String namespace) {
		this.namespace = namespace;
		return this;
	}

	public MultiLoader<T> set(String setName) {
		this.setName = setName;
		return this;
	}

	public MultiLoader<T> key(String... keys) {
		this.stringKeys.addAll(Arrays.asList(keys));
		return this;
	}

	public MultiLoader<T> key(Long... keys) {
		this.longKeys.addAll(Arrays.asList(keys));
		return this;
	}

	public MultiLoader<T> key(Key... keys) {
		this.keys.addAll(Arrays.asList(keys));
		return this;
	}

	public MultiLoader<T> policy(BatchPolicy policy) {
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
	 * Executes multiple get commands.
	 *
	 * @return The map of Keys and Java objects mapped from records
	 */
	public Map<Key, T> now() {

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

				result.put(key, object);
			}
		}

		return result;
	}
}
