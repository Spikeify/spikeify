package com.spikeify.commands;

import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.async.IAsyncClient;
import com.aerospike.client.command.ParticleType;
import com.aerospike.client.policy.BatchPolicy;
import com.spikeify.*;

import java.util.*;

public class MultiLoader<T, K> {

	public MultiLoader(Class<T> type, IAerospikeClient synClient, IAsyncClient asyncClient, ClassConstructor classConstructor,
	                   RecordsCache recordsCache, String namespace, K... keys) {
		this.synClient = synClient;
		this.asyncClient = asyncClient;
		this.classConstructor = classConstructor;
		this.recordsCache = recordsCache;
		this.namespace = namespace;
		this.policy = new BatchPolicy();
		this.policy.sendKey = true;
		this.mapper = MapperService.getMapper(type);
		this.type = type;
		if(keys[0].getClass().equals(Key.class)){
			this.keys = Arrays.asList((Key[]) keys);
			this.keyType = KeyType.KEY;
		} else if(keys[0].getClass().equals(Long.class)){
			this.longKeys = Arrays.asList((Long[]) keys);
			this.keyType = KeyType.LONG;
		} else if(keys[0].getClass().equals(String.class)){
			this.stringKeys = Arrays.asList((String[]) keys);
			this.keyType = KeyType.STRING;
		}
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
	protected KeyType keyType;

	public MultiLoader<T, K> namespace(String namespace) {
		this.namespace = namespace;
		return this;
	}

	public MultiLoader<T, K> set(String setName) {
		this.setName = setName;
		return this;
	}

	public MultiLoader<T, K> policy(BatchPolicy policy) {
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
	 * Executes multiple get commands.
	 *
	 * @return The map of Keys and Java objects mapped from records
	 */
	public Map<K, T> now() {

		collectKeys();

		Key[] keysArray = keys.toArray(new Key[keys.size()]);
		Record[] records = synClient.get(policy, keysArray);

		Map<K, T> result = new HashMap<>(keys.size());

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

				switch (keyType){
					case KEY:
						result.put((K) key, object);
						break;
					case LONG:
						Long longKey = key.userKey.toLong();
						result.put((K) longKey, object);
						break;
					case STRING:
						result.put((K) key.userKey.toString(), object);
						break;
				}
			}
		}

		return result;
	}
}
