package com.spikeify.commands;

import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.async.IAsyncClient;
import com.aerospike.client.policy.BatchPolicy;
import com.spikeify.*;
import com.spikeify.annotations.Namespace;
import com.spikeify.annotations.SetName;

import java.util.*;

/**
 * A command chain for getting multiple records from database.
 *
 * @param <T> Entity type
 * @param <K> Key type
 */
@SuppressWarnings({"unchecked", "WeakerAccess"})
public class MultiLoader<T, K> {

	/**
	 * Used internally to create a command chain. Not intended to be used by the user directly. Use {@link Spikeify#getAll(Class, Key...)} or similar instead.
	 */
	public MultiLoader(Class<T> type, IAsyncClient asynClient, ClassConstructor classConstructor,
	                   RecordsCache recordsCache, String namespace, K... keys) {

		this.asynClient = asynClient;
		this.classConstructor = classConstructor;
		this.recordsCache = recordsCache;
		this.namespace = namespace;
		this.mapper = MapperService.getMapper(type);
		this.type = type;
		Class componentType = keys.getClass().getComponentType();
		if (componentType.equals(Key.class)) {
			this.keys = Arrays.asList((Key[]) keys);
			this.keyType = KeyType.KEY;
		}
		else if (componentType.equals(Long.class)) {
			this.longKeys = Arrays.asList((Long[]) keys);
			this.keyType = KeyType.LONG;
		}
		else if (componentType.equals(String.class)) {
			this.stringKeys = Arrays.asList((String[]) keys);
			this.keyType = KeyType.STRING;
		}
		else {
			throw new IllegalArgumentException("Error: unsupported key type '" + componentType + "'. Supported key types are Key, Long and String.");
		}

	}

	protected String namespace;

	protected String setName;

	protected List<String> stringKeys = new ArrayList<>();

	protected List<Long> longKeys = new ArrayList<>();

	protected List<Key> keys = new ArrayList<>(10);

	protected final IAsyncClient asynClient;

	protected final ClassConstructor classConstructor;

	protected final RecordsCache recordsCache;

	protected BatchPolicy overridePolicy;

	protected final ClassMapper<T> mapper;

	protected final Class<T> type;

	protected KeyType keyType;

	/**
	 * Sets the Namespace. Overrides the default namespace and the namespace defined on the Class via {@link Namespace} annotation.
	 *
	 * @param namespace The namespace.
	 */
	public MultiLoader<T, K> namespace(String namespace) {

		this.namespace = namespace;
		return this;
	}

	/**
	 * Sets the SetName. Overrides any SetName defined on the Class via {@link SetName} annotation.
	 *
	 * @param setName The name of the set.
	 */
	public MultiLoader<T, K> setName(String setName) {

		this.setName = setName;
		return this;
	}

	/**
	 * Sets the {@link BatchPolicy} to be used when getting the record from the database.
	 * Internally the 'sendKey' property of the policy will always be set to true.
	 *
	 * @param policy The policy.
	 */
	public MultiLoader<T, K> policy(BatchPolicy policy) {

		this.overridePolicy = policy;
		this.overridePolicy.sendKey = true;
		return this;
	}

	private BatchPolicy getPolicy() {

		return overridePolicy != null ? overridePolicy : new BatchPolicy(asynClient.getBatchPolicyDefault());
	}

	protected void collectKeys() {

		if (!stringKeys.isEmpty()) {
			for (String stringKey : stringKeys) {
				keys.add(new Key(getNamespace(), getSetName(), stringKey));
			}
		}
		else if (!longKeys.isEmpty()) {
			for (long longKey : longKeys) {
				keys.add(new Key(getNamespace(), getSetName(), longKey));
			}
		}
		else if (keys.isEmpty()) {
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
	 * Synchronously executes multiple get commands.
	 *
	 * @return The map of Keys and Java objects mapped from records
	 */
	public Map<K, T> now() {

		collectKeys();

		if (keys.size() > 5000) {
			throw new SpikeifyError("Cannot request more then 5000 keys in single batch request. Check out: https://www.aerospike.com/docs/guide/batch.html");
		}

		BatchPolicy usePolicy = getPolicy();

		Key[] keysArray = keys.toArray(new Key[keys.size()]);
		Record[] records = asynClient.get(usePolicy, keysArray);

		Map<K, T> result = new HashMap<>(keys.size());

		Record record;
		for (int i = 0; i < records.length; i++) {
			record = records[i];
			if (record != null) {
				Key key = keysArray[i];

				// construct the entity object via provided ClassConstructor
				T object = classConstructor.construct(type);

				// save record hash into cache - used later for differential updating
				recordsCache.insert(key, record.bins);

				MapperService.map(mapper, key, record, object);

				// set LDT wrappers
				mapper.setBigDatatypeFields(object, asynClient, key);

				switch (keyType) {
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

	/**
	 * Generic method to convert iterator to list
	 *
	 * @return list of values or empty list if none present
	 */
	public List<T> toList() {

		collectKeys();

		if (keys.size() > 5000) {
			throw new SpikeifyError("Cannot request more then 5000 keys in single batch request. Check out: https://www.aerospike.com/docs/guide/batch.html");
		}

		BatchPolicy usePolicy = getPolicy();

		Key[] keysArray = keys.toArray(new Key[keys.size()]);
		Record[] records = asynClient.get(usePolicy, keysArray);

		List<T> result = new ArrayList<>(keys.size());

		Record record;
		for (int i = 0; i < records.length; i++) {
			record = records[i];
			if (record != null) {
				Key key = keysArray[i];

				// construct the entity object via provided ClassConstructor
				T object = classConstructor.construct(type);

				// save record hash into cache - used later for differential updating
				recordsCache.insert(key, record.bins);

				MapperService.map(mapper, key, record, object);

				// set LDT wrappers
				mapper.setBigDatatypeFields(object, asynClient, key);

				result.add(object);
			}
		}

		return result;
	}
}
