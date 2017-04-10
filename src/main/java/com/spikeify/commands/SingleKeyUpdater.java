package com.spikeify.commands;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.async.IAsyncClient;
import com.aerospike.client.listener.WriteListener;
import com.aerospike.client.policy.GenerationPolicy;
import com.aerospike.client.policy.RecordExistsAction;
import com.aerospike.client.policy.WritePolicy;
import com.spikeify.*;
import com.spikeify.annotations.Namespace;
import com.spikeify.annotations.SetName;
import com.spikeify.async.WriteListenerFuture;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;

/**
 * A command chain for creating or updating a single object in database.
 * This class is not intended to be instantiated by user.
 *
 * @param <T>
 */
@SuppressWarnings({"unchecked", "WeakerAccess"})
public class SingleKeyUpdater<T, K> {

	/**
	 * Used internally to create a command chain. Not intended to be used by the user directly.
	 * Instead use {@link Spikeify#update(Key, Object)} or similar method.
	 */
	public SingleKeyUpdater(boolean isTx,
	                        IAsyncClient asynClient,
	                        RecordsCache recordsCache,
	                        boolean create,
	                        String defaultNamespace,
	                        T object,
	                        K key) {
		this.isTx = isTx;
		this.asynClient = asynClient;
		this.recordsCache = recordsCache;
		this.create = create;
		this.namespace = defaultNamespace;
		this.object = object;
		this.mapper = MapperService.getMapper((Class<T>) object.getClass());
		// if both TTL and Expires is defined TTL is preferred
		Long ttl = mapper.getRecordTtl(object);
		this.recordExpiration = ttl != null ? Integer.valueOf(ttl.intValue()) : mapper.getRecordExpiration(object);
		if (key.getClass().equals(Key.class)) {
			this.key = (Key) key;
			this.keyType = KeyType.KEY;
		} else if (key.getClass().equals(Long.class)) {
			this.longKey = (Long) key;
			this.keyType = KeyType.LONG;
		} else if (key.getClass().equals(String.class)) {
			this.stringKey = (String) key;
			this.keyType = KeyType.STRING;
		} else {
			throw new IllegalArgumentException("Error: unsupported key type. " +
					"SingleKeyUpdater constructor can only ba called with K as : Key, Long or String");
		}
	}

	private final T object;
	protected KeyType keyType;
	protected String namespace;
	protected String setName;
	protected String stringKey;
	protected Long longKey;
	protected Key key;
	protected final IAsyncClient asynClient;
	protected final RecordsCache recordsCache;
	protected final boolean create;
	protected WritePolicy overridePolicy;
	protected final ClassMapper<T> mapper;
	private final boolean isTx;
	private final Integer recordExpiration;
	private boolean forceReplace = false;

	/**
	 * Sets the Namespace. Overrides the default namespace and the namespace defined on the Class via {@link Namespace} annotation.
	 *
	 * @param namespace The namespace.
	 */
	public SingleKeyUpdater<T, K> namespace(String namespace) {
		this.namespace = namespace;
		return this;
	}

	/**
	 * Sets the SetName. Overrides any SetName defined on the Class via {@link SetName} annotation.
	 *
	 * @param setName The name of the set.
	 */
	public SingleKeyUpdater<T, K> setName(String setName) {
		this.setName = setName;
		return this;
	}

	/**
	 * Sets the {@link WritePolicy} to be used when creating or updating the record in the database.
	 * Internally the 'sendKey' property of the policy will always be set to true.
	 * If this method is called within .transact() method then the 'generationPolicy' property will be set to GenerationPolicy.EXPECT_GEN_EQUAL
	 * The 'recordExistsAction' property is set accordingly depending if this is a create or update operation
	 *
	 * @param policy The policy.
	 */
	public SingleKeyUpdater<T, K> policy(WritePolicy policy) {
		this.overridePolicy = policy;
		this.overridePolicy.sendKey = true;
		return this;
	}

	private WritePolicy getPolicy(boolean nonNullField) {
		WritePolicy writePolicy = overridePolicy != null ? overridePolicy : new WritePolicy(asynClient.getWritePolicyDefault());
		// must be set in order for later queries to return record keys
		writePolicy.sendKey = true;

		if (recordExpiration != null) {
			writePolicy.expiration = recordExpiration;
		}

		// is version checking necessary
		if (isTx) {
			Integer generation = mapper.getGeneration(object);
			writePolicy.generationPolicy = GenerationPolicy.EXPECT_GEN_EQUAL;
			if (generation != null) {
				writePolicy.generation = generation;
			} else {
				throw new SpikeifyError("Error: missing @Generation field in class " + object.getClass() +
						". When using transact(..) you must have @Generation annotation on a field in the entity class.");
			}
		}

		if (create) {
			writePolicy.recordExistsAction = RecordExistsAction.CREATE_ONLY;
			if (!nonNullField) {
				throw new SpikeifyError("Error: cannot create object with no writable properties. " +
						"At least one object property other then UserKey must be different from NULL.");
			}
		} else {
			if (forceReplace) {
				writePolicy.recordExistsAction = RecordExistsAction.REPLACE;
			} else {
				writePolicy.recordExistsAction = RecordExistsAction.UPDATE;
			}
		}

		return writePolicy;
	}

	/**
	 * Sets updater to skip cache check for object changes. This causes that all
	 * object properties will be written to database. It also deletes previous saved
	 * properties in database and now not mapped to object.
	 */
	public SingleKeyUpdater<T, K> forceReplace() {
		this.forceReplace = true;
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

	/**
	 * Synchronously executes a single create or update command and returns the key of the record.
	 *
	 * @return The key of the record. The type of the key returned depends on the way this class is instantiated.
	 * It can be: {@link Key}, Long or String.
	 */
	@SuppressWarnings("ConstantConditions")
	public K now() {
		return now(null);  // invoke internal sync version
	}

	/**
	 * Internal update method that can run sync or async, depending on the provided listener being provided or null.
	 *
	 * @param writeListener
	 * @return
	 */
	private K now(WriteListener writeListener) {
		try {

			return nowInternal(writeListener);  // invoke internal sync version

		} catch (AerospikeException e) {
			if (e.getResultCode() == 2) {
				recordsCache.remove(key);
				return nowInternal(writeListener);
			}
			throw e;
		}
	}

	private K nowInternal(WriteListener writeListener) {

		collectKeys();
		mapper.checkKeyType(key);

		if (object == null) {
			throw new SpikeifyError("Error: parameter 'object' must not be null");
		}

		Map<String, Object> props = mapper.getProperties(object);
		Set<String> changedProps = recordsCache.update(key, props, create || forceReplace);

		List<Bin> bins = new ArrayList<>();
		boolean nonNullField = false;
		for (String propName : changedProps) {
			Object value = props.get(propName);
			if (value == null) {
				if (!forceReplace) {
					bins.add(Bin.asNull(propName));
				}
			} else if (value instanceof List<?>) {
				bins.add(new Bin(propName, (List) value));
				nonNullField = true;
			} else if (value instanceof Map<?, ?>) {
				bins.add(new Bin(propName, (Map) value));
				nonNullField = true;
			} else {
				bins.add(new Bin(propName, value));
				nonNullField = true;
			}
		}

		WritePolicy usePolicy = getPolicy(nonNullField);

		// if we are updating an existing record and no bins are to be updated,
		// then just touch the entity to update expiry timestamp
		if (!create && bins.isEmpty()) {
			if (recordExpiration != null) {
				asynClient.touch(usePolicy, key);
			}
		} else {
			if (writeListener == null) {
				asynClient.put(usePolicy, key, bins.toArray(new Bin[bins.size()]));  // sync
			} else {
				asynClient.put(usePolicy, writeListener, key, bins.toArray(new Bin[bins.size()]));  // async
			}
		}

		return prepareKey(key);
	}

	public Future<K> async() {


		WriteListenerFuture<K> future = new WriteListenerFuture<K>() {
			@Override
			public K prepareResult(Key key) {
				return prepareKey(key);
			}
		};

		now(future);

		return future;

	}

	private K prepareKey(Key key){
		switch (keyType) {
			case KEY:
				return (K) key;
			case LONG:
				return (K) (Long) key.userKey.toLong();
			case STRING:
				return (K) key.userKey.toString();
			default:
				throw new IllegalStateException("Error: unsupported key type. Must be one of: Key, Long or String"); // should not happen
		}
	}
}
