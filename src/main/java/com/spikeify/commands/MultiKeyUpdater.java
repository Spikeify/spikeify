package com.spikeify.commands;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.async.IAsyncClient;
import com.aerospike.client.policy.RecordExistsAction;
import com.aerospike.client.policy.WritePolicy;
import com.spikeify.*;
import com.spikeify.annotations.Namespace;
import com.spikeify.annotations.SetName;

import java.util.*;

/**
 * A command chain for creating or updating multiple objects in database.
 * This class is not intended to be instantiated by user.
 */
@SuppressWarnings({"unchecked", "WeakerAccess"})
public class MultiKeyUpdater {

	private boolean forceReplace = false;

	/*
	 * Used internally to create a command chain. Not intended to be used by the user directly.
	 * Instead use {@link Spikeify#createAll(Key[], Object[])} (Object...)} method.
	 */

	@SuppressWarnings("SameParameterValue")
	public MultiKeyUpdater(boolean isTx,  IAsyncClient asynClient,
	                       RecordsCache recordsCache, boolean create, String namespace,
	                       Key[] keys, Object[] objects) {

		this.isTx = isTx;
		this.asynClient = asynClient;
		this.recordsCache = recordsCache;
		this.create = create;
		this.namespace = namespace;
		if (keys.length != objects.length) {
			throw new SpikeifyError("Error: keys and objects arrays must be of the same size");
		}
		this.keys = Arrays.asList(keys);
		this.objects = objects;

		// must be set in order for later queries to return record keys
		this.asynClient.getWritePolicyDefault().sendKey = true;
	}

	/**
	 * Used internally to create a command chain. Not intended to be used by the user directly.
	 * Instead use {@link Spikeify#createAll(Long[], Object[])} (Object...)} method.
	 */
	@SuppressWarnings("SameParameterValue")
	public MultiKeyUpdater(boolean isTx, IAsyncClient asynClient,
	                       RecordsCache recordsCache, boolean create, String namespace,
	                       Long[] keys, Object[] objects) {

		this.isTx = isTx;
		this.asynClient = asynClient;
		this.recordsCache = recordsCache;
		this.create = create;
		this.namespace = namespace;
		if (keys.length != objects.length) {
			throw new SpikeifyError("Error: keys and objects arrays must be of the same size");
		}
		this.longKeys = Arrays.asList(keys);
		this.objects = objects;

		// must be set in order for later queries to return record keys
		this.asynClient.getWritePolicyDefault().sendKey = true;
	}

	/**
	 * Used internally to create a command chain. Not intended to be used by the user directly.
	 * Instead use {@link Spikeify#createAll(String[], Object[])} (Object...)} or  method.
	 */
	@SuppressWarnings("SameParameterValue")
	public MultiKeyUpdater(boolean isTx, IAsyncClient asynClient,
	                       RecordsCache recordsCache, boolean create, String namespace,
	                       String[] keys, Object[] objects) {

		this.isTx = isTx;
		this.asynClient = asynClient;
		this.recordsCache = recordsCache;
		this.create = create;
		this.namespace = namespace;
		if (keys.length != objects.length) {
			throw new SpikeifyError("Error: keys and objects arrays must be of the same size");
		}
		this.stringKeys = Arrays.asList(keys);
		this.objects = objects;

		// must be set in order for later queries to return record keys
		this.asynClient.getWritePolicyDefault().sendKey = true;
	}

	private final Object[] objects;

	protected String namespace;

	protected String setName;

	protected List<String> stringKeys = new ArrayList<>();

	protected List<Long> longKeys = new ArrayList<>();

	protected List<Key> keys = new ArrayList<>(10);

	private final boolean isTx;

	protected final IAsyncClient asynClient;

	protected final RecordsCache recordsCache;

	protected final boolean create;

	protected WritePolicy overridePolicy;

	/**
	 * Sets the Namespace. Overrides the default namespace and the namespace defined on the Class via {@link Namespace} annotation.
	 *
	 * @param namespace The namespace.
	 * @return updater
	 */
	public MultiKeyUpdater namespace(String namespace) {

		this.namespace = namespace;
		return this;
	}

	/**
	 * Sets the SetName. Overrides any SetName defined on the Class via {@link SetName} annotation.
	 *
	 * @param setName The name of the set.
	 * @return updater
	 */
	public MultiKeyUpdater setName(String setName) {

		this.setName = setName;
		return this;
	}

	/**
	 * Sets the keys of the records to be loaded.
	 *
	 * @param keys Array of keys
	 * @return updater
	 */
	public MultiKeyUpdater key(String... keys) {

		if (keys.length != objects.length) {
			throw new SpikeifyError("Number of keys does not match number of objects.");
		}
		this.stringKeys = Arrays.asList(keys);
		this.longKeys.clear();
		this.keys.clear();
		return this;
	}

	/**
	 * Sets the keys of the records to be loaded.
	 *
	 * @param keys Array of keys
	 * @return updater
	 */
	public MultiKeyUpdater key(Long... keys) {

		if (keys.length != objects.length) {
			throw new SpikeifyError("Number of keys does not match number of objects.");
		}
		this.longKeys = Arrays.asList(keys);
		this.stringKeys.clear();
		this.keys.clear();
		return this;
	}

	/**
	 * Sets the keys of the records to be loaded.
	 *
	 * @param keys Array of keys
	 * @return updater
	 */
	public MultiKeyUpdater key(Key... keys) {

		if (keys.length != objects.length) {
			throw new SpikeifyError("Number of keys does not match number of objects.");
		}
		this.keys = Arrays.asList(keys);
		this.stringKeys.clear();
		this.longKeys.clear();
		return this;
	}

	/**
	 * Sets the {@link WritePolicy} to be used when creating or updating the record in the database.
	 * Internally the 'sendKey' property of the policy will always be set to true.
	 * If this method is called within .transact() method then the 'generationPolicy' property will be set to GenerationPolicy.EXPECT_GEN_EQUAL
	 * The 'recordExistsAction' property is set accordingly depending if this is a create or update operation
	 *
	 * @param policy The policy.
	 * @return updater
	 */
	public MultiKeyUpdater policy(WritePolicy policy) {

		this.overridePolicy = policy;
		return this;
	}

	/**
	 * Sets updater to skip cache check for object changes. This causes that all
	 * object properties will be written to database. It also deletes previous saved
	 * properties in database and now not mapped to object.
	 *
	 * @return updater
	 */
	public MultiKeyUpdater forceReplace() {

		this.forceReplace = true;
		return this;
	}

	protected void collectKeys() {

		if (namespace == null) {
			throw new SpikeifyError("Namespace not set.");
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
			throw new SpikeifyError("Error: missing parameter 'key'");
		}
	}

	private WritePolicy getPolicy() {

		WritePolicy writePolicy = overridePolicy != null ? overridePolicy : new WritePolicy(asynClient.getWritePolicyDefault());

		writePolicy.recordExistsAction = create ? RecordExistsAction.CREATE_ONLY : forceReplace ? RecordExistsAction.REPLACE : RecordExistsAction.UPDATE;

		// must be set so that user key can be retrieved in queries
		writePolicy.sendKey = true;

		return writePolicy;
	}

	/**
	 * Synchronously executes multiple create or update commands and returns the keys of the records.
	 *
	 * @return The Map of Key, object pairs.
	 */
	public Map<Key, Object> now() {

		collectKeys();

		if (objects.length != keys.size()) {
			throw new SpikeifyError("Error: with multi-put you need to provide equal number of objects and keys");
		}

		WritePolicy usePolicy = getPolicy();

		boolean isReplace = usePolicy.recordExistsAction == RecordExistsAction.REPLACE;

		Map<Key, Object> result = new HashMap<>(objects.length);

		for (int i = 0; i < objects.length; i++) {

			Object object = objects[i];
			Key key = keys.get(i);

			try {

				nowInternalSingle(usePolicy, isReplace, result, object, key);

			} catch (AerospikeException e) {
				// Error Code 2: Key not found
				if (e.getResultCode() == 2) {
					recordsCache.remove(key);
					nowInternalSingle(usePolicy, isReplace, result, object, key);
				}
				else {
					throw e;
				}
			}

		}

		return result;
	}

	private void nowInternalSingle(WritePolicy usePolicy, boolean isReplace, Map<Key, Object> result, Object object, Key key) {
		if (key == null || object == null) {
            throw new SpikeifyError("Error: with multi-put all objects and keys must NOT be null");
        }

		result.put(key, object);

		ClassMapper mapper = MapperService.getMapper(object.getClass());

		Map<String, Object> props = mapper.getProperties(object);
		Set<String> changedProps = recordsCache.update(key, props, forceReplace);

		List<Bin> bins = new ArrayList<>();
		boolean nonNullField = false;
		for (String propName : changedProps) {
			Object value = props.get(propName);
			if (value == null) {
				if (!isReplace) {
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

		if (!nonNullField && props.size() == changedProps.size()) {
			throw new SpikeifyError("Error: cannot create object with no writable properties. " +
					"At least one object property other then UserKey must be different from NULL.");
		}

		// if both TTL and Expires is defined TTL is preferred
		Long ttl = mapper.getRecordTtl(object);
		Integer recordExpiration = ttl != null ? Integer.valueOf(ttl.intValue()) : mapper.getRecordExpiration(object);
		if (recordExpiration != null) {
			usePolicy.expiration = recordExpiration;
		}

        // if we are updating an existing record and no bins are to be updated,
		// then just touch the entity to update expiry timestamp
		if (!create && bins.isEmpty()) {
			if(recordExpiration != null){
				asynClient.touch(usePolicy, key);
			}
		} else {
			asynClient.put(usePolicy, key, bins.toArray(new Bin[bins.size()]));
		}

		// set LDT fields
		mapper.setBigDatatypeFields(object, asynClient, key);
	}

}
