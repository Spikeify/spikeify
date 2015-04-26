package com.spikeify.commands;

import com.aerospike.client.Bin;
import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.async.IAsyncClient;
import com.aerospike.client.policy.GenerationPolicy;
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
public class MultiKeyUpdater {

	/**
	 * Used internally to create a command chain. Not intended to be used by the user directly.
	 * Instead use {@link Spikeify#createAll(Key[], Object[])} (Object...)} method.
	 */
	public MultiKeyUpdater(boolean isTx, IAerospikeClient synClient, IAsyncClient asyncClient,
	                       RecordsCache recordsCache, boolean create, String namespace,
	                       Key[] keys, Object[] objects) {
		this.isTx = isTx;
		this.synClient = synClient;
		this.asyncClient = asyncClient;
		this.recordsCache = recordsCache;
		this.create = create;
		this.namespace = namespace;
		this.policy = new WritePolicy();
		if (keys.length != objects.length) {
			throw new SpikeifyError("Error: keys and objects arrays must be of the same size");
		}
		this.keys = Arrays.asList(keys);
		this.objects = objects;
	}

	/**
	 * Used internally to create a command chain. Not intended to be used by the user directly.
	 * Instead use {@link Spikeify#createAll(Long[], Object[])} (Object...)} method.
	 */
	public MultiKeyUpdater(boolean isTx, IAerospikeClient synClient, IAsyncClient asyncClient,
	                       RecordsCache recordsCache, boolean create, String namespace,
	                       Long[] keys, Object[] objects) {
		this.isTx = isTx;
		this.synClient = synClient;
		this.asyncClient = asyncClient;
		this.recordsCache = recordsCache;
		this.create = create;
		this.namespace = namespace;
		this.policy = new WritePolicy();
		if (keys.length != objects.length) {
			throw new SpikeifyError("Error: keys and objects arrays must be of the same size");
		}
		this.longKeys = Arrays.asList(keys);
		this.objects = objects;
	}

	/**
	 * Used internally to create a command chain. Not intended to be used by the user directly.
	 * Instead use {@link Spikeify#createAll(String[], Object[])} (Object...)} or  method.
	 */
	public MultiKeyUpdater(boolean isTx, IAerospikeClient synClient, IAsyncClient asyncClient,
	                       RecordsCache recordsCache, boolean create, String namespace,
	                       String[] keys, Object[] objects) {
		this.isTx = isTx;
		this.synClient = synClient;
		this.asyncClient = asyncClient;
		this.recordsCache = recordsCache;
		this.create = create;
		this.namespace = namespace;
		this.policy = new WritePolicy();
		if (keys.length != objects.length) {
			throw new SpikeifyError("Error: keys and objects arrays must be of the same size");
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
	private final boolean isTx;
	protected IAerospikeClient synClient;
	protected IAsyncClient asyncClient;
	protected RecordsCache recordsCache;
	protected final boolean create;
	protected WritePolicy policy;

	/**
	 * Sets the Namespace. Overrides the default namespace and the namespace defined on the Class via {@link Namespace} annotation.
	 *
	 * @param namespace The namespace.
	 * @return
	 */
	public MultiKeyUpdater namespace(String namespace) {
		this.namespace = namespace;
		return this;
	}

	/**
	 * Sets the SetName. Overrides any SetName defined on the Class via {@link SetName} annotation.
	 *
	 * @param setName The name of the set.
	 * @return
	 */
	public MultiKeyUpdater set(String setName) {
		this.setName = setName;
		return this;
	}

	/**
	 * Sets the keys of the records to be loaded.
	 *
	 * @param keys
	 * @return
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
	 * @param keys
	 * @return
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
	 * @param keys
	 * @return
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
	 * <br/> Internally the 'sendKey' property of the policy will always be set to true.
	 * <br/> If this method is called within .transact() method then the 'generationPolicy' property will be set to GenerationPolicy.EXPECT_GEN_EQUAL
	 * <br/> The 'recordExistsAction' property is set accordingly depending if this is a create or update operation
	 *
	 * @param policy The policy.
	 * @return
	 */
	public MultiKeyUpdater policy(WritePolicy policy) {
		this.policy = policy;
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

		Map<Key, Object> result = new HashMap<>(objects.length);

		for (int i = 0; i < objects.length; i++) {

			Object object = objects[i];
			Key key = keys.get(i);

			if (key == null || object == null) {
				throw new SpikeifyError("Error: with multi-put all objects and keys must NOT be null");
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

			// must be set so that user key can be retrieved in queries
			this.policy.sendKey = true;

			// type of operation: create or update?
			if (create) {
				this.policy.recordExistsAction = RecordExistsAction.CREATE_ONLY;
			} else {
				this.policy.recordExistsAction = RecordExistsAction.UPDATE;
			}

			// is version checking necessary
			if (isTx) {
				Integer generation = mapper.getGeneration(object);
				policy.generationPolicy = GenerationPolicy.EXPECT_GEN_EQUAL;
				if (generation != null) {
					policy.generation = generation;
				} else {
					throw new SpikeifyError("Error: missing @Generation field in class "+object.getClass()+
							". When using transact(..) you must have @Generation annotation on a field in the entity class.");
				}
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
