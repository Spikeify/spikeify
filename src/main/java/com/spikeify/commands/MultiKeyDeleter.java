package com.spikeify.commands;

import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.async.IAsyncClient;
import com.spikeify.*;
import com.spikeify.annotations.Namespace;
import com.spikeify.annotations.SetName;

import java.util.*;

/**
 * A command chain for deleting multiple objects from database.
 * This class is not intended to be instantiated by user.
 */
@SuppressWarnings("WeakerAccess")
public class MultiKeyDeleter {

	/**
	 * Used internally to create a command chain. Not intended to be used by the user directly.
	 * Use {@link Spikeify#deleteAll(Key...)} instead.
	 */
	public MultiKeyDeleter(IAerospikeClient synClient, IAsyncClient asyncClient,
	                       RecordsCache recordsCache, String defaultNamespace, Key... keys) {
		this.synClient = synClient;
		this.asyncClient = asyncClient;
		this.recordsCache = recordsCache;
		this.namespace = defaultNamespace;
		this.keys = Arrays.asList(keys);
	}

	/**
	 * Used internally to create a command chain. Not intended to be used by the user directly.
	 * Use {@link Spikeify#deleteAll(Long...)} instead.
	 */
	public MultiKeyDeleter(IAerospikeClient synClient, IAsyncClient asyncClient,
	                       RecordsCache recordsCache, String defaultNamespace, Long... keys) {
		this.synClient = synClient;
		this.asyncClient = asyncClient;
		this.recordsCache = recordsCache;
		this.namespace = defaultNamespace;
		this.longKeys = keys;
	}

	/**
	 * Used internally to create a command chain. Not intended to be used by the user directly.
	 * Use {@link Spikeify#deleteAll(String...)} instead.
	 */
	public MultiKeyDeleter(IAerospikeClient synClient, IAsyncClient asyncClient,
	                       RecordsCache recordsCache, String defaultNamespace, String... keys) {
		this.synClient = synClient;
		this.asyncClient = asyncClient;
		this.recordsCache = recordsCache;
		this.namespace = defaultNamespace;
		this.stringKeys = keys;
	}

	/**
	 * Used internally to create a command chain. Not intended to be used by the user directly.
	 * Use {@link Spikeify#deleteAll(String...)} instead.
	 */
	public MultiKeyDeleter(IAerospikeClient synClient, IAsyncClient asyncClient,
	                       RecordsCache recordsCache, String defaultNamespace, Class type) {
		this.synClient = synClient;
		this.asyncClient = asyncClient;
		this.recordsCache = recordsCache;
		this.namespace = defaultNamespace;

		this.mapper = MapperService.getMapper(type);
		this.type = type;
	}

	private ClassMapper mapper;
	private Class type;
	private List<Key> keys = new ArrayList<>();
	private Long[] longKeys;
	private String[] stringKeys;
	protected String namespace;
	protected String setName;
	protected final IAerospikeClient synClient;
	protected final IAsyncClient asyncClient;
	private final RecordsCache recordsCache;

	/**
	 * Sets the Namespace. Overrides the default namespace and the namespace defined on the Class via {@link Namespace} annotation.
	 *
	 * @param namespace The namespace.
	 */
	public MultiKeyDeleter namespace(String namespace) {
		this.namespace = namespace;
		return this;
	}

	/**
	 * Sets the SetName. Overrides any SetName defined on the Class via {@link SetName} annotation.
	 *
	 * @param setName The name of the set.
	 */
	public MultiKeyDeleter setName(String setName) {
		this.setName = setName;
		return this;
	}

	/**
	 * Sets the keys of the records to be loaded.
	 *
	 * @param keys Array of keys
	 */
	public MultiKeyDeleter key(String... keys) {
		this.stringKeys = keys;
		this.longKeys = null;
		this.keys.clear();
		return this;
	}

	/**
	 * Sets the keys of the records to be loaded.
	 *
	 * @param keys Array of keys
	 */
	public MultiKeyDeleter key(Long... keys) {
		this.longKeys = keys;
		this.stringKeys = null;
		this.keys.clear();
		return this;
	}

	/**
	 * Sets the keys of the records to be loaded.
	 *
	 * @param keys Array of keys
	 */
	public MultiKeyDeleter key(Key... keys) {
		this.keys = Arrays.asList(keys);
		this.stringKeys = null;
		this.longKeys = null;
		return this;
	}

	protected void collectKeys() {

		// check if any Long or String keys were provided
		if (stringKeys != null) {
			for (String stringKey : stringKeys) {
				keys.add(new Key(getNamespace(), getSetName(), stringKey));
			}
		} else if (longKeys != null) {
			for (long longKey : longKeys) {
				keys.add(new Key(getNamespace(), getSetName(), longKey));
			}
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
		String setName =  this.setName != null ? this.setName : (mapper != null ? mapper.getSetName() : null);
		if (setName == null) {
			throw new SpikeifyError("Set Name not provided.");
		}
		return setName;
	}

	/**
	 * Synchronously executes multiple delete commands.
	 *
	 * @return The Map of Key, Boolean pairs. The boolean tells whether object existed in the
	 * database prior to deletion.
	 */
	public Map<Key, Boolean> now() {

		collectKeys();
		Map<Key, Boolean> result = new HashMap<>(keys.size());

		for (Key key : keys) {
			recordsCache.remove(key);
			result.put(key, synClient.delete(null, key));
		}

		return result;
	}

}
