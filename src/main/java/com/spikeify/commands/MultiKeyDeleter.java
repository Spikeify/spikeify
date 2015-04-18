package com.spikeify.commands;

import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.async.IAsyncClient;
import com.spikeify.RecordsCache;

import java.util.*;

public class MultiKeyDeleter{

	private List<Key> keys = new ArrayList<>();
	private Long[] longKeys;
	private String[] stringKeys;

	public MultiKeyDeleter(IAerospikeClient synClient, IAsyncClient asyncClient,
	                       RecordsCache recordsCache, String defaultNamespace) {
		this.synClient = synClient;
		this.asyncClient = asyncClient;
		this.recordsCache = recordsCache;
		this.namespace = defaultNamespace;
	}

	protected String namespace;
	protected String setName;
	protected final IAerospikeClient synClient;
	protected final IAsyncClient asyncClient;
	private final RecordsCache recordsCache;

	public MultiKeyDeleter namespace(String namespace) {
		this.namespace = namespace;
		return this;
	}

	public MultiKeyDeleter set(String setName) {
		this.setName = setName;
		return this;
	}

	public MultiKeyDeleter key(String... keys) {
		this.stringKeys = keys;
		this.longKeys = null;
		this.keys.clear();
		return this;
	}

	public MultiKeyDeleter key(Long... keys) {
		this.longKeys = keys;
		this.stringKeys = null;
		this.keys.clear();
		return this;
	}

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
		if (namespace == null) {
			throw new IllegalStateException("Namespace not set.");
		}
		return namespace;
	}

	protected String getSetName() {
		return setName != null ? setName : null; //mapper.getSetName();
	}

	/**
	 * Executes the delete command immediately.
	 *
	 * @return whether record existed on server before deletion
	 */
	public Map<Key, Boolean> now() {

		collectKeys();
		Map<Key, Boolean> result = new HashMap<>(keys.size());

		for (Key key : keys) {
			recordsCache.remove(key);
			result.put(null, synClient.delete(null, key));
		}

		return result;
	}

}
