package com.spikeify;

import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.async.IAsyncClient;

import java.util.*;

public class MultiDeleter<T> {

	public MultiDeleter(IAerospikeClient synClient, IAsyncClient asyncClient,
	                    RecordsCache recordsCache, T... objects) {
		this.synClient = synClient;
		this.asyncClient = asyncClient;
		this.recordsCache = recordsCache;
		this.objects = objects;
	}

	private final T[] objects;

	protected String namespace;
	protected String setName;
	protected List<String> stringKeys = new ArrayList<>();
	protected List<Long> longKeys = new ArrayList<>();
	protected List<Key> keys = new ArrayList<>(10);
	protected IAerospikeClient synClient;
	protected IAsyncClient asyncClient;
	private RecordsCache recordsCache;

	public MultiDeleter<T> namespace(String namespace) {
		this.namespace = namespace;
		return this;
	}

	public MultiDeleter<T> set(String setName) {
		this.setName = setName;
		return this;
	}

	public MultiDeleter<T> key(String... keys) {
		this.stringKeys.addAll(Arrays.asList(keys));
		return this;
	}

	public MultiDeleter<T> key(Long... keys) {
		this.longKeys.addAll(Arrays.asList(keys));
		return this;
	}

	public MultiDeleter<T> key(Key... keys) {
		this.keys.addAll(Arrays.asList(keys));
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
	public Map<T, Boolean> now() {
		collectKeys();

		Map<T, Boolean> result = new HashMap<>(keys.size());

		for (Key key : keys) {
			recordsCache.remove(key);
			result.put(null, synClient.delete(null, key)) ;
		}

		return result;
	}

}
