package com.spikeify.commands;

import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.async.IAsyncClient;
import com.spikeify.RecordsCache;

import java.util.HashMap;
import java.util.Map;

public class SingleKeyDeleter<T> {

	private Key key;
	private Long longKey;
	private String stringKey;

	public SingleKeyDeleter(IAerospikeClient synClient, IAsyncClient asyncClient,
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

	public SingleKeyDeleter<T> namespace(String namespace) {
		this.namespace = namespace;
		return this;
	}

	public SingleKeyDeleter<T> set(String setName) {
		this.setName = setName;
		return this;
	}

	public SingleKeyDeleter<T> key(String key) {
		this.stringKey = key;
		this.longKey = null;
		this.key = null;
		return this;
	}

	public SingleKeyDeleter<T> key(Long key) {
		this.longKey = key;
		this.stringKey = null;
		this.key = null;
		return this;
	}

	public SingleKeyDeleter<T> key(Key key) {
		this.key = key;
		this.stringKey = null;
		this.longKey = null;
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
	public boolean now() {

		collectKeys();

		recordsCache.remove(key);
		return synClient.delete(null, key);
	}

}
