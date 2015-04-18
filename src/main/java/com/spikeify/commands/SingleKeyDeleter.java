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
	                        RecordsCache recordsCache, String defaultNamespace, Key key) {
		this.synClient = synClient;
		this.asyncClient = asyncClient;
		this.recordsCache = recordsCache;
		this.namespace = defaultNamespace;
		this.key = key;
	}

	public SingleKeyDeleter(IAerospikeClient synClient, IAsyncClient asyncClient,
	                        RecordsCache recordsCache, String defaultNamespace, Long userKey) {
		this.synClient = synClient;
		this.asyncClient = asyncClient;
		this.recordsCache = recordsCache;
		this.namespace = defaultNamespace;
		this.longKey = userKey;
	}

	public SingleKeyDeleter(IAerospikeClient synClient, IAsyncClient asyncClient,
	                        RecordsCache recordsCache, String defaultNamespace, String userKey) {
		this.synClient = synClient;
		this.asyncClient = asyncClient;
		this.recordsCache = recordsCache;
		this.namespace = defaultNamespace;
		this.stringKey = userKey;
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
