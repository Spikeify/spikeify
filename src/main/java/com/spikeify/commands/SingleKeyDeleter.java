package com.spikeify.commands;

import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.async.IAsyncClient;
import com.spikeify.RecordsCache;
import com.spikeify.Spikeify;
import com.spikeify.SpikeifyError;
import com.spikeify.annotations.Namespace;
import com.spikeify.annotations.SetName;

import java.util.HashMap;
import java.util.Map;

/**
 * A command chain for deleting object from database.
 * This class is not intended to be instantiated by user.
 * @param <T>
 */
public class SingleKeyDeleter<T> {

	/**
	 * Used internally to create a command chain. Not intended to be used by the user directly.
	 * Use {@link Spikeify#delete(Key)}  instead.
	 */
	public SingleKeyDeleter(IAerospikeClient synClient, IAsyncClient asyncClient,
	                        RecordsCache recordsCache, String defaultNamespace, Key key) {
		this.synClient = synClient;
		this.asyncClient = asyncClient;
		this.recordsCache = recordsCache;
		this.namespace = defaultNamespace;
		this.key = key;
	}

	/**
	 * Used internally to create a command chain. Not intended to be used by the user directly.
	 * Use {@link Spikeify#delete(Long)}  instead.
	 */
	public SingleKeyDeleter(IAerospikeClient synClient, IAsyncClient asyncClient,
	                        RecordsCache recordsCache, String defaultNamespace, Long userKey) {
		this.synClient = synClient;
		this.asyncClient = asyncClient;
		this.recordsCache = recordsCache;
		this.namespace = defaultNamespace;
		this.longKey = userKey;
	}

	/**
	 * Used internally to create a command chain. Not intended to be used by the user directly.
	 * Use {@link Spikeify#delete(String)}  instead.
	 */
	public SingleKeyDeleter(IAerospikeClient synClient, IAsyncClient asyncClient,
	                        RecordsCache recordsCache, String defaultNamespace, String userKey) {
		this.synClient = synClient;
		this.asyncClient = asyncClient;
		this.recordsCache = recordsCache;
		this.namespace = defaultNamespace;
		this.stringKey = userKey;
	}

	private Key key;
	private Long longKey;
	private String stringKey;
	protected String namespace;
	protected String setName;
	protected final IAerospikeClient synClient;
	protected final IAsyncClient asyncClient;
	private final RecordsCache recordsCache;

	/**
	 * Sets the Namespace. Overrides the default namespace and the namespace defined on the Class via {@link Namespace} annotation.
	 * @param namespace The namespace.
	 */
	public SingleKeyDeleter<T> namespace(String namespace) {
		this.namespace = namespace;
		return this;
	}

	/**
	 * Sets the SetName. Overrides any SetName defined on the Class via {@link SetName} annotation.
	 * @param setName The name of the set.
	 */
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
			throw new SpikeifyError("Namespace not set.");
		}
		return namespace;
	}

	protected String getSetName() {
		return setName != null ? setName : null; //mapper.getSetName();
	}

	/**
	 * Synchronously executes the delete command.
	 *
	 * @return whether record existed on server before deletion
	 */
	public boolean now() {

		collectKeys();

		recordsCache.remove(key);
		return synClient.delete(null, key);
	}

}
