package com.spikeify.commands;

import com.aerospike.client.Key;
import com.aerospike.client.async.IAsyncClient;
import com.aerospike.client.policy.WritePolicy;
import com.spikeify.MapperService;
import com.spikeify.ObjectMetadata;
import com.spikeify.RecordsCache;
import com.spikeify.Spikeify;

import java.util.HashMap;
import java.util.Map;

/**
 * A command chain for deleting multiple objects from database.
 * This class is not intended to be instantiated by user.
 */
@SuppressWarnings("WeakerAccess")
public class MultiObjectDeleter<T> {

	/**
	 * Used internally to create a command chain. Not intended to be used by the user directly.
	 * Use {@link Spikeify#deleteAll(Object...)} instead.
	 */
	public MultiObjectDeleter(IAsyncClient asynClient,
	                          RecordsCache recordsCache,
	                          String defaultNamespace,
	                          T... objects) {

		this.asynClient = asynClient;
		this.recordsCache = recordsCache;
		this.defaultNamespace = defaultNamespace;
		for (T object : objects) {
			data.put(object, collectKey(object));
		}

		// must be set in order for later queries to return record keys
		this.asynClient.getWritePolicyDefault().sendKey = true;
	}

	protected final Map<T, Key> data = new HashMap<>(10);

	private final String defaultNamespace;
	protected final IAsyncClient asynClient;
	private final RecordsCache recordsCache;
	private WritePolicy overridePolicy;

	/**
	 * Sets the {@link WritePolicy} to be used when deleting the record in the database.
	 *
	 * @param policy The write policy.
	 * @return this instance
	 */
	public MultiObjectDeleter policy(WritePolicy policy) {
		this.overridePolicy = policy;
		return this;
	}

	private WritePolicy getPolicy() {
		return overridePolicy != null ? overridePolicy : new WritePolicy(asynClient.getWritePolicyDefault());
	}

	protected Key collectKey(T obj) {

		// get metadata for object
		ObjectMetadata meta = MapperService.getMapper(obj.getClass()).getRequiredMetadata(obj, defaultNamespace);
		if (meta.userKeyString != null) {
			return new Key(meta.namespace, meta.setName, meta.userKeyString);
		} else {
			return new Key(meta.namespace, meta.setName, meta.userKeyLong);
		}
	}

	/**
	 * Synchronously executes multiple delete commands.
	 *
	 * @return The Map of Key, Boolean pairs. The boolean tells whether object existed in the
	 * database prior to deletion.
	 */
	public Map<T, Boolean> now() {

		Map<T, Boolean> result = new HashMap<>(data.size());

		for (T obj : data.keySet()) {
			Key key = data.get(obj);
			recordsCache.remove(key);
			result.put(obj, asynClient.delete(getPolicy(), key));
		}

		return result;
	}

}
