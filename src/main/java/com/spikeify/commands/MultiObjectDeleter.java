package com.spikeify.commands;

import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.async.IAsyncClient;
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
public class MultiObjectDeleter {

	/**
	 * Used internally to create a command chain. Not intended to be used by the user directly.
	 * Use {@link Spikeify#deleteAll(Object...)} instead.
	 */
	public MultiObjectDeleter(IAerospikeClient synClient, IAsyncClient asyncClient,
	                          RecordsCache recordsCache, String defaultNamespace, Object... objects) {
		this.synClient = synClient;
		this.asyncClient = asyncClient;
		this.recordsCache = recordsCache;
		this.defaultNamespace = defaultNamespace;
		for (Object object : objects) {
			data.put(object, collectKey(object));
		}
	}

	protected final Map<Object, Key> data = new HashMap<>(10);

	private final String defaultNamespace;
	protected final IAerospikeClient synClient;
	protected final IAsyncClient asyncClient;
	private final RecordsCache recordsCache;

	protected Key collectKey(Object obj) {

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
	public Map<Object, Boolean> now() {

		Map<Object, Boolean> result = new HashMap<>(data.size());

		for (Object obj : data.keySet()) {
			Key key = data.get(obj);
			recordsCache.remove(key);
			result.put(obj, synClient.delete(null, key));
		}

		return result;
	}

}
