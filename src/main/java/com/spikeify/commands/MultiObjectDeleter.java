package com.spikeify.commands;

import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.async.IAsyncClient;
import com.spikeify.MapperService;
import com.spikeify.ObjectMetadata;
import com.spikeify.RecordsCache;

import java.util.HashMap;
import java.util.Map;

public class MultiObjectDeleter {

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

	protected Map<Object, Key> data = new HashMap<>(10);

	private String defaultNamespace;
	protected final IAerospikeClient synClient;
	protected final IAsyncClient asyncClient;
	private final RecordsCache recordsCache;

	protected Key collectKey(Object obj) {

		// get metadata for object
		ObjectMetadata meta = MapperService.getMapper(obj.getClass()).getRequiredMetadata(obj, defaultNamespace);
		if (meta.userKeyString != null) {
			return new Key(meta.namespace, meta.setName, meta.userKeyString);
		} else {
			return new Key(meta.namespace, meta.setName, meta.userKeyLong.longValue());
		}
	}

	/**
	 * Executes the delete command immediately.
	 *
	 * @return whether record existed on server before deletion
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
