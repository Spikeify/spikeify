package com.spikeify.commands;

import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.async.IAsyncClient;
import com.spikeify.MapperService;
import com.spikeify.ObjectMetadata;
import com.spikeify.RecordsCache;

public class SingleObjectDeleter {


	public SingleObjectDeleter(IAerospikeClient synClient, IAsyncClient asyncClient,
	                           RecordsCache recordsCache, String defaultNamespace, Object object) {
		this.synClient = synClient;
		this.asyncClient = asyncClient;
		this.recordsCache = recordsCache;
		this.defaultNamespace = defaultNamespace;
		this.object = object;
	}

	private final Object object;

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
	public boolean now() {

		Key key = collectKey(object);
		recordsCache.remove(key);
		return synClient.delete(null, key);
	}

}
