package com.spikeify.commands;

import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.async.IAsyncClient;
import com.aerospike.client.policy.WritePolicy;
import com.spikeify.MapperService;
import com.spikeify.ObjectMetadata;
import com.spikeify.RecordsCache;
import com.spikeify.Spikeify;

/**
 * A command chain for deleting object from database.
 * This class is not intended to be instantiated by user.
 */
@SuppressWarnings("WeakerAccess")
public class SingleObjectDeleter {

	/**
	 * Used internally to create a command chain. Not intended to be used by the user directly.
	 * Use {@link Spikeify#delete(Object)}  instead.
	 */
	public SingleObjectDeleter(IAsyncClient asynClient,
	                           RecordsCache recordsCache,
	                           String defaultNamespace,
	                           Object object) {
		this.asynClient = asynClient;
		this.recordsCache = recordsCache;
		this.defaultNamespace = defaultNamespace;
		this.object = object;
	}

	private final Object object;

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
	public SingleObjectDeleter policy(WritePolicy policy) {
		this.overridePolicy = policy;
		return this;
	}

	protected Key collectKey(Object obj) {

		// get metadata for object
		ObjectMetadata meta = MapperService.getMapper(obj.getClass()).getRequiredMetadata(obj, defaultNamespace);
		if (meta.userKeyString != null) {
			return new Key(meta.namespace, meta.setName, meta.userKeyString);
		} else {
			return new Key(meta.namespace, meta.setName, meta.userKeyLong);
		}
	}

	private WritePolicy getPolicy(){
		WritePolicy writePolicy = overridePolicy != null ? overridePolicy : new WritePolicy(asynClient.getWritePolicyDefault());
		// must be set in order for later queries to return record keys
		writePolicy.sendKey = true;

		return writePolicy;
	}

	/**
	 * Synchronously executes the delete command.
	 *
	 * @return whether record existed on server before deletion
	 */
	public boolean now() {

		Key key = collectKey(object);
		recordsCache.remove(key);

		return asynClient.delete(getPolicy(), key);
	}


}
