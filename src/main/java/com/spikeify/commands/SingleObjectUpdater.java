package com.spikeify.commands;

import com.aerospike.client.Bin;
import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.async.IAsyncClient;
import com.aerospike.client.policy.RecordExistsAction;
import com.aerospike.client.policy.WritePolicy;
import com.spikeify.*;

import java.util.Map;
import java.util.Set;

public class SingleObjectUpdater<T> {

	private final T object;

	public SingleObjectUpdater(Class type, IAerospikeClient synClient, IAsyncClient asyncClient,
	                           RecordsCache recordsCache, boolean create, String defaultNamespace, T object) {
		this.synClient = synClient;
		this.asyncClient = asyncClient;
		this.recordsCache = recordsCache;
		this.create = create;
		this.defaultNamespace = defaultNamespace;
		this.policy = new WritePolicy();
		this.policy.sendKey = true;
		this.mapper = MapperService.getMapper(type);
		this.object = object;
	}

	protected String defaultNamespace;
	protected String setName;
	protected IAerospikeClient synClient;
	protected IAsyncClient asyncClient;
	protected RecordsCache recordsCache;
	protected final boolean create;
	protected WritePolicy policy;
	protected ClassMapper<T> mapper;

	public SingleObjectUpdater<T> policy(WritePolicy policy) {
		this.policy = policy;
		this.policy.sendKey = true;
		if (create) {
			this.policy.recordExistsAction = RecordExistsAction.CREATE_ONLY;
		} else {
			this.policy.recordExistsAction = RecordExistsAction.UPDATE_ONLY;
		}
		return this;
	}

	protected Key collectKey(Object obj) {

		// get metadata for object
		ObjectMetadata meta = MapperService.getMapper(obj.getClass()).getRequiredMetadata(obj, defaultNamespace);
		if (meta.userKeyString != null) {
			return new Key(meta.namespace, meta.setName, meta.userKeyString);
		} else {
			return new Key(meta.namespace, meta.setName, meta.userKeyLong.longValue());
		}
	}

	public Key now() {

		if (create) {
			policy.recordExistsAction = RecordExistsAction.CREATE_ONLY;
		} else {
			policy.recordExistsAction = RecordExistsAction.UPDATE;
		}

		// this should be a one-key operation
		// if multiple keys - use the first key

		if (object == null) {
			throw new SpikeifyError("Error: parameter 'objects' must not be null or empty array");
		}

		Key key = collectKey(object);

		Map<String, Object> props = mapper.getProperties(object);
		Set<String> changedProps = recordsCache.update(key, props);

		Bin[] bins = new Bin[changedProps.size()];
		int position = 0;
		for (String propName : changedProps) {
			bins[position++] = new Bin(propName, props.get(propName));
		}

		Long expiration = mapper.getExpiration(object);
		if (expiration != null) {
			// Entities expiration:  Java time in milliseconds
			// Aerospike expiration: seconds from 1.1.2010 = 1262304000s.
			policy.expiration = (int) (expiration / 1000) - 1262304000;
		}

		synClient.put(policy, key, bins);
		return key;
	}
}
