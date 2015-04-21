package com.spikeify.commands;

import com.aerospike.client.Bin;
import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.async.IAsyncClient;
import com.aerospike.client.policy.RecordExistsAction;
import com.aerospike.client.policy.WritePolicy;
import com.spikeify.*;

import java.util.*;

public class MultiObjectUpdater{

	private final Object[] objects;

	public MultiObjectUpdater(IAerospikeClient synClient, IAsyncClient asyncClient,
	                          RecordsCache recordsCache, boolean create, String defaultNamespace, Object... objects) {
		this.synClient = synClient;
		this.asyncClient = asyncClient;
		this.recordsCache = recordsCache;
		this.create = create;
		this.namespace = defaultNamespace;
		this.policy = new WritePolicy();
		this.policy.sendKey = true;
		this.objects = objects;
	}

	protected String namespace;
	protected IAerospikeClient synClient;
	protected IAsyncClient asyncClient;
	protected RecordsCache recordsCache;
	protected final boolean create;
	protected WritePolicy policy;

	public MultiObjectUpdater policy(WritePolicy policy) {
		this.policy = policy;
		this.policy.sendKey = true;
		if (create) {
			this.policy.recordExistsAction = RecordExistsAction.CREATE_ONLY;
		} else {
			this.policy.recordExistsAction = RecordExistsAction.UPDATE_ONLY;
		}
		return this;
	}

	protected List<Key> collectKeys() {

		List<Key> keys = new ArrayList<>(objects.length);

		// check if entities have @UserKey entity
		keys.clear();
		for (Object object : objects) {
			ObjectMetadata metadata = MapperService.getMapper(object.getClass()).getRequiredMetadata(object, namespace);
			if (metadata.userKeyString != null) {
				Key objectKey = new Key(metadata.namespace, metadata.setName, metadata.userKeyString);
				keys.add(objectKey);
			} else if (metadata.userKeyLong != null) {
				Key objectKey = new Key(metadata.namespace, metadata.setName, metadata.userKeyLong);
				keys.add(objectKey);
			}
		}

		if (keys.isEmpty()) {
			throw new SpikeifyError("Error: missing parameter 'key'");
		}
		if (keys.size() != objects.length) {
			throw new SpikeifyError("Error scanning @UserKey annotation on objects: " +
					"not all provided objects have @UserKey annotation provided on a field.");
		}

		return keys;
	}

	public Map<Key, Object> now() {

		List<Key> keys = collectKeys();

		if (objects.length != keys.size()) {
			throw new SpikeifyError("Error: with multi-put you need to provide equal number of objects and keys");
		}

		Map<Key, Object> result = new HashMap<>(objects.length);

		for (int i = 0; i < objects.length; i++) {

			Object object = objects[i];
			Key key = keys.get(i);

			if (key == null || object == null) {
				throw new SpikeifyError("Error: with multi-put all objects and keys must NOT be null");
			}

			result.put(key, object);

			ClassMapper mapper = MapperService.getMapper(object.getClass());

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
				policy.expiration = (int) (expiration / 1000) - 1262304000; // todo fix Expiration
			}

			synClient.put(policy, key, bins);
		}

		return result;
	}

}
