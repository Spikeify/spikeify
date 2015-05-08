package com.spikeify.commands;

import com.aerospike.client.Bin;
import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.async.IAsyncClient;
import com.aerospike.client.policy.GenerationPolicy;
import com.aerospike.client.policy.RecordExistsAction;
import com.aerospike.client.policy.WritePolicy;
import com.spikeify.*;

import java.util.*;

/**
 * A command chain for creating or updating multiple objects in database.
 * This class is not intended to be instantiated by user.
 */
@SuppressWarnings({"unchecked", "WeakerAccess"})
public class MultiObjectUpdater {

	private final Object[] objects;

	/**
	 * Used internally to create a command chain. Not intended to be used by the user directly.
	 * Instead use {@link Spikeify#createAll(Object...)} method.
	 */
	public MultiObjectUpdater(boolean isTx, IAerospikeClient synClient, IAsyncClient asyncClient,
	                          RecordsCache recordsCache, boolean create, String defaultNamespace, Object... objects) {
		this.isTx = isTx;
		this.synClient = synClient;
		this.asyncClient = asyncClient;
		this.recordsCache = recordsCache;
		this.create = create;
		this.namespace = defaultNamespace;
		this.policy = new WritePolicy();
		this.objects = objects;
	}

	protected final String namespace;
	private final boolean isTx;
	protected final IAerospikeClient synClient;
	protected final IAsyncClient asyncClient;
	protected final RecordsCache recordsCache;
	protected final boolean create;
	protected WritePolicy policy;

	/**
	 * Sets the {@link WritePolicy} to be used when creating or updating the record in the database.
	 * <br/> Internally the 'sendKey' property of the policy will always be set to true.
	 * <br/> If this method is called within .transact() method then the 'generationPolicy' property will be set to GenerationPolicy.EXPECT_GEN_EQUAL
	 * <br/> The 'recordExistsAction' property is set accordingly depending if this is a create or update operation
	 *
	 * @param policy The policy.
	 */
	public MultiObjectUpdater policy(WritePolicy policy) {
		this.policy = policy;
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
				Object value = props.get(propName);
				if (value instanceof List<?>) {
					bins[position++] = new Bin(propName, (List) value);
				} else if (value instanceof Map<?, ?>) {
					bins[position++] = new Bin(propName, (Map) value);
				} else {
					bins[position++] = new Bin(propName, value);
				}
			}

			// must be set so that user key can be retrieved in queries
			this.policy.sendKey = true;

			Long expiration = mapper.getExpiration(object);
			if (expiration != null) {
				// Entities expiration:  Java time in milliseconds
				// Aerospike expiration: seconds from 1.1.2010 = 1262304000s.
				policy.expiration = (int) (expiration / 1000) - 1262304000; // todo fix Expiration
			}

			// enable version checking?
			if (isTx) {
				Integer generation = mapper.getGeneration(object);
				policy.generationPolicy = GenerationPolicy.EXPECT_GEN_EQUAL;
				if (generation != null) {
					policy.generation = generation;
				} else {
					throw new SpikeifyError("Error: missing @Generation field in class "+object.getClass()+
							". When using transact(..) you must have @Generation annotation on a field in the entity class.");
				}
			}

			if (create) {
				this.policy.recordExistsAction = RecordExistsAction.CREATE_ONLY;
			} else {
				this.policy.recordExistsAction = RecordExistsAction.UPDATE;
			}

			synClient.put(policy, key, bins);
		}

		return result;
	}

}
