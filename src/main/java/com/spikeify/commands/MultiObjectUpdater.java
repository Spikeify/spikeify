package com.spikeify.commands;

import com.aerospike.client.*;
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

	final Object[] objects;
	private boolean forceReplace = false;

	/**
	 * Used internally to create a command chain. Not intended to be used by the user directly.
	 * Instead use {@link Spikeify#createAll(Object...)} method.
	 *
	 * @param isTx             transaction enabled
	 * @param synClient        synchrone native aerospike client
	 * @param asyncClient      asynchrone native aerospike client
	 * @param recordsCache     cache
	 * @param create           true create record, false update record
	 * @param defaultNamespace default namespace
	 * @param objects          list of objects to be created or updated
	 */
	public MultiObjectUpdater(boolean isTx, IAerospikeClient synClient, IAsyncClient asyncClient,
	                          RecordsCache recordsCache, boolean create, String defaultNamespace, Object... objects) {
		this.isTx = isTx;
		this.synClient = synClient;
		this.asyncClient = asyncClient;
		this.recordsCache = recordsCache;
		this.create = create;
		this.namespace = defaultNamespace;
		this.objects = objects;
	}

	protected final String namespace;
	private final boolean isTx;
	protected final IAerospikeClient synClient;
	protected final IAsyncClient asyncClient;
	protected final RecordsCache recordsCache;
	protected final boolean create;
	protected WritePolicy overridePolicy;

	/**
	 * Sets the {@link WritePolicy} to be used when creating or updating the record in the database.
	 * Internally the 'sendKey' property of the policy will always be set to true.
	 * If this method is called within .transact() method then the 'generationPolicy' property will be set to GenerationPolicy.EXPECT_GEN_EQUAL
	 * The 'recordExistsAction' property is set accordingly depending if this is a create or update operation
	 *
	 * @param policy The policy.
	 * @return multi object updater instance
	 */
	public MultiObjectUpdater policy(WritePolicy policy) {
		this.overridePolicy = policy;
		return this;
	}

	/**
	 * Sets updater to skip cache check for object changes. This causes that all
	 * object properties will be written to database. It also deletes previous saved
	 * properties in database and now not mapped to object.
	 *
	 * @return multi object updater instance
	 */
	public MultiObjectUpdater forceReplace() {
		this.forceReplace = true;
		return this;
	}

	private WritePolicy getPolicy() {
		WritePolicy writePolicy = overridePolicy != null ? overridePolicy : new WritePolicy(synClient.getWritePolicyDefault());
		// must be set in order for later queries to return record keys
		writePolicy.sendKey = true;

		writePolicy.recordExistsAction = create ? RecordExistsAction.CREATE_ONLY : forceReplace ? RecordExistsAction.REPLACE : RecordExistsAction.UPDATE;

		return writePolicy;
	}

	protected List<Key> collectKeys() {

		List<Key> keys = new ArrayList<>(objects.length);

		// check if entities have @UserKey entity
		// keys.clear();
		for (Object object : objects) {

			if (create && IdGenerator.shouldGenerateId(object)) {
				IdGenerator.generateId(object);
			}

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

		WritePolicy usePolicy = getPolicy();
		boolean isReplace = usePolicy.recordExistsAction == RecordExistsAction.REPLACE;

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
			Set<String> changedProps = recordsCache.update(key, props, forceReplace);

			List<Bin> bins = new ArrayList<>();
			boolean nonNullField = false;
			for (String propName : changedProps) {
				Object value = props.get(propName);
				if (value == null) {
					if (!isReplace) {
						bins.add(Bin.asNull(propName));
					}
				} else if (value instanceof List<?>) {
					bins.add(new Bin(propName, (List) value));
					nonNullField = true;
				} else if (value instanceof Map<?, ?>) {
					bins.add(new Bin(propName, (Map) value));
					nonNullField = true;
				} else {
					bins.add(new Bin(propName, value));
					nonNullField = true;
				}
			}

			// must be set so that user key can be retrieved in queries
			usePolicy.sendKey = true;

			Integer recordExpiration = mapper.getRecordExpiration(object);
			if (recordExpiration != null) {
				usePolicy.expiration = recordExpiration;
			}

			// enable version checking?
			if (isTx) {
				Integer generation = mapper.getGeneration(object);
				usePolicy.generationPolicy = GenerationPolicy.EXPECT_GEN_EQUAL;
				if (generation != null) {
					usePolicy.generation = generation;
				} else {
					throw new SpikeifyError("Error: missing @Generation field in class " + object.getClass() +
							". When using transact(..) you must have @Generation annotation on a field in the entity class.");
				}
			}

			if (!nonNullField && props.size() == changedProps.size()) {
				throw new SpikeifyError("Error: cannot create object with no writable properties. " +
						"At least one object property other then UserKey must be different from NULL.");
			}

			if (create && IdGenerator.shouldGenerateId(object)) {
				// retry 5 times in case same id is generated ...
				for (int count = 1; count <= SingleObjectUpdater.MAX_CREATE_GENERATE_RETRIES; count++) {
					try {
						synClient.put(usePolicy, key, bins.toArray(new Bin[bins.size()]));
						break;
					} catch (AerospikeException e) {
						// let's retry or not ?
						if (e.getResultCode() != ResultCode.KEY_EXISTS_ERROR ||
								SingleObjectUpdater.MAX_CREATE_GENERATE_RETRIES == count) {
							throw e;
						}
						// regenerate key ...
						IdGenerator.generateId(object);
						key = SingleObjectUpdater.collectKey(object, namespace);
					}
				}
			} else {
				// if we are updating an existing record and no bins are to be updated,
				// then just touch the entity to update expiry timestamp
				if (!create && bins.isEmpty()) {
					if(recordExpiration != null){
						synClient.touch(usePolicy, key);
					}
				} else {
					synClient.put(usePolicy, key, bins.toArray(new Bin[bins.size()]));
				}
			}

			// set LDT fields
			mapper.setBigDatatypeFields(object, synClient, key);
		}

		return result;
	}

}
