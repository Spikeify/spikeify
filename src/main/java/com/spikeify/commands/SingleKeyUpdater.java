package com.spikeify.commands;

import com.aerospike.client.Bin;
import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.async.IAsyncClient;
import com.aerospike.client.policy.GenerationPolicy;
import com.aerospike.client.policy.RecordExistsAction;
import com.aerospike.client.policy.WritePolicy;
import com.spikeify.*;
import com.spikeify.annotations.Namespace;
import com.spikeify.annotations.SetName;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A command chain for creating or updating a single object in database.
 * This class is not intended to be instantiated by user.
 *
 * @param <T>
 */
@SuppressWarnings({"unchecked", "WeakerAccess"})
public class SingleKeyUpdater<T, K> {

	private final boolean isTx;

	/**
	 * Used internally to create a command chain. Not intended to be used by the user directly.
	 * Instead use {@link Spikeify#update(Key, Object)} or similar method.
	 */
	public SingleKeyUpdater(boolean isTx, IAerospikeClient synClient, IAsyncClient asyncClient,
	                        RecordsCache recordsCache, boolean create, String defaultNamespace, T object, K key) {
		this.isTx = isTx;
		this.synClient = synClient;
		this.asyncClient = asyncClient;
		this.recordsCache = recordsCache;
		this.create = create;
		this.namespace = defaultNamespace;
		this.object = object;
		this.policy = new WritePolicy();
		this.mapper = MapperService.getMapper((Class<T>) object.getClass());
		if (key.getClass().equals(Key.class)) {
			this.key = (Key) key;
			this.keyType = KeyType.KEY;
		} else if (key.getClass().equals(Long.class)) {
			this.longKey = (Long) key;
			this.keyType = KeyType.LONG;
		} else if (key.getClass().equals(String.class)) {
			this.stringKey = (String) key;
			this.keyType = KeyType.STRING;
		} else {
			throw new IllegalArgumentException("Error: unsupported key type. " +
					"SingleKeyUpdater constructor can only ba called with K as : Key, Long or String");
		}
	}

	private final T object;
	protected KeyType keyType;
	protected String namespace;
	protected String setName;
	protected String stringKey;
	protected Long longKey;
	protected Key key;
	protected final IAerospikeClient synClient;
	protected final IAsyncClient asyncClient;
	protected final RecordsCache recordsCache;
	protected final boolean create;
	protected WritePolicy policy;
	protected final ClassMapper<T> mapper;

	/**
	 * Sets the Namespace. Overrides the default namespace and the namespace defined on the Class via {@link Namespace} annotation.
	 *
	 * @param namespace The namespace.
	 */
	public SingleKeyUpdater<T, K> namespace(String namespace) {
		this.namespace = namespace;
		return this;
	}

	/**
	 * Sets the SetName. Overrides any SetName defined on the Class via {@link SetName} annotation.
	 *
	 * @param setName The name of the set.
	 */
	public SingleKeyUpdater<T, K> setName(String setName) {
		this.setName = setName;
		return this;
	}

	/**
	 * Sets the {@link WritePolicy} to be used when creating or updating the record in the database.
	 * <br/>Internally the 'sendKey' property of the policy will always be set to true.
	 * <br/> If this method is called within .transact() method then the 'generationPolicy' property will be set to GenerationPolicy.EXPECT_GEN_EQUAL
	 * <br/> The 'recordExistsAction' property is set accordingly depending if this is a create or update operation
	 *
	 * @param policy The policy.
	 */
	public SingleKeyUpdater<T, K> policy(WritePolicy policy) {
		this.policy = policy;
		this.policy.sendKey = true;
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
		return setName != null ? setName : mapper.getSetName();
	}

	/**
	 * Synchronously executes a single create or update command and returns the key of the record.
	 *
	 * @return The key of the record. The type of the key returned depends on the way this class is instantiated.
	 * It can be: {@link Key}, Long or String.
	 */
	@SuppressWarnings("ConstantConditions")
	public K now() {

		collectKeys();

		if (object == null) {
			throw new SpikeifyError("Error: parameter 'objects' must not be null or empty array");
		}

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
			policy.expiration = (int) (expiration / 1000) - 1262304000;
		}

		// is version checking necessary
		if (isTx) {
			Integer generation = mapper.getGeneration(object);
			policy.generationPolicy = GenerationPolicy.EXPECT_GEN_EQUAL;
			if (generation != null) {
				policy.generation = generation;
			} else {
				throw new SpikeifyError("Error: missing @Generation field in class " + object.getClass() +
						". When using transact(..) you must have @Generation annotation on a field in the entity class.");
			}
		}

		if (create) {
			this.policy.recordExistsAction = RecordExistsAction.CREATE_ONLY;
		} else {
			this.policy.recordExistsAction = RecordExistsAction.UPDATE;
		}

		synClient.put(policy, key, bins);

		switch (keyType) {
			case KEY:
				return (K) key;
			case LONG:
				Long longKey = key.userKey.toLong();
				return (K) longKey;
			case STRING:
				return (K) key.userKey.toString();
			default:
				throw new IllegalStateException("Error: unsupported ket type. Must be one of: Key, Long or String"); // should not happen
		}
	}
}
