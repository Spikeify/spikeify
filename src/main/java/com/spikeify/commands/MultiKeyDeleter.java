package com.spikeify.commands;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.async.IAsyncClient;
import com.aerospike.client.listener.DeleteListener;
import com.spikeify.*;
import com.spikeify.annotations.Namespace;
import com.spikeify.annotations.SetName;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A command chain for deleting multiple objects from database.
 * This class is not intended to be instantiated by user.
 */
@SuppressWarnings({"WeakerAccess", "unchecked"})
public class MultiKeyDeleter<T, K> {

	/*
	 * Used internally to create a command chain. Not intended to be used by the user directly.
	 * Use {@link Spikeify#deleteAll(Key...)} instead.
	 */


	public MultiKeyDeleter(IAerospikeClient synClient, IAsyncClient asyncClient,
	                       RecordsCache recordsCache, String defaultNamespace, K... keys) {
		this(null, synClient, asyncClient, recordsCache, defaultNamespace, keys);
	}

	public MultiKeyDeleter(Class<T> type, IAerospikeClient synClient, IAsyncClient asyncClient,
	                       RecordsCache recordsCache, String defaultNamespace, K... keys) {
		this.type = type;
		this.synClient = synClient;
		this.asyncClient = asyncClient;
		this.recordsCache = recordsCache;
		this.namespace = defaultNamespace;
		this.mapper = type != null ? MapperService.getMapper(type) : null;
		Class componentType = keys.getClass().getComponentType();
		if (componentType.equals(Key.class)) {
			this.keys = Arrays.asList((Key[]) keys);
			this.keyType = KeyType.KEY;
		} else if (componentType.equals(Long.class)) {
			this.longKeys = Arrays.asList((Long[]) keys);
			this.keyType = KeyType.LONG;
		} else if (componentType.equals(String.class)) {
			this.stringKeys = Arrays.asList((String[]) keys);
			this.keyType = KeyType.STRING;
		} else {
			throw new IllegalArgumentException("Error: unsupported key type '" + componentType + "'. Supported key types are Key, Long and String.");
		}

	}

	private ClassMapper mapper;
	protected final Class<T> type;
	protected KeyType keyType;
	private List<Key> keys = new ArrayList<>();
	private List<Long> longKeys;
	private List<String> stringKeys;
	protected String namespace;
	protected String setName;
	protected final IAerospikeClient synClient;
	protected final IAsyncClient asyncClient;
	private final RecordsCache recordsCache;

	/**
	 * Sets the Namespace. Overrides the default namespace and the namespace defined on the Class via {@link Namespace} annotation.
	 *
	 * @param namespace The namespace.
	 * @return multi key deleter instance
	 */
	public MultiKeyDeleter namespace(String namespace) {
		this.namespace = namespace;
		return this;
	}

	/**
	 * Sets the SetName. Overrides any SetName defined on the Class via {@link SetName} annotation.
	 *
	 * @param setName The name of the set.
	 * @return multi key deleter instance
	 */
	public MultiKeyDeleter setName(String setName) {
		this.setName = setName;
		return this;
	}

	/**
	 * Sets the keys of the records to be loaded.
	 *
	 * @param keys Array of keys
	 * @return multi key deleter instance
	 */
	public MultiKeyDeleter key(String... keys) {
		this.stringKeys = Arrays.asList(keys);
		this.longKeys = null;
		this.keys.clear();
		return this;
	}

	/**
	 * Sets the keys of the records to be loaded.
	 *
	 * @param keys Array of keys
	 * @return multi key deleter instance
	 */
	public MultiKeyDeleter key(Long... keys) {
		this.longKeys = Arrays.asList(keys);
		this.stringKeys = null;
		this.keys.clear();
		return this;
	}

	/**
	 * Sets the keys of the records to be loaded.
	 *
	 * @param keys Array of keys
	 * @return multi key deleter instance
	 */
	public MultiKeyDeleter key(Key... keys) {
		this.keys = Arrays.asList(keys);
		this.stringKeys = null;
		this.longKeys = null;
		return this;
	}

	protected void collectKeys() {

		// check if any Long or String keys were provided
		if (stringKeys != null) {
			for (String stringKey : stringKeys) {
				keys.add(new Key(getNamespace(), getSetName(), stringKey));
			}
		} else if (longKeys != null) {
			for (long longKey : longKeys) {
				keys.add(new Key(getNamespace(), getSetName(), longKey));
			}
		}
	}

	protected String getNamespace() {
		String useNamespace = namespace != null ? namespace : mapper.getNamespace();
		if (useNamespace == null) {
			throw new SpikeifyError("Namespace not set.");
		}
		return useNamespace;
	}

	protected String getSetName() {
		String setName = this.setName != null ? this.setName : (mapper != null ? mapper.getSetName() : null);
		if (setName == null) {
			throw new SpikeifyError("Set Name not provided.");
		}
		return setName;
	}

	/**
	 * Synchronously executes multiple delete commands.
	 *
	 * @return The Map of Key, Boolean pairs. The boolean tells whether object existed in the
	 * database prior to deletion.
	 */
	public Map<Key, Boolean> now() {

		collectKeys();
		Map<Key, Boolean> result = new HashMap<>(keys.size());

		for (Key key : keys) {
			recordsCache.remove(key);
			result.put(key, synClient.delete(null, key));
		}

		return result;
	}

	/**
	 * Asynchronously executes multiple delete commands.
	 *
	 * @return The {code Future} which you can use to check when deletion is finished.
	 * The resulting map of Key, Boolean pairs tells whether record existed in the database prior to deletion.
	 */
	public Future<Map<Key, Boolean>> async() {

		collectKeys();

		FutureTask<Map<Key, Boolean>> futureTask = new FutureTask<>(new Callable<Map<Key, Boolean>>() {
			@Override
			public Map<Key, Boolean> call() throws Exception {

				final AtomicReference<AerospikeException> failureException = new AtomicReference<>(null);
				final Map<Key, Boolean> results = new ConcurrentHashMap<>(keys.size());

				DeleteListener deleteListener = new DeleteListener() {
					@Override
					public void onSuccess(Key key, boolean existed) {
						results.put(key, existed);
					}

					@Override
					public void onFailure(AerospikeException exception) {
						// save exception to be re-thrown on calling thread
						failureException.set(exception);
					}
				};

				for (Key key : keys) {
					recordsCache.remove(key);
					asyncClient.delete(null, deleteListener, key);
				}

				// wait until all listeners are called and there is no failure
				while (results.size() < keys.size()) {

					// check for errors
					if (failureException.get() != null) {
						throw failureException.get(); // rethrow original exception
					}
				}

				return results;
			}
		});

		Executors.newCachedThreadPool().execute(futureTask);

		return futureTask;
	}

}
