package com.spikeify;

import com.aerospike.client.*;
import com.aerospike.client.async.IAsyncClient;
import com.spikeify.commands.*;

import java.util.ConcurrentModificationException;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

@SuppressWarnings("unchecked")
public class SpikeifyImpl<P extends Spikeify> implements Spikeify {

	private static final Logger log = Logger.getLogger(SpikeifyImpl.class.getSimpleName());

	public SpikeifyImpl(IAerospikeClient synClient, IAsyncClient asyncClient, ClassConstructor classConstructor, String defaultNamespace) {
		this.synClient = synClient;
		this.asyncClient = asyncClient;
		this.classConstructor = classConstructor;
		this.defaultNamespace = defaultNamespace;
	}

	private final IAerospikeClient synClient;
	private final IAsyncClient asyncClient;
	private final ClassConstructor classConstructor;
	private final String defaultNamespace;

	private final ThreadLocal<Boolean> tlTransaction = new ThreadLocal<>();
	private final RecordsCache recordsCache = new RecordsCache();

	@Override
	public InfoFetcher info() {
		return new InfoFetcher(synClient);
	}

	@Override
	public String getNamespace() {
		return defaultNamespace;
	}

	@Override
	public IAerospikeClient getClient() {
		return synClient;
	}

	@Override
	public <E> SingleLoader<E> get(Class<E> type) {
		return new SingleLoader<>(type, synClient, asyncClient, classConstructor, recordsCache, defaultNamespace);
	}

	@Override
	public <E> MultiLoader<E, Key> getAll(Class<E> type, Key... keys) {
		return new MultiLoader<>(type, synClient, asyncClient, classConstructor, recordsCache, defaultNamespace, keys);
	}

	@Override
	public <E> MultiLoader<E, Long> getAll(Class<E> type, Long... keys) {
		return new MultiLoader<>(type, synClient, asyncClient, classConstructor, recordsCache, defaultNamespace, keys);
	}

	@Override
	public <E> MultiLoader<E, String> getAll(Class<E> type, String... keys) {
		return new MultiLoader<>(type, synClient, asyncClient, classConstructor, recordsCache, defaultNamespace, keys);
	}

	@Override
	public <T> ScanLoader<T> scanAll(Class<T> type) {
		return new ScanLoader<>(type, synClient, classConstructor, recordsCache, defaultNamespace);
	}

	@Override
	public <T> SingleKeyUpdater<T, Key> create(Key key, T object) {
		return new SingleKeyUpdater<>(false, synClient, asyncClient, recordsCache, true, defaultNamespace, object, key);
	}

	@Override
	public <T> SingleKeyUpdater<T, Long> create(Long userKey, T object) {
		return new SingleKeyUpdater<>(false, synClient, asyncClient, recordsCache, true, defaultNamespace, object, userKey);
	}

	@Override
	public <T> SingleKeyUpdater<T, String> create(String userKey, T object) {
		return new SingleKeyUpdater<>(false, synClient, asyncClient, recordsCache, true, defaultNamespace, object, userKey);
	}

	@Override
	public <T> SingleObjectUpdater<T> create(T object) {

		if (object == null) {
			throw new SpikeifyError("Error: parameter 'object' must not be null.");
		}
		boolean isTx = Boolean.TRUE.equals(tlTransaction.get());

		return new SingleObjectUpdater<>(isTx, object.getClass(), synClient, asyncClient,
				recordsCache, true, defaultNamespace, object);
	}

	@Override
	public MultiKeyUpdater createAll(Key[] keys, Object[] objects) {
		if (objects == null || objects.length == 0) {
			throw new SpikeifyError("Error: parameter 'objects' must not be null or empty array.");
		}
		if (keys == null || keys.length == 0) {
			throw new SpikeifyError("Error: parameter 'objects' must not be null or empty array.");
		}
		if (keys.length != objects.length) {
			throw new SpikeifyError("Error: array 'objects' must be same length as 'keys' array");
		}
		boolean isTx = Boolean.TRUE.equals(tlTransaction.get());
		return new MultiKeyUpdater(isTx, synClient, asyncClient, recordsCache, true, defaultNamespace, keys, objects);
	}

	@Override
	public MultiKeyUpdater createAll(Long[] keys, Object[] objects) {
		if (objects == null || objects.length == 0) {
			throw new SpikeifyError("Error: parameter 'objects' must not be null or empty array.");
		}
		if (keys == null || keys.length == 0) {
			throw new SpikeifyError("Error: parameter 'objects' must not be null or empty array.");
		}
		if (keys.length != objects.length) {
			throw new SpikeifyError("Error: array 'objects' must be same length as 'keys' array");
		}
		boolean isTx = Boolean.TRUE.equals(tlTransaction.get());
		return new MultiKeyUpdater(isTx, synClient, asyncClient, recordsCache, true, defaultNamespace, keys, objects);
	}

	@Override
	public MultiKeyUpdater createAll(String[] keys, Object[] objects) {
		if (objects == null || objects.length == 0) {
			throw new SpikeifyError("Error: parameter 'objects' must not be null or empty array.");
		}
		if (keys == null || keys.length == 0) {
			throw new SpikeifyError("Error: parameter 'objects' must not be null or empty array.");
		}
		if (keys.length != objects.length) {
			throw new SpikeifyError("Error: array 'objects' must be same length as 'keys' array");
		}
		boolean isTx = Boolean.TRUE.equals(tlTransaction.get());
		return new MultiKeyUpdater(isTx, synClient, asyncClient, recordsCache, true, defaultNamespace, keys, objects);
	}

	@Override
	public MultiObjectUpdater createAll(Object... objects) {

		if (objects == null || objects.length == 0) {
			throw new SpikeifyError("Error: parameter 'objects' must not be null or empty array.");
		}
		boolean isTx = Boolean.TRUE.equals(tlTransaction.get());
		return new MultiObjectUpdater(isTx, synClient, asyncClient,
				recordsCache, true, defaultNamespace, objects);
	}

	@Override
	public <T> SingleObjectUpdater<T> update(T object) {
		if (object == null) {
			throw new SpikeifyError("Error: parameter 'object' must not be null.");
		}
		boolean isTx = Boolean.TRUE.equals(tlTransaction.get());
		return new SingleObjectUpdater<>(isTx, object.getClass(), synClient, asyncClient,
				recordsCache, false, defaultNamespace, object);
	}

	@Override
	public <T> SingleKeyUpdater<T, Key> update(Key key, T object) {
		boolean isTx = Boolean.TRUE.equals(tlTransaction.get());
		return new SingleKeyUpdater<>(isTx, synClient, asyncClient, recordsCache, false, defaultNamespace, object, key);
	}

	@Override
	public <T> SingleKeyUpdater<T, Long> update(Long userKey, T object) {
		boolean isTx = Boolean.TRUE.equals(tlTransaction.get());
		return new SingleKeyUpdater<>(isTx, synClient, asyncClient, recordsCache, false, defaultNamespace, object, userKey);
	}

	@Override
	public <T> SingleKeyUpdater<T, String> update(String userKey, T object) {
		boolean isTx = Boolean.TRUE.equals(tlTransaction.get());
		return new SingleKeyUpdater<>(isTx, synClient, asyncClient, recordsCache, false, defaultNamespace, object, userKey);
	}

	@Override
	public MultiObjectUpdater updateAll(Object... objects) {
		if (objects == null || objects.length == 0) {
			throw new SpikeifyError("Error: parameter 'objects' must not be null or empty array");
		}
		boolean isTx = Boolean.TRUE.equals(tlTransaction.get());
		return new MultiObjectUpdater(isTx, synClient, asyncClient, recordsCache, false, defaultNamespace, objects);
	}

	@Override
	public MultiKeyUpdater updateAll(Key[] keys, Object[] objects) {
		if (objects == null || objects.length == 0) {
			throw new SpikeifyError("Error: parameter 'objects' must not be null or empty array.");
		}
		if (keys == null || keys.length == 0) {
			throw new SpikeifyError("Error: parameter 'objects' must not be null or empty array.");
		}
		if (keys.length != objects.length) {
			throw new SpikeifyError("Error: array 'objects' must be same length as 'keys' array");
		}
		boolean isTx = Boolean.TRUE.equals(tlTransaction.get());
		return new MultiKeyUpdater(isTx, synClient, asyncClient, recordsCache, false, defaultNamespace, keys, objects);
	}

	@Override
	public MultiKeyUpdater updateAll(Long[] keys, Object[] objects) {
		if (objects == null || objects.length == 0) {
			throw new SpikeifyError("Error: parameter 'objects' must not be null or empty array.");
		}
		if (keys == null || keys.length == 0) {
			throw new SpikeifyError("Error: parameter 'objects' must not be null or empty array.");
		}
		if (keys.length != objects.length) {
			throw new SpikeifyError("Error: array 'objects' must be same length as 'keys' array");
		}
		boolean isTx = Boolean.TRUE.equals(tlTransaction.get());
		return new MultiKeyUpdater(isTx, synClient, asyncClient, recordsCache, false, defaultNamespace, keys, objects);
	}

	@Override
	public MultiKeyUpdater updateAll(String[] keys, Object[] objects) {
		if (objects == null || objects.length == 0) {
			throw new SpikeifyError("Error: parameter 'objects' must not be null or empty array.");
		}
		if (keys == null || keys.length == 0) {
			throw new SpikeifyError("Error: parameter 'objects' must not be null or empty array.");
		}
		if (keys.length != objects.length) {
			throw new SpikeifyError("Error: array 'objects' must be same length as 'keys' array");
		}
		boolean isTx = Boolean.TRUE.equals(tlTransaction.get());
		return new MultiKeyUpdater(isTx, synClient, asyncClient, recordsCache, false, defaultNamespace, keys, objects);
	}

	public SingleObjectDeleter delete(Object object) {
		return new SingleObjectDeleter(synClient, asyncClient, recordsCache, defaultNamespace, object);
	}

	@Override
	public <T> MultiObjectDeleter<T> deleteAll(T... objects) {
		return new MultiObjectDeleter<>(synClient, asyncClient, recordsCache, defaultNamespace, objects);
	}

	@Override
	public SingleKeyDeleter delete(Key key) {
		return new SingleKeyDeleter(synClient, asyncClient, recordsCache, defaultNamespace, key);
	}

	@Override
	public SingleKeyDeleter delete(Long userKey) {
		return new SingleKeyDeleter(synClient, asyncClient, recordsCache, defaultNamespace, userKey);
	}

	@Override
	public SingleKeyDeleter delete(String userKey) {
		return new SingleKeyDeleter(synClient, asyncClient, recordsCache, defaultNamespace, userKey);
	}

	@Override
	public MultiKeyDeleter deleteAll(Key... keys) {
		return new MultiKeyDeleter(synClient, asyncClient, recordsCache, defaultNamespace, keys);
	}

	@Override
	public MultiKeyDeleter deleteAll(Long... keys) {
		return new MultiKeyDeleter(synClient, asyncClient, recordsCache, defaultNamespace, keys);
	}

	@Override
	public MultiKeyDeleter deleteAll(String... keys) {
		return new MultiKeyDeleter(synClient, asyncClient, recordsCache, defaultNamespace, keys);
	}

	@Override
	public <E> MultiKeyDeleter<E, Key> deleteAll(Class<E> clazz, Key... keys) {
		return new MultiKeyDeleter<>(clazz, synClient, asyncClient, recordsCache, defaultNamespace, keys);
	}

	@Override
	public <E> MultiKeyDeleter<E, Long> deleteAll(Class<E> clazz, Long... keys) {
		return new MultiKeyDeleter<>(clazz, synClient, asyncClient, recordsCache, defaultNamespace, keys);
	}

	@Override
	public <E> MultiKeyDeleter<E, String> deleteAll(Class<E> clazz, String... keys) {
		return new MultiKeyDeleter<>(clazz, synClient, asyncClient, recordsCache, defaultNamespace, keys);
	}

	@Override
	public <T> Scanner<T> query(Class<T> type) {
		return new Scanner<>(type, synClient, asyncClient, classConstructor, recordsCache, defaultNamespace);
	}


	@Override
	public SingleKeyCommander command(Class type) {
		return new SingleKeyCommander(type, synClient, asyncClient, classConstructor, recordsCache, defaultNamespace);
	}

	@Override
	public boolean exists(Key key) {
		return synClient.exists(null, key);
	}

	@Override
	public Map<Key, Boolean> exist(Key... keys) {
		boolean[] exist = synClient.exists(null, keys);

		Map<Key, Boolean> results = new HashMap<>(keys.length);
		for (int i = 0; i < keys.length; i++) {
			results.put(keys[i], exist[i]);
		}
		return results;
	}

	@Override
	public boolean exists(Class type, String id) {
		ClassMapper mapper = MapperService.getMapper(type);
		return synClient.exists(null, new Key(getNamespace(mapper), getSetName(mapper), id));
	}

	@Override
	public Map<String, Boolean> exist(Class type, String... ids) {
		ClassMapper mapper = MapperService.getMapper(type);
		String namespace = getNamespace(mapper);
		String setName = getSetName(mapper);
		Key[] keys = new Key[ids.length];
		for (int i = 0; i < ids.length; i++) {
			String id = ids[i];
			keys[i] = new Key(namespace, setName, id);
		}
		boolean[] exist = synClient.exists(null, keys);

		Map<String, Boolean> results = new HashMap<>(keys.length);
		for (int i = 0; i < ids.length; i++) {
			results.put(ids[i], exist[i]);
		}
		return results;
	}

	@Override
	public boolean exists(Class type, Long id) {
		ClassMapper mapper = MapperService.getMapper(type);
		Key key = new Key(getNamespace(mapper), getSetName(mapper), id);
		return synClient.exists(null, key);
	}

	@Override
	public Map<Long, Boolean> exist(Class type, Long... ids) {
		ClassMapper mapper = MapperService.getMapper(type);
		String namespace = getNamespace(mapper);
		String setName = getSetName(mapper);
		Key[] keys = new Key[ids.length];
		for (int i = 0; i < ids.length; i++) {
			Long id = ids[i];
			keys[i] = new Key(namespace, setName, id);
		}
		boolean[] exist = synClient.exists(null, keys);

		Map<Long, Boolean> results = new HashMap<>(keys.length);
		for (int i = 0; i < ids.length; i++) {
			results.put(ids[i], exist[i]);
		}
		return results;
	}

	private String getNamespace(ClassMapper mapper) {
		String useNamespace = mapper.getNamespace();
		useNamespace = useNamespace == null ? defaultNamespace : useNamespace;
		if (useNamespace == null) {
			throw new SpikeifyError("Namespace not set. Use annotations or set a default namespace.");
		}
		return useNamespace;
	}

	private String getSetName(ClassMapper mapper) {
		String setName = mapper.getSetName();
		if (setName == null) {
			throw new SpikeifyError("Set Name not provided.");
		}
		return setName;
	}

	@Override
	public <T> T map(Class<T> type, Key key, Record record) {

		if (record == null) {
			return null;
		}

		T object = classConstructor.construct(type);

		ClassMapper<T> mapper = MapperService.getMapper(type);

		MapperService.map(mapper, key, record, object);

		// set LDT wrappers
		mapper.setBigDatatypeFields(object, synClient, key);

		return object;
	}

	@Override
	public <T> Key key(T object) {
		if (object == null) {
			return null;
		}

		ClassMapper<T> mapper = MapperService.getMapper((Class<T>) object.getClass());

		ObjectMetadata meta = mapper.getRequiredMetadata(object, defaultNamespace);
		if (meta.userKeyString != null) {
			return new Key(meta.namespace, meta.setName, meta.userKeyString);
		} else {
			return new Key(meta.namespace, meta.setName, meta.userKeyLong);
		}
	}

	@Override
	public void truncateSet(String namespace, String setName) {
		Truncater.truncateSet(namespace, setName, synClient);
	}

	@Override
	public void truncateSet(Class type) {
		ClassMapper mapper = MapperService.getMapper(type);
		String ns = mapper.getNamespace() != null ? mapper.getNamespace() : defaultNamespace;
		if (ns == null) {
			throw new SpikeifyError("Error: defaultNamespace not defined.");
		}
		String setName = mapper.getSetName();
		if (setName == null) {
			throw new SpikeifyError("Error: @SetName annotation not defined on class " + type.getName());
		}
		Truncater.truncateSet(ns, setName, synClient);
	}

	@Override
	public void truncateNamespace(String namespace) {
		Truncater.truncateNamespace(namespace, synClient);
	}

	@Override
	public void dropIndexesInNamespace(String namespace) {
		Truncater.dropNamespaceIndexes(namespace, asyncClient);
	}

	@Override
	public void dropIndexesInNamespace(String namespace, String setName) {
		Truncater.dropSetIndexes(namespace, setName, asyncClient);
	}

	@Override
	public <R> R transact(int limitTries, Work<R> work) {

		// log.info("Transaction_started");

		Boolean threadTx = tlTransaction.get();

		// are we already inside a transaction
		if (Boolean.TRUE.equals(threadTx)) {
			throw new SpikeifyError("Error: transaction already in progress. Nesting transactions is not supported.");
		}
		tlTransaction.set(Boolean.TRUE);

		int retries = 0;
		while (true) {
			try {
				R result = work.run();

				tlTransaction.remove();

				// success - exit the transact wrapper
				return result;
			} catch (AerospikeException ex) {

				// only retry in case of generation error, which happens in case of concurrent updates
				if (ex.getResultCode() == ResultCode.GENERATION_ERROR) {
					if (retries++ < limitTries) {
						if (log.isLoggable(Level.FINEST) && retries >= (limitTries - 3)) {
							log.warning("Optimistic concurrency failure for " + work + " (retrying:" + retries + "): " + ex);
						}
					} else {
						if (log.isLoggable(Level.FINEST)) {//&& retries >= (limitTries - 3)) {
							log.warning("Optimistic concurrency failure for " + work + ": Could not update record.");
						}
						tlTransaction.remove();
						throw new ConcurrentModificationException("Error: too much contention. Record could not be updated.");
					}
				} else {
					throw new AerospikeException(ex.getResultCode(), ex.getMessage());
				}
			} catch (Exception e) {

				log.log(Level.SEVERE, "Worker exception, transaction aborted!", e);
				tlTransaction.remove();
				throw e;
			}

			try {
				Thread.sleep((long) (10 + (Math.random() * 10 * retries))); // sleep for random time to give competing thread chance to finish job
			} catch (InterruptedException e) {
				log.log(Level.SEVERE, "Thread.sleep() InterruptedException: ", e);
			}
		}

	}
}
