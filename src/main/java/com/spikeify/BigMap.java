package com.spikeify;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.Key;
import com.aerospike.client.Value;
import com.aerospike.client.large.LargeList;
import com.aerospike.client.policy.RecordExistsAction;
import com.aerospike.client.policy.WritePolicy;
import com.spikeify.annotations.AsJson;
import com.spikeify.commands.InfoFetcher;

import java.lang.reflect.Field;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A type-safe wrapper for a native {@link LargeList}, exposing its interface as a Map.
 *
 * @param <K> the type of keys in this list
 * @param <V> the type of values in this list
 */
public class BigMap<K, V> extends BigDatatypeWrapper {

	protected Type keyType;
	protected Type valueType;

	/**
	 * Internal function - must be called before this LDT can be used.
	 * This function is called during a setup of mapping relation between this class and mapped field.
	 *
	 * @param client  The underlying Aerospike client
	 * @param key     The record key under which this list is saved in DB
	 * @param binName The bin name under which this list is saved in DB
	 * @param field   The field in the object to which this list is assigned
	 */
	public void init(AerospikeClient client, Key key, String binName, Field field) {
		this.keyType = TypeUtils.getBigMapKeyType(field);
		if (keyType != null) {
			Class keyClass = (Class) keyType;
			if (keyClass.isAnnotationPresent(AsJson.class)) {
				throw new SpikeifyError("Error: @AsJson is not supported on BigMap key type argument.");
			}
		}

		valueType = TypeUtils.getBigMapValueType(field);
		setConverterForValueType(valueType);

		WritePolicy wp = new WritePolicy();
		wp.recordExistsAction = RecordExistsAction.UPDATE;
		inner = new LargeList(client, wp, key, binName);

		if (!(new InfoFetcher(client).isUDFEnabled(key.namespace))) {
			throw new SpikeifyError("Error: LDT support not enabled on namespace '" + key.namespace + "'. Please add 'ldt-enabled true' to namespace section in your aerospike.conf file.");
		}

		try {
			inner.size();
		} catch (AerospikeException ae) {
			if (ae.getResultCode() == 1417) {
				isEmpty = true;
			}
		}
	}

	/**
	 * Returns true if this map contains a mapping for the specified key.
	 *
	 * @param key key whose presence in this map is to be tested
	 * @return if this map contains a mapping for the specified key
	 */
	public boolean containsKey(K key) {

		try {
			return inner.exists(Value.get(key));
		} catch (AerospikeException ae) {
			if (ae.getResultCode() == 1417) {
				isEmpty = true;
			}
		}
		return false;
	}

	/**
	 * Inserts a value into the map and associate it with provided key. If the map previously contained a mapping for
	 * the key, the old value is replaced by the specified value.
	 *
	 * @param key   key with which the specified value is to be associated
	 * @param value value to be associated with the specified key
	 */
	@SuppressWarnings("unchecked")
	public void put(K key, V value) {
		if (value == null) {
			throw new IllegalArgumentException("Can not add 'null' to BigList.");
		}

		Map<String, Object> valMap = new HashMap<>(2);
		valMap.put("key", key);
		valMap.put("value", converter == null ? value : converter.fromField(value));
		inner.update(Value.get(valMap));
		isEmpty = false;
	}

	/**
	 * Copies all of the mappings from the specified map to this map.
	 * This is equivalent as calling {@link #put(Object, Object)} for each mapping of specified map.
	 *
	 * @param map mappings to be stored in this map
	 */
	@SuppressWarnings("unchecked")
	public void putAll(Map<K, V> map) {
		if (map == null) {
			throw new IllegalArgumentException("Can not add 'null' to BigMap.");
		}

		if (map.isEmpty()) {
			return;
		}

		List<Value> values = new ArrayList<>(map.size());
		int inLoop = 0;

		for (Map.Entry<K, V> entry : map.entrySet()) {
			K key = entry.getKey();
			V value = entry.getValue();
			inLoop++;
			Map<String, Object> valMap = new HashMap<>(2);
			valMap.put("key", key);
			valMap.put("value", converter == null ? value : converter.fromField(value));
			values.add(Value.get(valMap));
			if (inLoop % step == 0) {
				addTransactionally(values); // add in chunks
				values.clear();
			}
		}
		if (!values.isEmpty()) {
			addTransactionally(values);  // add remaining chunk
		}
	}

	/**
	 * Returns the value to which the specified key is mapped,
	 * or {@code null} if this map contains no mapping for the key.
	 *
	 * @param key the key whose associated value is to be returned
	 * @return the value to which the specified key is mapped, or
	 * {@code null} if this map contains no mapping for the key
	 */
	@SuppressWarnings("unchecked")
	public V get(K key) {
		List found = inner.find(Value.get(key));

		if (found == null || found.isEmpty()) {
			return null;
		}

		if (found.size() > 1) {
			throw new IllegalStateException("List consistency error: list should only contain one value for each index.");
		}

		if (converter != null) {
			return (V) converter.fromProperty(((Map<String, Object>) found.get(0)).get("value"));
		} else {
			return (V) ((Map<String, Object>) found.get(0)).get("value");
		}
	}

	/**
	 * Returns a map of all keys, values.
	 *
	 * @return A map of all keys, values.
	 */
	@SuppressWarnings("unchecked")
	public Map<K, V> getAll() {

		List found = null;

		try {
			found = inner.scan();
		} catch (AerospikeException ae) {
			if (ae.getResultCode() == 1417) {
				return new HashMap<>(0);
			}
			throw ae;
		}

		if (found == null || found.isEmpty()) {
			return new HashMap<>(0);  // return empty list if no results
		}

		return toTypedResults(found);
	}

	/**
	 * Returns a range of values between from an to positions.
	 *
	 * @param from Starting position
	 * @param to   Ending position
	 * @return A map of key, values between requested key positions.
	 */
	@SuppressWarnings("unchecked")
	public Map<K, V> range(K from, K to) {

		Value fromValue = Value.get(from);
		Value toValue = Value.get(to);

		List found = null;
		try {
			found = inner.range(fromValue, toValue);
		} catch (AerospikeException ae) {
			if (ae.getResultCode() == 1417) {
				return new HashMap<>();
			}
			throw ae;
		}

		if (found == null || found.isEmpty()) {
			return new HashMap<>(0);  // return empty list if no results
		}

		return toTypedResults(found);
	}

	private Map<K, V> toTypedResults(List untyped) {
		Map<K, V> results = new HashMap<>(untyped.size());
		for (Object obj : untyped) {
			K key = (K) ((Map<String, Object>) obj).get("key");
			V val = (V) ((Map<String, Object>) obj).get("value");
			if (converter != null) {
				results.put(key, (V) converter.fromProperty(val));
			} else {
				results.put(key, val);
			}
		}
		return results;
	}

	/**
	 * Removes the mapping for a key from this map if it is present.
	 *
	 * @param key key whose mapping is to be removed from the map
	 */
	public void remove(K key) {
		inner.remove(Value.get(key));
	}

	/**
	 * Removes the mappings for a list keys from this map for each mapping that is present.
	 *
	 * @param keys a list of keys whose mapping is to be removed from the map
	 */
	public void remove(List<K> keys) {
		// no need to use Value.get(keys) as this is already done by the underlying client
		inner.remove(keys);
	}

}
