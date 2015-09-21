package com.spikeify;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.Value;
import com.aerospike.client.large.LargeList;
import com.spikeify.annotations.AsJson;
import com.spikeify.converters.JsonConverter;

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

	private Type valueType;
	private Converter converter;
	private LargeList inner;

	private final int step = 100;

	/**
	 * Internal function - must be called before this map can be used.
	 * This function is called during a setup of mapping relation between this class and mapped field.
	 *
	 * @param client  The underlying Aerospike client
	 * @param key     The record key under which this list is saved in DB
	 * @param binName The bin name under which this list is saved in DB
	 * @param field   The field in the object to which this map is assigned
	 */
	void init(AerospikeClient client, Key key, String binName, Field field) {
		this.valueType = TypeUtils.getBigMapValueType(field);
		if (valueType != null) {
			Class valueClass = (Class) valueType;
			if (valueClass.isAnnotationPresent(AsJson.class)) {
				this.converter = new JsonConverter<>(valueClass);
			} else {
				this.converter = MapperUtils.findConverter(valueClass);
			}
		} else {
			this.converter = null;
		}
		this.inner = new LargeList(client, null, key, binName);
	}

	/**
	 * Inserts a value into the map and associate it with provided key. If the map previously contained a mapping for
	 * the key, the old value is replaced by the specified value.
	 *
	 * @param key   key with which the specified value is to be associated
	 * @param value value to be associated with the specified key
	 */
	public void put(K key, V value) {
		if (value == null) {
			throw new IllegalArgumentException("Can not add 'null' to BigList.");
		}

		Map<String, Object> valMap = new HashMap<>(2);
		valMap.put("key", key);
		valMap.put("value", converter == null ? value : converter.fromField(value));
		inner.add(Value.get(valMap));
	}

	/**
	 * Copies all of the mappings from the specified map to this map.
	 * This is equivalent as calling {@link #put(Object, Object)} for each mapping of specified map.
	 *
	 * @param map mappings to be stored in this map
	 */
	public void put(Map<K, V> map) {
		if (map == null) {
			throw new IllegalArgumentException("Can not add 'null' to BigList.");
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
				inner.add(values); // add in chunks
				values.clear();
			}
		}
		if (!values.isEmpty()) {
			inner.add(values);  // add remaining chunk
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
		inner.remove(keys);
	}

}
