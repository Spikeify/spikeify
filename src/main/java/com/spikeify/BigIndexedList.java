package com.spikeify;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
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

public class BigIndexedList<T> extends BigDatatypeWrapper {

	private String binName;
	private Type valueType;
	private Converter converter;
	private LargeList inner;
	private final int step = 100;
	boolean isEmpty = true;

	public void init(AerospikeClient client, Key key, String binName, Field field) {
		this.binName = binName;
		this.valueType = TypeUtils.getBigListValueType(field);
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

	public int size() {
		return isEmpty ? 0 : inner.size();
	}

	/**
	 * Add object to the end of list
	 *
	 * @param value
	 */
	public void add(T value) {
		if (value == null) {
			throw new IllegalArgumentException("Can not add 'null' to BigList.");
		}
		int lastIndex = isEmpty ? 0 : inner.size();
		isEmpty = false;

		Map<String, Object> valMap = new HashMap<>(2);
		valMap.put("key", lastIndex);
		valMap.put("value", converter == null ? value : converter.fromField(value));
		inner.add(Value.get(valMap));
	}

	public void addAll(List<T> list) {

		if (list == null) {
			throw new IllegalArgumentException("Can not add 'null' to BigList.");
		}

		if (list.isEmpty()) {
			return;
		}

		List<Value> values = new ArrayList<>(list.size());
		int inLoop = 0;
		int lastIndex = isEmpty ? 0 : inner.size();
		isEmpty = false;

		for (T value : list) {
			inLoop++;
			Map<String, Object> valMap = new HashMap<>(2);
			valMap.put("key", lastIndex);
			valMap.put("value", converter == null ? value : converter.fromField(value));
			values.add(Value.get(valMap));
			lastIndex++;
			if (inLoop % step == 0) {
				inner.add(values); // add in chunks
				values.clear();
			}
		}
		if (!values.isEmpty()) {
			inner.add(values);  // add remaining chunk
		}
	}

	public T get(int index) {
		List found = inner.find(Value.get(index));

		if (found == null || found.isEmpty()) {
			return null;
		}

		if (found.size() > 1) {
			throw new IllegalStateException("List consistency error: list should only contain one value for each index.");
		}

		return (T) ((Map<String, Object>) found.get(0)).get("value");
	}

	/**
	 * Returns a range of values between from an to indexes.
	 *
	 * @param from Starting index
	 * @param to   Ending index
	 * @return
	 */
	public List<T> range(int from, int to) {

		if (to < from) {
			throw new IllegalArgumentException("Inverted range: 'to' is smaller then 'from'");
		}

		List found = inner.range(Value.get(from), Value.get(to));

		if (found == null || found.isEmpty()) {
			return new ArrayList<>(0);  // return empty list if no results
		}

		List<T> results = new ArrayList<>(found.size());
		for (Object obj : found) {
			T val = (T) ((Map<String, Object>) obj).get("value");
			results.add(val);
		}
		return results;
	}


	/**
	 * Removes values from start index to the end of list.
	 *
	 * @param from Starting index of trim (inclusive)
	 * @return Number of items removed
	 */
	public int trim(int from) {
		int to = inner.size() - 1;
		if (to < from) {
			throw new IndexOutOfBoundsException("Parameter 'from' is out of range.");
		}
		return inner.remove(Value.get(from), Value.get(to));
	}

	/**
	 * Does value exist?
	 *
	 * @param index index of the value to check existance for
	 */
	public boolean exists(int index) throws AerospikeException {
		if (index < 0) {
			return false;
		}
		return inner.exists(Value.get(index));
	}

	/**
	 * Is list empty?
	 *
	 * @return True if list is empty
	 */
	public boolean isEmpty() {
		return isEmpty ? true : inner.size() == 0;
	}

	/**
	 * Exposes inner config settings of the underlying LargeList
	 *
	 * @return Map of setting name ,setting value pairs
	 */
	public Map getInnerConfig() {
		return inner.getConfig();
	}

}
