package com.spikeify;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.Key;
import com.aerospike.client.Value;
import com.aerospike.client.large.LargeList;
import com.spikeify.annotations.AsJson;
import com.spikeify.commands.InfoFetcher;
import com.spikeify.converters.JsonConverter;

import java.lang.reflect.Field;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A type-safe wrapper for a native {@link LargeList}, exposing its interface as a List.
 *
 * @param <T> the type of elements in this list
 */
public class BigIndexedList<T> extends BigDatatypeWrapper {

	private Type valueType;
	private Converter converter;
	private LargeList inner;
	private final int step = 100;
	private boolean isEmpty = true;

	/**
	 * Internal function - must be called before this list can be used.
	 * This function is called during a setup of mapping relation between this class and mapped field.
	 *
	 * @param client  The underlying Aerospike client
	 * @param key     The record key under which this list is saved in DB
	 * @param binName The bin name under which this list is saved in DB
	 * @param field   The field in the object to which this list is assigned
	 */
	@SuppressWarnings("unchecked")
	void init(AerospikeClient client, Key key, String binName, Field field) {
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

		if (!(new InfoFetcher(client).isUDFEnabled(key.namespace))) {
			throw new SpikeifyError("Error: LDT support not enabled on namespace '" + key.namespace + "'. Please add 'ldt-enabled true' to namespace section in your aerospike.conf file.");
		}

		if (client.get(null, key, binName) == null) {
			isEmpty = true;
		}
	}

	/**
	 * Size of list, i.e. a number of elements in the list
	 *
	 * @return Number of elements in the list
	 */
	public int size() {
		try {
			return isEmpty ? 0 : inner.size();
		} catch (AerospikeException ae) {
			if (ae.getResultCode() == 1417) {
				return 0;
			}
			throw ae;
		}
	}

	/**
	 * Add object to the end of list
	 *
	 * @param value Value
	 */
	@SuppressWarnings("unchecked")
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

	/**
	 * Add a List of objects to the end of list
	 *
	 * @param list A list of object to be added to the end of list.
	 */
	@SuppressWarnings("unchecked")
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

	/**
	 * Returns a value at given position in the list.
	 *
	 * @param index Index
	 * @return A value at requested indexes.
	 */
	@SuppressWarnings("unchecked")
	public T get(int index) {

		try {

			List found = inner.find(Value.get(index));

			if (found == null || found.isEmpty()) {
				return null;
			}

			if (found.size() > 1) {
				throw new IllegalStateException("List consistency error: list should only contain one value for each index.");
			}

			if (converter != null) {
				return (T) converter.fromProperty(((Map<String, Object>) found.get(0)).get("value"));
			} else {
				return (T) ((Map<String, Object>) found.get(0)).get("value");
			}

		} catch (AerospikeException ae) {
			if (ae.getResultCode() == 1417) {
				return null;
			}
			throw ae;
		}
	}

	/**
	 * Returns a range of values between from an to positions.
	 *
	 * @param from Starting position
	 * @param to   Ending position
	 * @return A list of values between requested positions.
	 */
	@SuppressWarnings("unchecked")
	public List<T> range(int from, int to) {

		if (to < from) {
			throw new IllegalArgumentException("Inverted range: 'to' is smaller then 'from'");
		}


		List found = null;

		try {
			found = inner.range(Value.get(from), Value.get(to));
		} catch (AerospikeException ae) {
			if (ae.getResultCode() == 1417) {
				return new ArrayList<>();
			}
			throw ae;
		}

		if (found == null || found.isEmpty()) {
			return new ArrayList<>(0);  // return empty list if no results
		}

		List<T> results = new ArrayList<>(found.size());
		for (Object obj : found) {
			Object val = ((Map<String, Object>) obj).get("value");
			if (converter != null) {
				results.add((T) converter.fromProperty(val));
			} else {
				results.add((T) val);
			}

		}
		return results;
	}


	/**
	 * Removes values from start position to the end of list.
	 *
	 * @param from Starting index of trim (inclusive)
	 * @return Number of items removed
	 */
	public int trim(int from) {

		try {
			int to = inner.size() - 1;
			if (to < from) {
				throw new IndexOutOfBoundsException("Parameter 'from' is out of range.");
			}
			return inner.remove(Value.get(from), Value.get(to));
		} catch (AerospikeException ae) {
			if (ae.getResultCode() == 1417) {
				return 0;
			}
			throw ae;
		}

	}

	/**
	 * Does value exist?
	 *
	 * @param index index of the value to check existance for
	 * @return True if  value at given index exists
	 */
	public boolean exists(int index) throws AerospikeException {
		try {
			return index >= 0 && inner.exists(Value.get(index));
		} catch (AerospikeException ae) {
			if (ae.getResultCode() == 1417) {
				return false;
			}
			throw ae;
		}
	}

	/**
	 * Is list empty?
	 *
	 * @return True if list is empty
	 */
	public boolean isEmpty() {
		try {
			return isEmpty || inner.size() == 0;
		} catch (AerospikeException ae) {
			if (ae.getResultCode() == 1417) {
				return true;
			}
			throw ae;
		}
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
