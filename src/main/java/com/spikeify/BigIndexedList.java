package com.spikeify;

import com.aerospike.client.*;
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
	private int index = -1;
	private final int step = 100;

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

	/**
	 * Add object to the end of list
	 *
	 * @param value
	 */
	public void append(T value) {
		if (value == null) {
			throw new IllegalArgumentException("Can not add 'null' to BigList.");
		}
		index++;
		Map valMap = new HashMap(1);
		valMap.put("key", index);
		valMap.put("value", converter == null ? value : converter.fromField(value));
		inner.add(Value.get(valMap));
	}

	public void addAll(List<T> list) {

		if (list == null) {
			throw new IllegalArgumentException("Can not add 'null' to BigList.");
		}

		int size = list.size();

		List<Value> values = new ArrayList<>(list.size());
		int inLoop = 0;
		for (T value : list) {
			inLoop++;
			index++;
			Map<String, Object> valMap = new HashMap<>(1);
			valMap.put("key", index);
			valMap.put("value", converter == null ? value : converter.fromField(value));
			values.add(Value.get(valMap));
			if (inLoop % step == 0) {
				inner.add(Value.get(values)); // add in chunks
				values.clear();
			}
		}
		inner.add(Value.get(values));  // add remaining chunk
	}

	/**
	 * Removes values from start index to the end of list.
	 *
	 * @param from Starting index of trim (inclusive)
	 * @return Number of items removed
	 */
	public int trim(int from) {
		int to = inner.size() - 1;
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


	private Value toValue(T object) {
		return null;
	}

}
