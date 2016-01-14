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
import java.util.List;
import java.util.Map;

public abstract class BigDatatypeWrapper {

	protected Converter converter;
	protected LargeList inner;
	protected boolean isEmpty = false;
	protected final int step = 1000;

	public boolean isInitialized() {
		return inner != null;
	}

	protected void setConverterForValueType(Type valueType) {
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
	}

	protected void addTransactionally(List<Value> values) {
		int retries = 10;

		// retry loop in case of clashing indexes
		while (retries > 0) {
			try {
				inner.update(values);
				isEmpty = false;
				return;
			} catch (AerospikeException ae) {
				if (ae.getResultCode() == 1402) {
					retries--;
				} else {
					throw ae;
				}
			}
		}
	}

	/**
	 * Internal function - must be called before this LDT can be used.
	 * This function is called during a setup of mapping relation between this class and mapped field.
	 *
	 * @param client  The underlying Aerospike client
	 * @param key     The record key under which this list is saved in DB
	 * @param binName The bin name under which this list is saved in DB
	 * @param field   The field in the object to which this list is assigned
	 */
	public abstract void init(AerospikeClient client, Key key, String binName, Field field);

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

	public Converter getConverter() {
		return converter;
	}

}
