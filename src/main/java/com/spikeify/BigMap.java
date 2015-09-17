package com.spikeify;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.Value;
import com.aerospike.client.large.LargeList;
import com.spikeify.annotations.AsJson;
import com.spikeify.converters.JsonConverter;

import java.lang.reflect.Field;
import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BigMap<K, V> extends BigDatatypeWrapper {


	private String binName;
	private Type keyType;
	private Type valueType;
	private Converter converter;
	private LargeList inner;

	@Override
	public void init(AerospikeClient client, Key key, String binName, Field field) {
		this.binName = binName;
		this.keyType = TypeUtils.getBigMapKeyType(field);
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

	public void put(K key, V value) {
		Map valMap = new HashMap(1);
		valMap.put("key", key);
		valMap.put("value", converter == null ? value : converter.fromField(value));
		inner.add(Value.get(valMap));
	}

	public void put(Map<K, V> map) {
		if (converter == null) {
			inner.add(Value.get(map));
		} else {
			Map valMap = new HashMap(map.size());
			for (K key : map.keySet()) {
				valMap.put("key", key);
				valMap.put("value", converter == null ? map.get(key) : converter.fromField(map.get(key)));
			}
			inner.add(Value.get(valMap));
		}
	}

	public void remove(K key) {
		inner.remove(Value.get(key));
	}

	public void remove(List<K> key) {
		inner.remove(key);
	}


}
