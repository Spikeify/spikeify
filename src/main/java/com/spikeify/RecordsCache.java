package com.spikeify;

import com.aerospike.client.Key;

import java.util.*;

/**
 * A cache of records, used to calculate changes between loaded and saved objects.
 * This is needed because Aerospike encourages saving only changed bins (records).
 */
public class RecordsCache {

	private final ThreadLocal<Map<Key/*key of mapped object*/, Map<String, Long>/*record properties*/>> cache =
			new ThreadLocal<Map<Key/*key of mapped object*/, Map<String, Long>/*record properties*/>>() {
				@Override
				protected Map<Key, Map<String, Long>> initialValue() {
					return new HashMap<>();
				}
			};

//	private static Map<Key/*key of mapped object*/, Map<String, String>/*record properties*/> propertiesCache = new HashMap<>();

	/**
	 * Insert a set of properties linked to a Key
	 *
	 * @param key        The Key
	 * @param properties Object properties
	 */
	public void insert(Key key, Map<String, Object> properties) {

		Map<String, Long> propertiesKeys = new HashMap<>(properties.size());
		for (Map.Entry<String, Object> prop : properties.entrySet()) {
			Object property = prop.getValue();
			if (property != null) {
				Long propertyHash = getPropertyHash(property);
				if (propertyHash != null) {
					propertiesKeys.put(prop.getKey(), propertyHash);
				}
			}
		}

		cache.get().put(key, propertiesKeys);
	}

	/**
	 * Remove a set of properties linked to a Key
	 *
	 * @param key The Key
	 */
	public void remove(Key key) {
		cache.get().remove(key);
	}

	/**
	 * Updates a set of possibly existing properties.
	 * Returns changes between new and existing property sets.
	 *
	 * @param key           The Key
	 * @param newProperties New object properties
	 * @param forceReplace  Skip smart cache check for changes and replace all property values
	 * @return Changed properties.
	 */
	public Set<String> update(Key key, Map<String, Object> newProperties, boolean forceReplace) {

		Map<String, Long> existing = cache.get().get(key);

		if (existing != null && !forceReplace) {

			Set<String> changed = new HashSet<>(newProperties.size());

			for (Map.Entry<String, Object> newEntry : newProperties.entrySet()) {
				String newEntryKey = newEntry.getKey();
				Object existingPropHash = existing.get(newEntryKey);

				// property does not exist yet or has different value then new property
				Object newEntryValue = newEntry.getValue();
				if (newEntryValue != null) {
					Long newEntryHash = getPropertyHash(newEntryValue);
					if (existingPropHash == null || !existingPropHash.equals(newEntryHash)) {
						changed.add(newEntryKey);
					}
				} else { // set to null, write it
					if (existingPropHash != null) {
						changed.add(newEntryKey);
					}
				}
			}

			insert(key, newProperties);
			return changed;
		}

		insert(key, newProperties);
		return newProperties.keySet();
	}

	/**
	 * Returns a hash of AS-supported properties: Long, Double, String, byte[]
	 *
	 * @param property
	 * @return
	 */
	private Long getPropertyHash(Object property) {
		if (property instanceof Long) {

			return (Long) property;
		} else if (property instanceof Double) {

			return Double.doubleToLongBits((Double) property);
		} else if (property instanceof byte[]) {

			return new MurmurHash3().add((byte[]) property).hash()[0];
		} else if (property instanceof String) {

			return new MurmurHash3().add(((String) property).getBytes()).hash()[0];
		} else if (property instanceof Map) {

			Map<Object, Object> propMap = (Map<Object, Object>) property;
			MurmurHash3 hasher = new MurmurHash3();
			for (Map.Entry entry : propMap.entrySet()) {
				Long valueHash = entry.getValue() == null ? Long.MAX_VALUE : getPropertyHash(entry.getValue());
				Long keyHash = entry.getKey() == null ? Long.MAX_VALUE : getPropertyHash(entry.getKey());
				hasher.add(keyHash);
				hasher.add(valueHash);
			}

			return hasher.hash()[0];
		} else if (property instanceof List) {

			List<Object> propMap = (List<Object>) property;
			MurmurHash3 hasher = new MurmurHash3();
			for (Object entry : propMap) {
				Long propertyHash = entry == null ? Long.MAX_VALUE : getPropertyHash(entry);
				hasher.add(propertyHash);
			}

			return hasher.hash()[0];
		} else {
			// this happens if POJOs are directly saved to database or when POJOs are added to Maps/Lists
			// AS client then serializes & saves them
			return getPropertyHash(property.getClass().getName() + property.hashCode());  // a simplistic hash for POJOs
		}
	}


}
