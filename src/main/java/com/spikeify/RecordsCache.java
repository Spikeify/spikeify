package com.spikeify;

import com.aerospike.client.Key;

import java.util.HashMap;
import java.util.Map;

/**
 * A cache of records, used to calculate changes between loaded and saved objects.
 * This is needed because Aerospike encourages saving only changed bins (records).
 */
public class RecordsCache {

	private static Map<Key/*key of mapped object*/, Map<String, Object>/*record properties*/> propertiesCache = new HashMap<>();

	/**
	 * Insert a set of properties linked to a Key
	 *
	 * @param key
	 * @param properties
	 */
	public void insert(Key key, Map<String, Object> properties) {
		propertiesCache.put(key, properties);
	}

	/**
	 * Remove a set of properties linked to a Key
	 *
	 * @param key
	 */
	public void remove(Key key) {
		propertiesCache.remove(key);
	}

	/**
	 * Updates a set of possibly existing properties.
	 * Returns changes between current and previous property sets.
	 *
	 * @param key
	 * @param newProperties
	 * @return Changed properties.
	 */
	public Map<String, Object> update(Key key, Map<String, Object> newProperties) {

		Map<String, Object> existing = propertiesCache.get(key);

		if (existing != null) {
			Map<String, Object> changed = new HashMap<>(newProperties.size());

			for (Map.Entry<String, Object> newEntry : newProperties.entrySet()) {
				Object existingProp = existing.get(newEntry.getKey());

				// property does not exist yet or has different value then new property
				if (existingProp == null || !existingProp.equals(newEntry.getValue())) {
					changed.put(newEntry.getKey(), newEntry.getValue());
				}
			}

			propertiesCache.put(key, newProperties);
			return changed;
		}

		propertiesCache.put(key, newProperties);
		return newProperties;
	}


}
