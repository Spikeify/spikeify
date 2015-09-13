package com.spikeify;

import com.aerospike.client.Key;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * A cache of records, used to calculate changes between loaded and saved objects.
 * This is needed because Aerospike encourages saving only changed bins (records).
 */
public class RecordsCache {

	private final ThreadLocal<Map<Key/*key of mapped object*/, Map<String, String>/*record properties*/>> cache =
			new ThreadLocal<Map<Key/*key of mapped object*/, Map<String, String>/*record properties*/>>() {
				@Override
				protected Map<Key, Map<String, String>> initialValue() {
					return new HashMap<>();
				}
			};

//	private static Map<Key/*key of mapped object*/, Map<String, String>/*record properties*/> propertiesCache = new HashMap<>();

	/**
	 * Insert a set of properties linked to a Key
	 *
	 * @param key The Key
	 * @param properties Object properties
	 */
	public void insert(Key key, Map<String, Object> properties) {

		Map<String, String> propertiesKeys = new HashMap<>(properties.size());
		for (Map.Entry<String, Object> prop : properties.entrySet()) {
			Object property = prop.getValue();
			if (property != null) {
				String propertyHash = getPropertyHash(property);
				propertiesKeys.put(prop.getKey(), propertyHash);
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
	 * @param key The Key
	 * @param newProperties New object properties
	 * @param forceReplace Skip smart cache check for changes and replace all property values
	 * @return Changed properties.
	 */
	public Set<String> update(Key key, Map<String, Object> newProperties, boolean forceReplace) {

		Map<String, String> existing = cache.get().get(key);

		if (existing != null) {
			if (forceReplace) {
				return newProperties.keySet();
			}

			Set<String> changed = new HashSet<>(newProperties.size());

			for (Map.Entry<String, Object> newEntry : newProperties.entrySet()) {
				Object existingProp = existing.get(newEntry.getKey());

				// property does not exist yet or has different value then new property
				Object newEntryValue = newEntry.getValue();
				if (newEntryValue != null) {
					String newEntryHash = getPropertyHash(newEntryValue);
					if (existingProp == null || !existingProp.equals(newEntryHash)) {
						changed.add(newEntry.getKey());
					}
				} else { // set to null, write it
					if (existingProp != null) {
						changed.add(newEntry.getKey());
					}
				}
			}

			insert(key, newProperties);
			return changed;
		}

		insert(key, newProperties);
		return newProperties.keySet();
	}


	private String getPropertyHash(Object property) {
		return property.getClass().getName() + ":" + property.hashCode();
	}

}
