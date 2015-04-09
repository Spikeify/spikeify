package com.spikeify;

import com.aerospike.client.Key;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class RecordCacheTest {

	@Test
	public void testRecordsCache() {

		RecordsCache cache = new RecordsCache();

		// create initial data
		Key key1 = new Key("namespace1", "set1", "12345");
		Map<String, Object> props1 = new HashMap<>(5);
		props1.put("one", 1);
		props1.put("two", "test");
		Map<String, Object> updateResult1 = cache.update(key1, props1);
		Assert.assertEquals(props1, updateResult1);

		Key key2 = new Key("namespace2", "set2", "12345");
		Map<String, Object> props2 = new HashMap<>(5);
		props2.put("one", 1);
		props2.put("two", "test");
		Map<String, Object> updateResult2 = cache.update(key2, props2);
		Assert.assertEquals(props2, updateResult2);

		// make some changes on key1
		props1 = new HashMap<>(5);
		props1.put("one", 2);
		props1.put("two", "test"); // same value - should not be updated
		props1.put("three", 1.1d);
		Map<String, Object> updateResult12 = cache.update(key1, props1);
		Assert.assertEquals(2, updateResult12.size());

		// make some changes on key2
		props2 = new HashMap<>(5);
		props2.put("one", 1);  // same value - should not be updated
		props2.put("two", 5);
		props2.put("three", 1.1d);
		props2.put("four", 1.5f);
		Map<String, Object> updateResult22 = cache.update(key2, props2);
		Assert.assertEquals(3, updateResult22.size());

		// make more changes on key1 - flip property values
		props1 = new HashMap<>(5);
		props1.put("one", "test");
		props1.put("two", 1);
		Map<String, Object> updateResult13 = cache.update(key1, props1);
		Assert.assertEquals(2, updateResult13.size());

		// remove props on key1
		cache.remove(key1);
		props1 = new HashMap<>(5);
		props1.put("one", "test"); // same value - but should be updated, because key was removed from cache
		props1.put("two", 15);
		props1.put("three", 12345l);
		Map<String, Object> updateResult14 = cache.update(key1, props1);
		Assert.assertEquals(3, updateResult14.size());

	}
}
