package com.spikeify;

import com.aerospike.client.Key;
import org.junit.Assert;
import org.junit.Test;

import java.util.*;

public class RecordCacheTest {

	@Test
	public void testRecordsCache() {

		RecordsCache cache = new RecordsCache();

		// create initial data
		Key key1 = new Key("namespace1", "set1", "12345");
		Map<String, Object> props1 = new HashMap<>(5);
		props1.put("one", 1L);
		props1.put("two", "test");
		props1.put("five", null); // null should be ignored
		List<Long> longs1 = new ArrayList<>();  // add a list of longs
		longs1.add(1L);
		longs1.add(2L);
		props1.put("longs", longs1);
		Set<String> updateResult1 = cache.update(key1, props1, false);
		Assert.assertEquals(props1.keySet(), updateResult1);

		Key key2 = new Key("namespace2", "set2", "12345");
		Map<String, Object> props2 = new HashMap<>(5);
		props2.put("one", 1L);
		props2.put("two", "test");
		List<String> strings = new ArrayList<>();  // add a list of strings
		strings.add("1");
		strings.add("2");
		props2.put("string", strings);
		Set<String> updateResult2 = cache.update(key2, props2, false);
		Assert.assertEquals(props2.keySet(), updateResult2);

		// make some changes on key1
		props1 = new HashMap<>(5);
		props1.put("one", 2L);
		props1.put("two", "test"); // same value - should not be updated
		props1.put("three", 1.1d);
		longs1.add(3L);  // add another long
		props1.put("longs", longs1);
		Set<String> updateResult12 = cache.update(key1, props1, false);
		Assert.assertEquals(3, updateResult12.size());

		// make some changes on key2
		props2 = new HashMap<>(5);
		props2.put("one", 1L);  // same value - should not be updated
		props2.put("two", 5L);
		props2.put("three", 1.1d);
		props2.put("four", 1.5f);
		props2.put("five", null);  // null should be ignored
		strings.remove(0); // change list of strings
		props2.put("string", strings);
		Set<String> updateResult22 = cache.update(key2, props2, false);
		Assert.assertEquals(4, updateResult22.size());

		// make more changes on key1 - flip property values
		props1 = new HashMap<>(5);
		props1.put("one", "test");
		props1.put("two", 1);
		Set<String> updateResult13 = cache.update(key1, props1, false);
		Assert.assertEquals(2, updateResult13.size());

		// remove props on key1
		cache.remove(key1);
		props1 = new HashMap<>(5);
		props1.put("one", "test"); // same value - but should be updated, because key was removed from cache
		props1.put("two", 15);
		props1.put("three", 12345L);
		Set<String> updateResult14 = cache.update(key1, props1, false);
		Assert.assertEquals(3, updateResult14.size());
	}

	@Test
	public void testCacheListPojo() {

		RecordsCache cache = new RecordsCache();

		// create initial data
		Key key1 = new Key("namespace1", "set1", "12345");
		Map<String, Object> props1 = new HashMap<>(5);
		List<Object> objects = new ArrayList<>();  // add a list of objects
		objects.add(new POJO("a", 1));
		objects.add(new POJO("b", 2));
		props1.put("objects", objects);
		Set<String> updateResult1 = cache.update(key1, props1, false);
		Assert.assertEquals(props1.keySet(), updateResult1);

		// make some changes on key1
		objects = new ArrayList<>();
		objects.add(new POJO("a", 1));
		objects.add(new POJO("b", 3));  // last element changed
		props1.put("objects", objects);
		Set<String> updateResult12 = cache.update(key1, props1, false);
		Assert.assertEquals(1, updateResult12.size());
	}

	public static class POJO{
		public String one;
		public int two;

		public POJO(String one, int two) {
			this.one = one;
			this.two = two;
		}
	}

	@Test
	public void testRecordsCacheWithForce() {

		RecordsCache cache = new RecordsCache();

		// create initial data
		Key key1 = new Key("namespace1", "set1", "12345");
		Map<String, Object> props1 = new HashMap<>(5);
		props1.put("one", 1);
		props1.put("two", "test");
		props1.put("five", null); // null should be ignored
		List<Long> longs1 = new ArrayList<>();  // add a list of longs
		longs1.add(1L);
		longs1.add(2L);
		props1.put("longs", longs1);
		Set<String> updateResult1 = cache.update(key1, props1, true);
		Assert.assertEquals(props1.keySet(), updateResult1);

		Key key2 = new Key("namespace2", "set2", "12345");
		Map<String, Object> props2 = new HashMap<>(5);
		props2.put("one", 1);
		props2.put("two", "test");
		List<String> strings = new ArrayList<>();  // add a list of strings
		strings.add("1");
		strings.add("2");
		props2.put("string", strings);
		Set<String> updateResult2 = cache.update(key2, props2, true);
		Assert.assertEquals(props2.keySet(), updateResult2);

		// make some changes on key1
		props1 = new HashMap<>(5);
		props1.put("one", 2);
		props1.put("two", "test"); // same value - should be updated when used with force=true
		props1.put("three", 1.1d);
		longs1.add(3L);  // add another long
		props1.put("longs", longs1);
		Set<String> updateResult12 = cache.update(key1, props1, true);
		Assert.assertEquals(4, updateResult12.size());

		// make some changes on key2
		props2 = new HashMap<>(5);
		props2.put("one", 1);  // same value - should not be updated, except with force=true
		props2.put("two", 5);
		props2.put("three", 1.1d);
		props2.put("four", 1.5f);
		props2.put("five", null);  // null should be ignored, except with force=true
		strings.remove(0); // change list of strings
		props2.put("string", strings);
		Set<String> updateResult22 = cache.update(key2, props2, true);
		Assert.assertEquals(6, updateResult22.size());

		// make more changes on key1 - flip property values
		props1 = new HashMap<>(5);
		props1.put("one", "test");
		props1.put("two", 1);
		Set<String> updateResult13 = cache.update(key1, props1, true);
		Assert.assertEquals(2, updateResult13.size());

		// remove props on key1
		cache.remove(key1);
		props1 = new HashMap<>(5);
		props1.put("one", "test"); // same value - but should be updated, because key was removed from cache
		props1.put("two", 15);
		props1.put("three", 12345L);
		Set<String> updateResult14 = cache.update(key1, props1, true);
		Assert.assertEquals(3, updateResult14.size());

	}

}
