package com.spikeify;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.Value;
import com.aerospike.client.large.LargeList;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.spikeify.entity.*;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.*;

import static org.junit.Assert.assertEquals;

public class BigMapTest {

	private final Long userKey1 = new Random().nextLong();
	private final String namespace = "test";
	private final String setName = "newTestSet";
	private Spikeify sfy;
	private AerospikeClient client;

	@Before
	public void dbSetup() {

		SpikeifyService.globalConfig(namespace, 3000, "localhost");
		sfy = SpikeifyService.sfy();

		client = new AerospikeClient("localhost", 3000);
	}

	@After
	public void dbCleanup() {
		sfy.truncateNamespace(namespace);
	}

	@Test
	public void testBigMap() {

		EntityLargeMap entity = new EntityLargeMap();
		entity.userId = userKey1;
		sfy.create(entity).now();

		List<EntityLargeMap> list = sfy.scanAll(EntityLargeMap.class).now();
		assertEquals(1, list.size());

		int count = 1000;
		long offset = 1_000_000L;

		Map<Long, Long> data = new HashMap<>(count);
		for (int i = 0; i < count; i++) {
			data.put((long) i, i + offset);
		}
		entity.map.putAll(data);

		// get map config, check count setting
		Map confmap = entity.map.getInnerConfig();
		assertEquals((long) count, confmap.get("PropItemCount"));
		assertEquals((long) entity.map.size(), confmap.get("PropItemCount"));

		assertEquals(offset + 100L, (long) entity.map.get(100L));
		assertEquals(offset + 999L, (long) entity.map.get(999L));
		Assert.assertTrue(entity.map.containsKey(0L));
		Assert.assertTrue(entity.map.containsKey((long) (count - 1)));
		Assert.assertFalse(entity.map.containsKey(-1L));  // invalid index
		Assert.assertFalse(entity.map.containsKey((long) count));  // out of range
		Assert.assertEquals(null, entity.map.get((long) count));  // out of range, returns null

		Map<Long, Long> last = entity.map.findLast(2);
		Assert.assertEquals(Long.valueOf(count - 2 + offset), last.get((long) count - 2));
		Assert.assertEquals(Long.valueOf(count - 1 + offset), last.get((long) count - 1));

		Map<Long, Long> first = entity.map.findFirst(2);
		Assert.assertEquals(Long.valueOf(0 + offset), first.get(0L));
		Assert.assertEquals(Long.valueOf(1 + offset), first.get(1L));
	}

	@Test
	public void testGetAll() {

		EntityLargeMap entity = new EntityLargeMap();
		entity.userId = userKey1;
		sfy.create(entity).now();

		int count = 10_000;
		long offset = 1_000_000L;

		Map<Long, Long> data = new HashMap<>(count);
		for (int i = 0; i < count; i++) {
			data.put(i + offset, (long) i);
		}
		entity.map.putAll(data);

		// get all
		Map<Long, Long> allList = entity.map.getAll();
		Assert.assertEquals(count, allList.size());
	}

	@Test
	public void testBigIndexedListAddEmpty() {

		EntityLargeMap entity = new EntityLargeMap();
		entity.userId = userKey1;
		sfy.create(entity).now();

		Map<Long, Long> emptyList = new HashMap<>(0);

		entity.map.putAll(emptyList);
		Assert.assertTrue(entity.map.isEmpty());
		assertEquals(0, entity.map.size());
	}


	@Test
	public void testOutOfRange() {

		EntityLargeMap entity = new EntityLargeMap();
		entity.userId = userKey1;
		sfy.create(entity).now();

		int count = 100;
		int step = 10;
		long offset = 1_000_000L;

		Map<Long, Long> data = new HashMap<>(count);
		for (int i = 0; i < count; i += step) {
			data.put((long) i, i + offset);
		}
		entity.map.putAll(data);

		assertEquals(count / step, entity.map.size());

		// request in-bounds range
		Map<Long, Long> range = entity.map.range(50L, 99L);
		// only five elements found
		assertEquals(5, range.size());

		// request an out of bounds range - does not complain
		Map<Long, Long> range2 = entity.map.range(50L, 150L);
		// only five elements found
		assertEquals(5, range2.size());

		// request inverted range - returns nothing
		Map<Long, Long> range3 = entity.map.range(99L, 50L);
		// only five elements found
		assertEquals(0, range3.size());

	}

	@Test
	public void testAddingJson() {

		EntityLargeMap entity = new EntityLargeMap();
		entity.userId = userKey1;
		sfy.create(entity).now();

		Map<Long, EntitySubJson> sample = new HashMap<>(10);
		for (int i = 0; i < 10; i++) {
			sample.put((long) i, new EntitySubJson(i, "text" + i, new Date(i * 10000)));
		}

		entity.jsonMap.putAll(sample);
		assertEquals(10, entity.jsonMap.size());

		for (long i = 0; i < 10; i++) {
			EntitySubJson json = entity.jsonMap.get(i);
			assertEquals(i, json.first);
			assertEquals("text" + i, json.second);
			assertEquals(null, json.date);
		}

		Map<Long, EntitySubJson> range = entity.jsonMap.range(0L, 9L);
		for (int i = 0; i < 10; i++) {
			EntitySubJson json = range.get((long) i);
			assertEquals(i, json.first);
			assertEquals("text" + i, json.second);
			assertEquals(null, json.date);
		}

		// add 10 more elements
		EntityLargeMap reloaded = sfy.get(EntityLargeMap.class).key(userKey1).now();
		Map<Long, EntitySubJson> sample2 = new HashMap<>(10);
		for (int i = 10; i < 20; i++) {
			sample2.put((long) i, new EntitySubJson(i, "text" + i, new Date(i * 10000)));
		}

		// add some more
		for (Map.Entry<Long, EntitySubJson> jsonEntry : sample2.entrySet()) {
			reloaded.jsonMap.put(jsonEntry.getKey() + 10, jsonEntry.getValue());
		}
		reloaded.jsonMap.putAll(sample2);

		Assert.assertEquals(30, reloaded.jsonMap.size());

		reloaded = sfy.get(EntityLargeMap.class).key(userKey1).now();
		Assert.assertEquals(30, reloaded.jsonMap.size());

	}

	@Test
	public void addByteArray() {

		SpikeifyService.register(EntityMapOfBytes.class);

		EntityMapOfBytes entity = new EntityMapOfBytes();
		entity.userId = userKey1;
		entity.name = "test";

		sfy.create(entity).now();

		for (int i = 0; i < 10; i++) {

			byte[] bytes = new byte[10];
			for (int index = 0; index < 10; index++) {
				bytes[index] = (byte) (index + 10 * i);
			}
			entity.data.put((long) i, bytes);
		}

		List<EntityMapOfBytes> list = sfy.query(EntityMapOfBytes.class).filter("name", "test").now().toList();
		assertEquals(1, list.size());

		EntityMapOfBytes compare = list.get(0);
		assertEquals(10, compare.data.size());

		// update entity loaded with query ...
		compare.name = "newName";
		sfy.update(compare).now();

		// check list
		EntityMapOfBytes found = sfy.get(EntityMapOfBytes.class).key(userKey1).now();

		for (int i = 0; i < found.data.size(); i++) {
			byte[] array = found.data.get((long) i);
			for (int index = 0; index < array.length; index++) {
				assertEquals(index + 10 * i, array[index]);
			}
		}
	}

	@Test
	public void testEmptyList() {
		EntityLargeMap entity = new EntityLargeMap();
		entity.userId = userKey1;
		sfy.create(entity).now();

		EntityLargeMap entityCheck = sfy.get(EntityLargeMap.class).key(userKey1).now();
		Assert.assertNotNull(entityCheck);

		Assert.assertTrue(entityCheck.map.range(0L, 100L).isEmpty());
		Assert.assertEquals(0, entityCheck.map.size());
		Assert.assertTrue(entityCheck.map.isEmpty());
		Assert.assertFalse(entityCheck.map.containsKey(0L));
	}


	@Test
	public void testLargeMapLongObjectToJson() {

		EntityLargeMap2 entity = new EntityLargeMap2();
		entity.bla = "Bla2";
		sfy.create(entity).now();

		entity.javaMap.put(10L, new EntitySubJava("10"));
		entity.javaMap.put(20L, new EntitySubJava("20"));

		LargeList reloaded = sfy.getClient().getLargeList(null, new Key(namespace, "EntityLargeMap2", entity.userId), "javaMap");

		Map<String, Object> val1 = (Map<String, Object>) reloaded.find(Value.get(10L)).get(0);

		Assert.assertEquals(10L, val1.get("key"));
		Assert.assertEquals(EntitySubJava.class, val1.get("value").getClass());  // we get back Java object
		Assert.assertEquals("10", ((EntitySubJava) val1.get("value")).getValue());
	}

	@Test
	public void testLargeMapIsPrivateTest() throws IOException {

		EntityLargeMap3 entity = new EntityLargeMap3();
		entity.bla = "Bla";
		sfy.create(entity).now();

		entity.put(10L, new EntitySubJava("10"));
		entity.put(20L, new EntitySubJava("20"));

		LargeList reloaded = sfy.getClient().getLargeList(null, new Key(namespace, "EntityLargeMap3", entity.userId), "jsonMap");

		Map<String, Object> val1 = (Map<String, Object>) reloaded.find(Value.get(10L)).get(0);

		Assert.assertEquals(10L, val1.get("key"));
		Assert.assertEquals(String.class, val1.get("value").getClass());  // we get back a String

		Assert.assertEquals(new EntitySubJava("10"), new ObjectMapper().readValue(((String) val1.get("value")), EntitySubJava.class));

	}
}
