package com.spikeify;

import com.aerospike.client.AerospikeClient;
import com.spikeify.entity.EntityLDT;
import com.spikeify.entity.EntityListOfBytes;
import com.spikeify.entity.EntitySubJson;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.*;

import static org.junit.Assert.assertEquals;

public class LargeDataTest {

	private final Long userKey1 = new Random().nextLong();
	private final String namespace = "test";
	private final String setName = "newTestSet";
	private Spikeify sfy;
	private AerospikeClient client;

	@Before
	public void dbSetup() {

		SpikeifyService.globalConfig(namespace, 3000, "localhost");
		sfy = SpikeifyService.sfy();
		sfy.truncateNamespace(namespace);

		client = new AerospikeClient("localhost", 3000);
	}

	@After
	public void dbCleanup() {
		sfy.truncateNamespace(namespace);
	}

	@Test
	public void testBigIndexedList() {

		EntityLDT entity = new EntityLDT();
		entity.userId = userKey1;
		sfy.create(entity).now();


		List<EntityLDT> list = sfy.scanAll(EntityLDT.class).now();
		assertEquals(1, list.size());


		int count = 1000;
		long offset = 1_000_000L;

		List<Long> data = new ArrayList<>(count);
		for (int i = 0; i < count; i++) {
			data.add(i + offset);
		}
		entity.list.addAll(data);

		// get map config, check count setting
		Map confmap = entity.list.getInnerConfig();
		assertEquals((long) count, confmap.get("PropItemCount"));
		assertEquals((long) entity.list.size(), confmap.get("PropItemCount"));

		assertEquals(offset + 100L, (long) entity.list.get(100));
		assertEquals(offset + 999L, (long) entity.list.get(999));
		Assert.assertTrue(entity.list.exists(0));
		Assert.assertTrue(entity.list.exists(count - 1));
		Assert.assertFalse(entity.list.exists(-1));  // invalid index
		Assert.assertFalse(entity.list.exists(count));  // out of range
		Assert.assertEquals(null, entity.list.get(count));  // out of range, returns null

		// test range
		List<Long> range = entity.list.range(100, 104);
		assertEquals(5, range.size());
		assertEquals(offset + 100L, (long) range.get(0));
		assertEquals(offset + 104L, (long) range.get(4));

		// test range & trim & add back
		List<Long> rangeTrim = entity.list.range(995, 999);
		assertEquals(5, rangeTrim.size());
		assertEquals(offset + 995, (long) rangeTrim.get(0));
		assertEquals(offset + 999, (long) rangeTrim.get(4));
		// data exists, trim the list now
		int removed = entity.list.trim(995);
		assertEquals(5, removed);
		rangeTrim = entity.list.range(995, 999);
		Assert.assertTrue(rangeTrim.isEmpty());
		assertEquals(count - 5, entity.list.size());
		// add back
		for (int i = 995; i < 1000; i++) {
			entity.list.add(i + offset);
		}
		assertEquals(count, entity.list.size());
		rangeTrim = entity.list.range(995, 999);
		assertEquals(5, rangeTrim.size());
		assertEquals(offset + 995, (long) rangeTrim.get(0));
		assertEquals(offset + 996, (long) rangeTrim.get(1));
		assertEquals(offset + 997, (long) rangeTrim.get(2));
		assertEquals(offset + 998, (long) rangeTrim.get(3));
		Assert.assertTrue(entity.list.exists(999));
	}

	@Test
	public void testBigIndexedListAddEmpty() {

		EntityLDT entity = new EntityLDT();
		entity.userId = userKey1;
		sfy.create(entity).now();

		List<Long> emptyList = new ArrayList<>(0);

		entity.list.addAll(emptyList);
		Assert.assertTrue(entity.list.isEmpty());
		assertEquals(0, entity.list.size());
	}


	@Test
	public void testOutOfRange() {

		EntityLDT entity = new EntityLDT();
		entity.userId = userKey1;
		sfy.create(entity).now();

		List<Long> sample = new ArrayList<>(10);
		for (int i = 0; i < 10; i++) {
			sample.add((long) i);
		}

		entity.list.addAll(sample);
		assertEquals(10, entity.list.size());

		// request an out of bounds range - does not complain
		List<Long> range = entity.list.range(5, 15);
		// only five elements found
		assertEquals(5, range.size());

	}


	@Test(expected = IllegalArgumentException.class)
	public void testInvertedRange() {

		EntityLDT entity = new EntityLDT();
		entity.userId = userKey1;
		sfy.create(entity).now();

		List<Long> sample = new ArrayList<>(10);
		for (int i = 0; i < 10; i++) {
			sample.add((long) i);
		}

		entity.list.addAll(sample);
		assertEquals(10, entity.list.size());

		// request with reversed indexes - throws exception
		List<Long> range = entity.list.range(5, 0);
	}

	@Test(expected = IndexOutOfBoundsException.class)
	public void testTrimOutOfBounds() {

		EntityLDT entity = new EntityLDT();
		entity.userId = userKey1;
		sfy.create(entity).now();

		List<Long> sample = new ArrayList<>(10);
		for (int i = 0; i < 10; i++) {
			sample.add((long) i);
		}

		entity.list.addAll(sample);
		assertEquals(10, entity.list.size());

		// request with reversed indexes - throws exception
		entity.list.trim(15);
	}

	@Test
	public void testAddingJson() {

		EntityLDT entity = new EntityLDT();
		entity.userId = userKey1;
		sfy.create(entity).now();

		List<EntitySubJson> sample = new ArrayList<>(10);
		for (int i = 0; i < 10; i++) {
			sample.add(new EntitySubJson(i, "text" + i, new Date(i * 10000)));
		}

		entity.jsonList.addAll(sample);
		assertEquals(10, entity.jsonList.size());

		for (int i = 0; i < 10; i++) {
			EntitySubJson json = entity.jsonList.get(i);
			assertEquals(i, json.first);
			assertEquals("text" + i, json.second);
			assertEquals(null, json.date);
		}

		List<EntitySubJson> range = entity.jsonList.range(0, 9);
		for (int i = 0; i < 10; i++) {
			EntitySubJson json = range.get(i);
			assertEquals(i, json.first);
			assertEquals("text" + i, json.second);
			assertEquals(null, json.date);
		}
	}

	@Test
	public void addByteArray() {

		SpikeifyService.register(EntityListOfBytes.class);

		EntityListOfBytes entity = new EntityListOfBytes();
		entity.userId = userKey1;
		entity.name = "test";

		sfy.create(entity).now();

		byte[] bytes = new byte[10];
		for (int index = 0; index < 10; index ++) {
			bytes[index] = (byte)index;
		}

		entity.data.add(bytes);


		List<EntityListOfBytes> list = sfy.query(EntityListOfBytes.class).filter("name", "test").now().toList();
		assertEquals(1, list.size());

		EntityListOfBytes compare = list.get(0);
		assertEquals(0, compare.data.size());

		// update entity loaded with query ...
		compare.name = "newName";
		sfy.update(compare).now();

		// check list
		EntityListOfBytes found = sfy.get(EntityListOfBytes.class).key(userKey1).now();

		for (int index = 0; index < found.data.size(); index ++) {
			assertEquals((byte)index, found.data.get(0)[index]);
		}
	}

	@Test
	public void testRangeOnEmptyList() {
		EntityLDT entity = new EntityLDT();
		entity.userId = userKey1;
		entity.list = new BigIndexedList<>();
		sfy.create(entity).now();

		EntityLDT entityCheck = sfy.get(EntityLDT.class).key(userKey1).now();
		Assert.assertNotNull(entityCheck);

		entityCheck.list.range(0, 100);
	}

	@Test
	public void testTrimOnEmptyList() {
		EntityLDT entity = new EntityLDT();
		entity.userId = userKey1;
		entity.list = new BigIndexedList<>();
		sfy.create(entity).now();

		EntityLDT entityCheck = sfy.get(EntityLDT.class).key(userKey1).now();
		Assert.assertNotNull(entityCheck);

		entityCheck.list.trim(10);
	}
}
