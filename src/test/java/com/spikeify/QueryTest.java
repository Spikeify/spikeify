package com.spikeify;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.query.*;
import com.spikeify.annotations.BinName;
import com.spikeify.annotations.Generation;
import com.spikeify.annotations.Indexed;
import com.spikeify.annotations.UserKey;
import com.spikeify.entity.EntityIndexed;
import com.spikeify.entity.EntityOne;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.junit.Assert.assertEquals;

public class QueryTest {

	private final String namespace = "test";
	private final String setName = "EntityOne";
	private Spikeify sfy;

	@Before
	public void dbSetup() {

		SpikeifyService.globalConfig(namespace, 3000, "localhost");
		sfy = SpikeifyService.sfy();
		sfy.truncateNamespace(namespace);
	}

	@Test
	public void testEntityQuery() {

		SpikeifyService.register(EntityOne.class);

		// create records
		for (int i = 0; i < 100; i++) {
			EntityOne ent = TestUtils.randomEntityOne(setName);
			ent.theSetName = setName;
			if (i % 10 == 0) {
				ent.two = "content";
				ent.one = TestUtils.random.nextInt(1000);
			}
			sfy.create(ent).now();
		}

		ResultSet<EntityOne> entities = sfy.query(EntityOne.class)
										   .filter("two", "content")
										   .now();

		int count = 0;
		for (EntityOne entity : entities) {
			count++;
			assertEquals("content", entity.two);
		}
		assertEquals(10, count);


		ResultSet<EntityOne> entities2 = sfy.query(EntityOne.class)
											.filter("two", "content")
											.now();

		int count2 = 0;
		for (EntityOne entity2 : entities2) {
			count2++;
			Assert.assertTrue(0 < entity2.one && entity2.one < 1000);
		}
		assertEquals(10, count2);

	}

	@Test
	public void testListQueryNative() {

		Random random = new Random();

		AerospikeClient client = new AerospikeClient("localhost", 3000);
		String namespace = "test";
		String setName = "testSetQuery";
		String stringListIndex = "index_list_string";
		String binString = "binString";

		client.createIndex(new Policy(), namespace, setName, stringListIndex, binString, IndexType.STRING, IndexCollectionType.LIST);

		// create records
		for (int i = 0; i < 100; i++) {
			Key key = new Key(namespace, setName, random.nextLong());
			Bin listStringBin;

			List<String> listStrings = new ArrayList<>();

			// subset of records have predictable bin values - so they can be found by query
			if (i % 10 == 0) {
				listStrings.add("content"); // fixed string
			}

			// random strings added to list
			listStrings.add(TestUtils.randomWord());
			listStrings.add(TestUtils.randomWord());
			listStringBin = new Bin(binString, listStrings);

			client.put(null, key, listStringBin);

		}

		Statement statement = new Statement();
		statement.setIndexName(stringListIndex);
		statement.setNamespace(namespace);
		statement.setSetName(setName);
		statement.setFilters(Filter.contains(binString, IndexCollectionType.LIST, "content"));

		RecordSet recSet = client.query(null, statement);

		int count = 0;
		while (recSet.next()) {
			count++;
			Record record = recSet.getRecord();
			Assert.assertTrue(((List) record.bins.get(binString)).contains("content"));
		}

		// query should return 10 records
		assertEquals(10, count);
	}

	@Test
	public void testListQuery() {

		SpikeifyService.register(EntityOne.class);
		Map<Long, EntityOne> entities = TestUtils.randomEntityOne(1000, setName);

		int count = 0;
		for (EntityOne entity : entities.values()) {
			entity.nine = new ArrayList<>();

			// subset of records have predictable bin values - so they can be found by query
			if (count % 20 == 0) {
				entity.nine.add("content"); // fixed string
			}
			entity.nine.add(TestUtils.randomWord());
			entity.nine.add(TestUtils.randomWord());

			if (count % 3 == 0) {
				sfy.create(entity).now();
			}
			else {
				sfy.create(entity.userId, entity).setName(setName).now();
			}

			count++;
		}

		ResultSet<EntityOne> results = sfy
			.query(EntityOne.class)
			.filter("nine", "content")
			.now();

		// query should return 10 records
		List<EntityOne> list = results.toList();
		assertEquals(50, list.size());
	}

	@Test
	public void testListQuery_CustomSetName() {

		// custom set name and index on same entity ...
		String stringListIndex = "index_list_string_2";
		String setName = "testSetQuery";
		String binString = "nine";

		sfy.getClient().createIndex(new Policy(), namespace, setName, stringListIndex, binString, IndexType.STRING, IndexCollectionType.LIST);

		Map<Long, EntityOne> entities = TestUtils.randomEntityOne(1000, setName);

		int count = 0;
		for (EntityOne entity : entities.values()) {
			entity.nine = new ArrayList<>();

			// subset of records have predictable bin values - so they can be found by query
			if (count % 20 == 0) {
				entity.nine.add("content"); // fixed string
			}
			entity.nine.add(TestUtils.randomWord());
			entity.nine.add(TestUtils.randomWord());

			if (count % 3 == 0) {
				sfy.create(entity).now();
			}
			else {
				sfy.create(entity.userId, entity).setName(setName).now();
			}

			count++;
		}

		ResultSet<EntityOne> results = sfy
			.query(EntityOne.class)
			.setName(setName)
			.filter("nine", "content")
			.now();

		// query should return 50 records
		List<EntityOne> list = results.toList();
		assertEquals(50, list.size());

		// 2. ... set name after filter ...
		results = sfy
			.query(EntityOne.class)
			.filter("nine", "content")
			.setName(setName)
			.now();

		list = results.toList();
		assertEquals(50, list.size());
	}

	@Test
	public void testQueryWithIndexingAnnotation() {

		// register entity (and create indexes)
		SpikeifyService.register(EntityIndexed.class);

		// fill up
		// create records
		for (int i = 0; i < 100; i++) {
			EntityIndexed ent = new EntityIndexed();
			ent.key = Long.toString(new Random().nextLong());
			ent.list = new ArrayList<>();

			if (i % 10 == 0) {
				ent.text = "content";

				ent.list.add("bla");
			}

			if (i % 5 == 0) {
				ent.list.add("hopa");
				ent.list.add("cupa");
			}

			ent.number = i;

			sfy.create(ent).now();
		}

		// 1. equals
		ResultSet<EntityIndexed> entities = sfy.query(EntityIndexed.class)
											   .filter("text", "content")
											   .now();


		int count = 0;
		for (EntityIndexed entity : entities) {
			count++;
			assertEquals("content", entity.text);
		}
		assertEquals(10, count);


		// 2. range
		entities = sfy.query(EntityIndexed.class)
					  .filter("number", 10, 20)
					  .now();

		count = 0;
		for (EntityIndexed entity : entities) {
			count++;
			Assert.assertTrue(entity.number >= 10);
			Assert.assertTrue(entity.number <= 20);
		}
		assertEquals(11, count);

		// 2. list
		entities = sfy.query(EntityIndexed.class)
					  .filter("list", "bla")
					  .now();

		count = 0;
		for (EntityIndexed entity : entities) {
			count++;
			Assert.assertTrue(entity.list.contains("bla"));
		}
		assertEquals(10, count);
	}

	@Test
	public void multiQueryTest() {

		// add items to database
		for (int i = 0; i < 100; i++) {
			EntityIndexed ent = new EntityIndexed();
			ent.key = Long.toString(new Random().nextLong());
			ent.number = i;
			ent.text = "A";
			sfy.create(ent).now();
		}

		// query again and again on same field
		List<EntityIndexed> list = sfy.query(EntityIndexed.class).filter("text", "A").now().toList();
		assertEquals(100, list.size());

		for (int i = 1; i <= 100; i++) {

			Random rand = new Random();
			int idx = rand.nextInt(list.size());

			EntityIndexed change = list.get(idx);
			change.text = "B";
			sfy.update(change).now();

			list = sfy.query(EntityIndexed.class).filter("text", "A").now().toList();
			assertEquals("Number of result should be shorter than before", 100 - i, list.size());
		}
	}

	@Test
	public void multithreadedQueryTest() throws InterruptedException {

		// add items to database
		int WORKERS = 4;
		int ITEMS = 400;

		for (int i = 0; i < ITEMS; i++) {
			EntityIndexed ent = new EntityIndexed();
			ent.key = Long.toString(new Random().nextLong());
			ent.number = 0;
			ent.text = "A";
			sfy.create(ent).now();
		}

		// create threads
		Thread[] threads = new Thread[WORKERS];

		for (int i = 0; i < WORKERS; i++) {

			Runnable runnable = new Runnable() {
				@Override
				public void run() {

					execute();
				}
			};

			threads[i] = new Thread(runnable);
		}

		for (int i = 0; i < WORKERS; i++) {
			threads[i].start();
		}
		for (int i = 0; i < WORKERS; i++) {
			threads[i].join();
		}

		// query again and again on same field
		List<EntityIndexed> list = sfy.query(EntityIndexed.class).filter("text", "B").now().toList();
		assertEquals(ITEMS, list.size());

		for (EntityIndexed item: list) {
			assertEquals("Item has been changed by two threads: " + item.key, 1, item.number);
		}
	}

	private List<String> execute() {

		List<String> ids = new ArrayList<>();
		List<EntityIndexed> list;
		do {

			list = sfy.query(EntityIndexed.class).filter("text", "A").now().toList();

			if (list.size() > 0) {
				Random rand = new Random();
				int idx = rand.nextInt(list.size());

				final EntityIndexed item = list.get(idx);

				sfy.transact(1, new Work<EntityIndexed>() {
					@Override
					public EntityIndexed run() {

						EntityIndexed original = sfy.get(EntityIndexed.class).key(item.key).now();
						if (original.text.equals("A")) {

							original.text = "B";
							original.number = original.number + 1; // number of changes
							sfy.update(original).now();
						//	sfy.command(EntityIndexed.class).add("number", 1).now();
						}

						return original;
					}

				});
			}
		}
		while (list.size() > 0);

		return ids;
	}

	public static class LongEntity {
		@UserKey
		public String id;

		@Generation
		public Integer generation;

		@Indexed
		@BinName("sKey")
		public String sourceBucketAndKey;
	}

	@Test
	public void testTooLongFieldNameForIndex() {
		SpikeifyService.register(LongEntity.class);
		// test fails because index creation does not check for annotation @BinName

		// this filter by actual field name should be replaced with name in annotation @BinName
		ResultSet<LongEntity> entities2 = sfy.query(LongEntity.class)
				.filter("sourceBucketAndKey", "content")
				.now();
	}
}
