package com.spikeify;

import com.aerospike.client.*;
import com.aerospike.client.policy.CommitLevel;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.query.*;
import com.spikeify.annotations.BinName;
import com.spikeify.annotations.Generation;
import com.spikeify.annotations.Indexed;
import com.spikeify.annotations.UserKey;
import com.spikeify.entity.EntityIndexed;
import com.spikeify.entity.EntityOne;
import com.spikeify.entity.EntitySubJson;
import com.spikeify.generators.IdGenerator;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.util.*;
import java.util.function.Function;

import static org.junit.Assert.assertEquals;

public class QueryTest extends SpikeifyTest {

	private final String setName = "EntityOne";

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

		String setName = "testSetQuery";
		String stringListIndex = "index_list_string";
		String binString = "binString";

		//client.dropIndex(new Policy(), namespace, setName, stringListIndex);
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

		for (EntityIndexed item : list) {
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

		LongEntity test = new LongEntity();
		test.id = "1";
		test.sourceBucketAndKey = "Bla";
		sfy.create(test).now();

		test = new LongEntity();
		test.id = "2";
		test.sourceBucketAndKey = "Bla";
		sfy.create(test).now();

		test = new LongEntity();
		test.id = "3";
		test.sourceBucketAndKey = "Blabla";
		sfy.create(test).now();

		// this filter by actual field name should be replaced with name in annotation @BinName
		List<LongEntity> list = sfy.query(LongEntity.class)
			.filter("sourceBucketAndKey", "Bla")
			.now()
			.toList();

		assertEquals(2, list.size());

		// query should also be possible by binName
		list = sfy.query(LongEntity.class)
			.filter("sKey", "Blabla")
			.now()
			.toList();
		assertEquals(1, list.size());
	}

	@Test
	public void booleanFilterTest() {

		SpikeifyService.register(EntityOne.class);

		EntityOne test = new EntityOne();
		test.userId = 1L;
		test.seven = true;
		sfy.create(test).now();

		test = new EntityOne();
		test.userId = 2L;
		test.seven = false;
		sfy.create(test).now();

		test = new EntityOne();
		test.userId = 3L;
		test.seven = true;
		sfy.create(test).now();

		List<EntityOne> list = sfy.query(EntityOne.class).filter("seven", true).now().toList();
		assertEquals(2, list.size());

		Collections.sort(list, new Comparator<EntityOne>() {
			@Override
			public int compare(EntityOne o1, EntityOne o2) {

				return Long.compare(o1.userId, o2.userId);
			}
		});

		assertEquals(1L, list.get(0).userId.longValue());
		assertEquals(3L, list.get(1).userId.longValue());

		list = sfy.query(EntityOne.class).filter("seven", false).now().toList();
		assertEquals(1, list.size());
		assertEquals(2L, list.get(0).userId.longValue());
	}

	@Test
	public void nullableBooleanFilterTest() {

		SpikeifyService.register(EntityIndexed.class);

		EntityIndexed test = new EntityIndexed();
		test.key = "1";
		test.aBoolean = false;
		sfy.create(test).now();

		test = new EntityIndexed();
		test.key = "2";
		test.aBoolean = true;
		sfy.create(test).now();

		test = new EntityIndexed();
		test.key = "3";
		test.aBoolean = false;
		sfy.create(test).now();

		test = new EntityIndexed();
		test.key = "4";
		test.aBoolean = true;
		sfy.create(test).now();

		List<EntityIndexed> list = sfy.query(EntityIndexed.class).filter("aBoolean", true).now().toList();
		assertEquals(2, list.size());
		assertEquals("2", list.get(0).key);
		assertEquals("4", list.get(1).key);

		list = sfy.query(EntityIndexed.class).filter("aBoolean", false).now().toList();
		assertEquals(2, list.size());
		assertEquals("1", list.get(0).key);
		assertEquals("3", list.get(1).key);


		test.mapJson = new HashMap<>();
		test.mapJson.put("K", new EntitySubJson(1, "2", new Date()));
		test.mapJson.put("O", new EntitySubJson(1, "2", new Date()));
		sfy.update(test).now();

		list = sfy.query(EntityIndexed.class).filter("mapJson", "O").now().toList();
		assertEquals(1, list.size());
		assertEquals(2, list.get(0).mapJson.size());
	}


	@Test(expected = SpikeifyError.class)
	public void booleanFilterOnNonBooleanFieldTest() {

		SpikeifyService.register(EntityOne.class);

		EntityOne test = new EntityOne();
		test.userId = 1L;
		test.seven = true;
		sfy.create(test).now();

		test = new EntityOne();
		test.userId = 2L;
		test.seven = false;
		sfy.create(test).now();

		test = new EntityOne();
		test.userId = 3L;
		test.seven = true;
		sfy.create(test).now();

		try {
			sfy.query(EntityOne.class).filter("one", true).now().toList();
		}
		catch (SpikeifyError e) {
			assertEquals("Can't query with boolean filter on: class com.spikeify.entity.EntityOne#one, not a boolean field!", e.getMessage());
			throw e;
		}
	}

	public void createUniqueIndexIfItDoesNotExist(List<String> keys) throws InterruptedException {

		for (int i = 0; i < 10; i++) {
			for (String key : keys) {
				Thread.sleep(new Random().nextInt(100));
				if (sfy.query(UniqueIndex.class).filter("key", key).now().toList().size() == 0) {

					UniqueIndex obj = new UniqueIndex();
					obj.key = key;
					boolean created = false;
					do {
						obj.id = IdGenerator.generateKey();
						try {
							WritePolicy wp = new WritePolicy();
							wp.commitLevel = CommitLevel.COMMIT_ALL;

							sfy.create(obj).policy(wp).now();
							created = true;
						}
						catch (AerospikeException ignored) {
						}
					} while (!created);
				}
			}
		}
	}

	// This test is broken ... Aerospike does not ensure non duplicate index creation on non key fields
	@Ignore
	@Test
	public void testQueryingByIndex() throws InterruptedException {

		sfy.truncateNamespace(namespace);

		SpikeifyService.register(UniqueIndex.class);
		int WORKERS = 5;

		final List<String> keys = Arrays.asList("test1", "test2", "test3", "test4", "test5", "test6", "test7", "test8", "test9", "test10");
		// create threads
		Thread[] threads = new Thread[WORKERS];

		for (int i = 0; i < WORKERS; i++) {

			final int index = i;

			Runnable runnable = new Runnable() {
				@Override
				public void run() {

					try {
						createUniqueIndexIfItDoesNotExist(keys);
					}
					catch (InterruptedException ignored) {
					}
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

		List<UniqueIndex> list;
		int counter = 10;
		do {
			Thread.sleep(100);
			list = sfy.scanAll(UniqueIndex.class).now();
			System.out.println("list:" + list.size());
			counter--;
		} while (list.size() != 10 && counter > 0);

		Assert.assertEquals(10, list.size());
		for (String key : keys) {
			Assert.assertNotNull(sfy.query(UniqueIndex.class).filter("key", key).now().getFirst());
		}
		for (int i = 0; i < 10; i++) {
			for (String key : keys) {
				if (sfy.query(UniqueIndex.class).filter("key", key).now().toList().size() == 0) {
					UniqueIndex obj = new UniqueIndex();
					obj.key = key;
					boolean created = false;
					do {
						obj.id = IdGenerator.generateKey();
						try {
							sfy.create(obj).now();
							created = true;
						}
						catch (AerospikeException ignored) {
						}
					} while (!created);
				}
			}
		}
		Assert.assertEquals(list.size(), 10);
		for (String key : keys) {
			Assert.assertNotNull(sfy.query(UniqueIndex.class).filter("key", key).now().getFirst());
		}
	}

	@Test(expected = SpikeifyError.class)
	public void queryWithoutEntityRegister() {

		try {
			Spikeify sfy = this.sfy;
			sfy.query(UnregisteredIndex.class).filter("key", "test1").now();
		}
		catch (SpikeifyError e) {
			assertEquals("Must register entity class com.spikeify.QueryTest$UnregisteredIndex to allow quering!", e.getMessage());
			throw e;
		}
	}

	public static class UniqueIndex {

		@UserKey
		public String id;

		@Indexed
		public String key;
	}

	public static class UnregisteredIndex {

		@UserKey
		public String id;

		@Indexed
		public String key;
	}
}
