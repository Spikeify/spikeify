package com.spikeify;

import com.aerospike.client.*;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.query.*;
import com.spikeify.entity.EntityOne;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

public class QueryTest {

	private String namespace = "test";
	private String setName = "testSetQuery";
	private Spikeify sfy;
	private IAerospikeClient client;

	@Before
	public void dbSetup() {
		SpikeifyService.globalConfig(namespace, 3000, "localhost");
		client = SpikeifyService.getClient();
		sfy = SpikeifyService.sfy();
	}

	@After
	public void dbCleanup() {
		sfy.truncateNamespace(namespace);
	}

	@Test
	public void testEntityQuery() {

		client.createIndex(new Policy(), namespace, setName, setName + "_index", "second", IndexType.STRING);
		client.createIndex(new Policy(), namespace, setName, setName + "_index_long", "one", IndexType.NUMERIC);

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
				.indexName(setName + "_index")
				.setName(setName)
				.setFilters(Filter.equal("second", "content")) // explicitly set bin name via @BinName annotation
				.now();

		int count = 0;
		for (EntityOne entity : entities) {
			count++;
			Assert.assertEquals("content", entity.two);
		}
		Assert.assertEquals(10, count);


		ResultSet<EntityOne> entities2 = sfy.query(EntityOne.class)
				.indexName(setName + "_index")
				.setName(setName)
				.setFilters(Filter.equal("second", "content")) // explicitly set bin name via @BinName annotation
				.now();

		int count2 = 0;
		for (EntityOne entity2 : entities2) {
			count2++;
			Assert.assertTrue(0 < entity2.one && entity2.one < 1000);
		}
		Assert.assertEquals(10, count2);

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
		Assert.assertEquals(10, count);
	}

	@Test
	public void testListQuery() {

		String stringListIndex = "index_list_string_2";
		String binString = "nine";

		client.createIndex(new Policy(), namespace, setName, stringListIndex, binString, IndexType.STRING, IndexCollectionType.LIST);

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
			} else {
				sfy.create(entity.userId, entity).setName(setName).now();
			}

			count++;
		}

		ResultSet<EntityOne> results = sfy
				.query(EntityOne.class)
				.setName(setName)
				.indexName("krneki")
				.setFilters(Filter.contains(binString, IndexCollectionType.LIST, "content"))
				.now();


		int resultCount = 0;
		for (EntityOne result : results) {
			resultCount++;
		}

		// query should return 10 records
		Assert.assertEquals(50, resultCount);
	}

}
