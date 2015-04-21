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
import java.util.Random;

public class QueryTest {

	private String namespace = "test";
	private String setName = "testSetQuery";
	private Spikeify sfy;
	private IAerospikeClient client;

	@Before
	public void dbSetup() {
		SpikeifyService.globalConfig(namespace, 3000, "localhost");
		client = new AerospikeClient("localhost", 3000);
		sfy = SpikeifyService.sfy();
	}

	@After
	public void dbCleanup() {
		client.scanAll(null, namespace, setName, new ScanCallback() {
			@Override
			public void scanCallback(Key key, Record record) throws AerospikeException {
				client.delete(null, key);
			}
		});
	}

	@Test
	public void testEntityQuery() {

		client.createIndex(new Policy(), namespace, setName, setName + "_index", "two", IndexType.STRING);
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

		EntitySet<EntityOne> entities = sfy.query(EntityOne.class)
				.indexName(setName + "_index")
				.setName(setName)
				.setFilters(Filter.equal("two", "content"))
				.now();

		while (entities.next()) {
			Assert.assertEquals("content", entities.getObject().two);
		}

		EntitySet<EntityOne> entities2 = sfy.query(EntityOne.class)
				.indexName(setName + "_index")
				.setName(setName)
				.setFilters(Filter.equal("two", "content"))
				.now();

		while (entities2.next()) {
			EntityOne entity = entities2.getObject();
			Assert.assertTrue(0 < entity.one && entity.one < 1000);
		}
	}

	@Test
	public void testListQuery() {

		Random random = new Random();

		AerospikeClient client = new AerospikeClient("localhost", 3000);
		String namespace = "test";
		String setName = "testSetQuery";
		String stringListIndex = "index_list_string";
		String longListIndex = "index_list_long";
		String binString = "binString";
		String binLong = "binLong";

		client.createIndex(new Policy(), namespace, setName, stringListIndex, binString, IndexType.STRING, IndexCollectionType.LIST);
		client.createIndex(new Policy(), namespace, setName, longListIndex, binLong, IndexType.NUMERIC, IndexCollectionType.LIST);

		// create records
		for (int i = 0; i < 100; i++) {
			Key key = new Key("test", "testSetQuery", random.nextLong());
			Bin listStringBin;
			Bin listLongBin;


			List<String> listStrings = new ArrayList<>();
			List<Long> listLongs= new ArrayList<>();

			// subset of records have predictable bin values - so they can be found by query
			if (i % 10 == 0) {
				listStrings.add("content"); // fixed string
				listLongs.add((long) random.nextInt(100));  // predictable value 0-100
			}

			// random strings added to list
			listStrings.add(TestUtils.randomWord());
			listStrings.add(TestUtils.randomWord());
			listStringBin = new Bin(binString, listStrings);

			// random Longs added to list
			listLongs.add(random.nextLong());
			listLongs.add(random.nextLong());
			listLongBin = new Bin(binLong, listLongs);

			client.put(null, key, listStringBin, listLongBin);

		}

		// does DB need time to sync indexes?
		try {
			Thread.sleep(2000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		Statement statement = new Statement();
		statement.setNamespace(namespace);
		statement.setSetName(setName);
		statement.setIndexName(stringListIndex);
		statement.setFilters(Filter.equal(binString, "content"));

		RecordSet recSet = client.query(null, statement);

		int count = 0;
		while (recSet.next()) {
			count++;
			Record record = recSet.getRecord();
			System.out.println(record);
		}

		// query should return 10 records
		Assert.assertEquals(10, count);
	}
}
