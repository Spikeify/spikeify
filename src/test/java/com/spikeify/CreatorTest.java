package com.spikeify;

import com.aerospike.client.*;
import com.spikeify.entity.EntityOne;
import com.spikeify.mock.AerospikeClientMock;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Date;
import java.util.Map;
import java.util.Random;

public class CreatorTest {

	private Long userKey1 = new Random().nextLong();
	private Long userKey2 = new Random().nextLong();
	private String namespace = "test";
	private String setName = "newTestSet";
	private Spikeify sfy;
	private IAerospikeClient client;

	@Before
	public void dbSetup() {
		SpikeifyService.globalConfig("localhost", 3000, "test");
		client = new AerospikeClientMock();
		sfy = SpikeifyService.mock(client);
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
	public void simpleCreate() {

		EntityOne entity = new EntityOne();
		entity.one = 123;
		entity.two = "a test";
		entity.three = 123.0d;
		entity.four = 123.0f;
		entity.setFive((short) 234);
		entity.setSix((byte) 100);
		entity.seven = true;
		entity.eight = new Date(1420070400);
		entity.nine = new ArrayList<>();
		entity.nine.add("one");
		entity.nine.add("two");

		// set Metadata fields
		entity.userId = userKey1;
		entity.theSetName = setName;

		Key saveKey = sfy
				.create(entity)
				.now();

		// reload entity and check that only two properties were updated
		// we did not provide namespace on purpose - let default kick in
		EntityOne reloaded = sfy.get(EntityOne.class)
				.set(setName)
				.key(userKey1)
				.now();

		Assert.assertEquals(reloaded.one, 123);
		Assert.assertEquals(reloaded.two, "a test");
		Assert.assertEquals(reloaded.three, 123.0d, 0.1);
		Assert.assertEquals(reloaded.four, 123.0f, 0.1);
		Assert.assertEquals(reloaded.getFive(), 234);
		Assert.assertEquals(reloaded.getSix(), 100);
		Assert.assertEquals(reloaded.eight.getTime(), 1420070400);
		Assert.assertEquals(reloaded.userId, userKey1);

	}

	@Test
	public void multiCreate() {

		EntityOne entity1 = new EntityOne();
		entity1.one = 123;
		entity1.two = "a test";
		entity1.three = 123.0d;
		entity1.four = 123.0f;
		entity1.setFive((short) 234);
		entity1.setSix((byte) 100);
		entity1.seven = true;
		entity1.eight = new Date(1420070400);
		entity1.nine = new ArrayList<>();
		entity1.nine.add("one");
		entity1.nine.add("two");
		entity1.userId = userKey1;
		entity1.theSetName = setName;

		EntityOne entity2 = new EntityOne();
		entity2.one = 123;
		entity2.two = "a test";
		entity2.three = 123.0d;
		entity2.four = 123.0f;
		entity2.setFive((short) 234);
		entity2.setSix((byte) 100);
		entity2.seven = true;
		entity2.eight = new Date(1420070400);
		entity2.nine = new ArrayList<>();
		entity2.nine.add("one");
		entity2.nine.add("two");
		entity2.userId = userKey2;
		entity2.theSetName = setName;

		Map<Key,EntityOne> saveKeys = sfy
				.createAll(entity1, entity2)
				.now();

		// reload entity and check that only two properties were updated
		Map<Key, EntityOne> reloaded = sfy.getAll(EntityOne.class, userKey1, userKey2)
				.namespace(namespace)
				.set(setName)
				.now();

		Assert.assertEquals(2, saveKeys.size());
		Assert.assertEquals(2, reloaded.size());

	}

	@Test(expected = com.aerospike.client.AerospikeException.class)
	public void doubleCreate() {

		EntityOne entity = new EntityOne();
		entity.one = 123;
		entity.two = "a test";
		entity.three = 123.0d;
		entity.four = 123.0f;
		entity.setFive((short) 234);
		entity.setSix((byte) 100);
		entity.seven = true;
		entity.eight = new Date(1420070400);
		entity.nine = new ArrayList<>();
		entity.nine.add("one");
		entity.nine.add("two");

		sfy.create(userKey1, entity)
				.namespace(namespace)
				.set(setName)
				.now();

		sfy.create(userKey1, entity)
				.namespace(namespace)
				.set(setName)
				.now();
	}

	@Test
	public void createWIthEntityKeyOnly() {

		EntityOne entity = new EntityOne();
		entity.one = 123;
		entity.two = "a test";
		entity.three = 123.0d;
		entity.four = 123.0f;
		entity.setFive((short) 234);
		entity.setSix((byte) 100);
		entity.seven = true;
		entity.eight = new Date(1420070400);
		entity.nine = new ArrayList<>();
		entity.nine.add("one");
		entity.nine.add("two");

		Key saveKey = sfy
				.create(userKey1, entity)
				.namespace(namespace)
				.set(setName)
				.now();

		// reload entity and check that only two properties were updated
		EntityOne reloaded = sfy.get(EntityOne.class)
				.namespace(namespace)
				.set(setName)
				.key(userKey1)
				.now();

		Assert.assertEquals(reloaded.one, 123);
		Assert.assertEquals(reloaded.two, "a test");
		Assert.assertEquals(reloaded.three, 123.0d, 0.1);
		Assert.assertEquals(reloaded.four, 123.0f, 0.1);
		Assert.assertEquals(reloaded.getFive(), 234);
		Assert.assertEquals(reloaded.getSix(), 100);
		Assert.assertEquals(reloaded.eight.getTime(), 1420070400);
		Assert.assertEquals(reloaded.nine.size(), 2);
	}
}
