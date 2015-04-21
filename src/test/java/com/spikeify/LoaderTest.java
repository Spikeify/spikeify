package com.spikeify;

import com.aerospike.client.*;
import com.aerospike.client.policy.WritePolicy;
import com.spikeify.entity.EntityEnum;
import com.spikeify.entity.EntityOne;
import com.spikeify.mock.AerospikeClientMock;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.*;

public class LoaderTest {

	private Long userKey1 = new Random().nextLong();
	private Long userKey2 = new Random().nextLong();
	private String namespace = "test";
	private String setName = "testSet";
	private Spikeify sfy;
	private IAerospikeClient client;

	@Before
	public void dbSetup() {
		SpikeifyService.globalConfig(namespace, 3000, "localhost");
		client = new AerospikeClientMock(namespace);
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
	public void loadEntity() {

		int one = 123;
		String two = "a test";
		double three = 123.0d;
		float four = 123.0f;
		short five = (short) 234;
		byte six = (byte) 100;
		boolean seven = true;
		Date eight = new Date();
		List<String> nine = new ArrayList<>();
		nine.add("one");
		nine.add("two");
		EntityEnum eleven = EntityEnum.FIRST;

		Bin binOne = new Bin("one", one);
		Bin binTwo = new Bin("two", two);
		Bin binThree = new Bin("three", three);
		Bin binFour = new Bin("four", four);
		Bin binFive = new Bin("five", five);
		Bin binSix = new Bin("six", six);
		Bin binSeven = new Bin("seven", seven);
		Bin binEight = new Bin("eight", eight);
		Bin binNine = new Bin("nine", nine);
		Bin binEleven = new Bin("eleven", eleven.name());

		WritePolicy policy = new WritePolicy();
		policy.sendKey = true;

		String namespace = "test";
		String setName = "testSet";

		Key key = new Key(namespace, setName, userKey1);
		client.put(policy, key, binOne, binTwo, binThree, binFour, binFive, binSix, binSeven, binEight, binNine, binEleven);

		// testing default namespace - we did not explicitly provide namespace
		EntityOne entity = sfy.get(EntityOne.class).key(userKey1).namespace(namespace).set(setName).now();

		// UserKey value
		Assert.assertEquals(userKey1, entity.userId);

		// field values
		Assert.assertEquals(one, entity.one);
		Assert.assertEquals(two, entity.two);
		Assert.assertEquals(three, entity.three, 0.1);
		Assert.assertEquals(four, entity.four, 0.1);
		Assert.assertEquals(five, entity.getFive());
		Assert.assertEquals(six, entity.getSix());
		Assert.assertEquals(seven, entity.seven);
		Assert.assertEquals(eight, entity.eight);
		Assert.assertEquals(nine, entity.nine);
		Assert.assertEquals(eleven, entity.eleven);
	}

	@Test
	public void multiGet() {

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

		Long saveKey1 = sfy
				.update(userKey1, entity1)
				.namespace(namespace)
				.set(setName)
				.now();

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

		Long saveKey2 = sfy
				.update(userKey2, entity1)
				.set(setName)
				.now();

		Map<Long,EntityOne> result = sfy.getAll(EntityOne.class, saveKey1, saveKey2).namespace(namespace).set(setName).now();

		Assert.assertEquals(2, result.size());
		Assert.assertNotNull(result.get(saveKey1));
		Assert.assertNotNull(result.get(saveKey2));

		// UserKey value
		Assert.assertEquals(userKey1, result.get(saveKey1).userId);
		Assert.assertEquals(userKey2, result.get(saveKey2).userId);
	}

	@Test
	public void loadNonExisting(){
		EntityOne res = sfy.get(EntityOne.class).namespace(namespace).key(0l).now();
		Assert.assertNull(res);
	}

	@Test
	public void loadAllNonExisting(){
		Map<Long, EntityOne> recs = sfy.getAll(EntityOne.class, 0l, 1l).namespace(namespace).now();
		Assert.assertTrue(recs.isEmpty());

	}

}
