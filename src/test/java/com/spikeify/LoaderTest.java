package com.spikeify;

import com.aerospike.client.Bin;
import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.policy.WritePolicy;
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
		SpikeifyService.globalConfig("localhost", 3000, "test");
		client = new AerospikeClientMock();
		sfy = SpikeifyService.mock(client);
	}

	@After
	public void dbCleanup() {
		Key deleteKey1 = new Key(namespace, setName, userKey1);
		Key deleteKey2 = new Key(namespace, setName, userKey2);
		sfy.delete().key(deleteKey1).now();
		sfy.delete().key(deleteKey2).now();
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

		Bin binOne = new Bin("one", one);
		Bin binTwo = new Bin("two", two);
		Bin binThree = new Bin("three", three);
		Bin binFour = new Bin("four", four);
		Bin binFive = new Bin("five", five);
		Bin binSix = new Bin("six", six);
		Bin binSeven = new Bin("seven", seven);
		Bin binEight = new Bin("eight", eight);
		Bin binNine = new Bin("nine", nine);

		WritePolicy policy = new WritePolicy();
		policy.sendKey = true;

		String namespace = "test";
		String setName = "testSet";

		Key key = new Key(namespace, setName, userKey1);
		client.put(policy, key, binOne, binTwo, binThree, binFour, binFive, binSix, binSeven, binEight, binNine);

		// testing default namespace - we did not explicitly provide namespace
		EntityOne entity = sfy.get(EntityOne.class).key(userKey1).set(setName).now();

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

		Key saveKey1 = sfy
				.update(entity1)
				.namespace(namespace)
				.set(setName)
				.key(userKey1)
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

		Key saveKey2 = sfy
				.update(entity2)
				.namespace(namespace)
				.set(setName)
				.key(userKey2)
				.now();

		Map<Key, EntityOne> result = sfy.getAll(EntityOne.class).key(saveKey1, saveKey2).now();

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
		Map<Key, EntityOne> recs = sfy.getAll(EntityOne.class).namespace(namespace).key(0l, 1l).now();
		Assert.assertTrue(recs.isEmpty());

	}

}
