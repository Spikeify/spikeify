package com.spikeify;

import com.aerospike.client.*;
import com.aerospike.client.policy.WritePolicy;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.spikeify.entity.*;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.*;

@SuppressWarnings("ConstantConditions")
public class LoaderTest {

	private final Long userKey1 = new Random().nextLong();
	private final Long userKey2 = new Random().nextLong();
	private final String namespace = "test";
	private final String setName = "testSet";
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
		int unmapped1 = 555;
		String unmapped2 = "something";
		Float unmapped3 = 666.6f;

		EntitySub sub = new EntitySub(333, "something", new Date(1234567l));

		Bin binOne = new Bin("one", one);
		Bin binTwo = new Bin("two", two);
		Bin binThree = new Bin("third", three); // explicitly set bin name via @BinName annotation
		Bin binFour = new Bin("four", four);
		Bin binFive = new Bin("five", five);
		Bin binSix = new Bin("six", six);
		Bin binSeven = new Bin("seven", seven);
		Bin binEight = new Bin("eight", eight.getTime());
		Bin binNine = new Bin("nine", nine);
		Bin binEleven = new Bin("eleven", eleven.name());
		Bin binUnmapped1 = new Bin("unmapped1", unmapped1);
		Bin binUnmapped2 = new Bin("unmapped2", unmapped2);
		Bin binUnmapped3 = new Bin("unmapped3", unmapped3);

		// create a Bin containing JSON representation of EntitySub
		Bin binSub;
		try {
			String jsonValue = new ObjectMapper().writeValueAsString(sub);
			binSub = new Bin("sub", jsonValue);
		} catch (JsonProcessingException e) {
			throw new IllegalStateException(e);
		}

		WritePolicy policy = new WritePolicy();
		policy.sendKey = true;

		String namespace = "test";
		String setName = "testSet";

		Key key = new Key(namespace, setName, userKey1);
		client.put(policy, key, binOne, binTwo, binThree, binFour, binFive, binSix, binSeven, binEight,
				binNine, binEleven, binUnmapped1, binUnmapped2, binUnmapped3, binSub);

		// testing default namespace - we did not explicitly provide namespace
		EntityOne entity = sfy.get(EntityOne.class).key(userKey1).namespace(namespace).setName(setName).now();

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
		Assert.assertEquals((long) unmapped1, entity.unmapped.get("unmapped1"));
		Assert.assertEquals(unmapped2, entity.unmapped.get("unmapped2"));
		Assert.assertEquals(unmapped3, entity.unmapped.get("unmapped3"));
		Assert.assertEquals(sub.first, entity.sub.first);
		Assert.assertEquals(sub.second, entity.sub.second);
		Assert.assertNull(entity.sub.date); // JSON ignored field - deserialize to null

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
				.setName(setName)
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
				.setName(setName)
				.now();

		Map<Long, EntityOne> result = sfy.getAll(EntityOne.class, saveKey1, saveKey2).namespace(namespace).setName(setName).now();

		Assert.assertEquals(2, result.size());
		Assert.assertNotNull(result.get(saveKey1));
		Assert.assertNotNull(result.get(saveKey2));

		// UserKey value
		Assert.assertEquals(userKey1, result.get(saveKey1).userId);
		Assert.assertEquals(userKey2, result.get(saveKey2).userId);
	}

	@Test
	public void loadNonExisting() {
		EntityOne res = sfy.get(EntityOne.class).namespace(namespace).key(0l).now();
		Assert.assertNull(res);
	}

	@Test
	public void loadAllNonExisting() {
		Map<Long, EntityOne> recs = sfy.getAll(EntityOne.class, 0l, 1l).namespace(namespace).now();
		Assert.assertTrue(recs.isEmpty());
	}

	@Test
	public void mapEntity() {
		EntityOne original = TestUtils.randomEntityOne(setName);
		sfy.create(original).now();
		Key key = new Key(namespace, original.theSetName, original.userId);


		// load via Spikeify
		EntityOne loaded = sfy.get(EntityOne.class).key(key).now();
		Assert.assertEquals(original, loaded);

		// load natively an map
		Record loadedRecord = client.get(null, key);
		EntityOne loadedNative = sfy.map(EntityOne.class, key, loadedRecord);
		Assert.assertEquals(original, loadedNative);
		Assert.assertEquals(loaded, loadedNative);
	}


	@Test(expected = SpikeifyError.class)
	public void mapTooLongFieldName() {
		EntityTooLongFieldName ent = new EntityTooLongFieldName();
		ent.thisIsAFieldWithATooLongName = "something";
		sfy.create(123l, ent).now();
	}

	@Test(expected = SpikeifyError.class)
	public void mapTooLongBinName() {
		EntityTooLongBinName ent = new EntityTooLongBinName();
		ent.thisIsAFieldWithATooLongName = "something";
		sfy.create(123l, ent).now();
	}
}
