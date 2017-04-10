package com.spikeify;

import com.aerospike.client.Bin;
import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.query.IndexCollectionType;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.spikeify.annotations.AsJson;
import com.spikeify.annotations.Indexed;
import com.spikeify.annotations.UserKey;
import com.spikeify.entity.*;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@SuppressWarnings("ConstantConditions")
public class LoaderTest extends SpikeifyTest {

	private final Long userKey1 = new Random().nextLong();
	private final Long userKey2 = new Random().nextLong();
	private final String setName = "testSet";

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

		EntitySub sub = new EntitySub(333, "something", new Date(1234567L));

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
		assertEquals(userKey1, entity.userId);

		// field values
		assertEquals(one, entity.one);
		assertEquals(two, entity.two);
		assertEquals(three, entity.three, 0.1);
		assertEquals(four, entity.four, 0.1);
		assertEquals(five, entity.getFive());
		assertEquals(six, entity.getSix());
		assertEquals(seven, entity.seven);
		assertEquals(eight, entity.eight);
		assertEquals(nine, entity.nine);
		assertEquals(eleven, entity.eleven);
		assertEquals((long) unmapped1, entity.unmapped.get("unmapped1"));
		assertEquals(unmapped2, entity.unmapped.get("unmapped2"));
		assertEquals(unmapped3.floatValue(), (double)entity.unmapped.get("unmapped3"), 1.0f);
		assertEquals(sub.first, entity.sub.first);
		assertEquals(sub.second, entity.sub.second);
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

		assertEquals(2, result.size());
		Assert.assertNotNull(result.get(saveKey1));
		Assert.assertNotNull(result.get(saveKey2));

		// UserKey value
		assertEquals(userKey1, result.get(saveKey1).userId);
		assertEquals(userKey2, result.get(saveKey2).userId);
	}

	@Test
	public void loadNonExisting() {
		EntityOne res = sfy.get(EntityOne.class).namespace(namespace).key(0L).now();
		Assert.assertNull(res);
	}

	@Test
	public void loadAllNonExisting() {
		Map<Long, EntityOne> recs = sfy.getAll(EntityOne.class, 0L, 1L).namespace(namespace).now();
		assertTrue(recs.isEmpty());
	}

	@Test
	public void mapEntity() {
		EntityOne original = TestUtils.randomEntityOne(setName);
		sfy.create(original).now();
		Key key = new Key(namespace, original.theSetName, original.userId);

		// load via Spikeify
		EntityOne loaded = sfy.get(EntityOne.class).key(key).now();
		assertEquals(original, loaded);

		// load natively an map
		Record loadedRecord = client.get(null, key);
		EntityOne loadedNative = sfy.map(EntityOne.class, key, loadedRecord);
		assertEquals(original, loadedNative);
		assertEquals(loaded, loadedNative);
	}


	@Test(expected = SpikeifyError.class)
	public void mapTooLongFieldName() {
		EntityTooLongFieldName ent = new EntityTooLongFieldName();
		ent.thisIsAFieldWithATooLongName = "something";
		sfy.create(123L, ent).now();
	}

	@Test(expected = SpikeifyError.class)
	public void mapTooLongBinName() {
		EntityTooLongBinName ent = new EntityTooLongBinName();
		ent.thisIsAFieldWithATooLongName = "something";
		sfy.create(123L, ent).now();
	}

	public static class NormalObject {
		@UserKey
		public String id;

		public String something;
	}

	public static class NormalObjectChanged {
		@UserKey
		public String id;

		public String something;

		public SomethingEnum somethingEnum = SomethingEnum.somewhere;
	}

	public enum SomethingEnum {
		something,
		somewhere
	}

	@Test
	public void testReadingJavaDefinedDefaultValue() {
		NormalObject normalObject = new NormalObject();
		normalObject.id = "1";
		normalObject.something = "test";
		sfy.create(normalObject).now();

		NormalObjectChanged out = sfy.get(NormalObjectChanged.class).setName(NormalObject.class.getSimpleName()).key("1").now();
		Assert.assertEquals(out.id, "1");
		Assert.assertEquals(out.something, "test");
		Assert.assertEquals(out.somethingEnum, SomethingEnum.somewhere);
	}

	@Test
	public void testFieldWithMapAndValueAsJson() throws IOException {
		ObjectMapper objectMapper = new ObjectMapper();
		String jsonTest = "{\"platform\":\"ios\"}";
		JsonInfo out = objectMapper.readValue(jsonTest, objectMapper.getTypeFactory().constructType(JsonInfo.class));
		Assert.assertEquals(out.platform, Platform.ios);

		SpikeifyService.register(EntityWithJSON.class);
		EntityWithJSON obj = new EntityWithJSON();
		obj.id = "test1";
		obj.map = new HashMap<>();
		JsonInfo json = new JsonInfo();
		json.name = "name";
		json.value = "test";
		json.platform = Platform.ios;
		obj.map.put("HASH", json);
		sfy.create(obj).now();

		EntityWithJSON test = sfy.get(EntityWithJSON.class).key(obj.id).now();
		Assert.assertNotNull(test);
		Assert.assertNotNull(test.map);
		Assert.assertNotNull(test.map.get("HASH"));
		Assert.assertEquals(test.map.get("HASH").name, "name");
		Assert.assertEquals(test.map.get("HASH").value, "test");
		Assert.assertEquals(test.map.get("HASH").platform, Platform.ios);
		Assert.assertNull(test.map.get("HASH").bool);

		obj.map.get("HASH").platform = Platform.android;
		obj.map.get("HASH").name = "name2";
		obj.map.get("HASH").bool = true;
		sfy.update(obj).now();

		test = sfy.get(EntityWithJSON.class).key(obj.id).now();
		Assert.assertNotNull(test);
		Assert.assertNotNull(test.map);
		Assert.assertNotNull(test.map.get("HASH"));
		Assert.assertEquals(test.map.get("HASH").name, "name2");
		Assert.assertEquals(test.map.get("HASH").value, "test");
		Assert.assertEquals(test.map.get("HASH").platform, Platform.android);
		Assert.assertTrue(test.map.get("HASH").bool);
	}

	public static class EntityWithJSON {
		@UserKey
		public String id;

		@Indexed(collection = IndexCollectionType.MAPKEYS)
		public Map<String, JsonInfo> map;
	}

	@AsJson
	@JsonIgnoreProperties(ignoreUnknown = true)
	@JsonInclude(JsonInclude.Include.NON_NULL)
	public static class JsonInfo {
		public Platform platform;
		public String name;
		public String value;
		public Boolean bool;
	}

	public enum Platform {
		ios,
		android
	}
}
