package com.spikeify;

import com.aerospike.client.Bin;
import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.spikeify.annotations.AsJson;
import com.spikeify.annotations.Generation;
import com.spikeify.annotations.UserKey;
import com.spikeify.entity.EntityParent;
import com.spikeify.entity.EntitySub;
import com.spikeify.entity.EntitySubJson;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.*;

/**
 * A test where Map or List contains classes marked with @AsJson
 */
@SuppressWarnings({"unchecked", "UnusedAssignment"})
public class JsonCollectionsTest {

	private final String userKey1 = String.valueOf(new Random().nextLong());
	private final String namespace = "test";
	private Spikeify sfy;
	private IAerospikeClient client;

	@Before
	public void dbSetup() {
		SpikeifyService.globalConfig(namespace, 3000, "localhost");
		sfy = SpikeifyService.sfy();
		client = SpikeifyService.getClient();
	}

	@After
	public void dbCleanup() {
		sfy.truncateNamespace(namespace);
	}

	@Test
	public void create() throws IOException {

		EntityParent entity = new EntityParent();
		entity.jsonMap = new HashMap<>();
		entity.jsonMap.put("one", new EntitySub(1, "one", new Date(1000)));
		entity.jsonMap.put("two", new EntitySub(2, "two", new Date(2000)));
		entity.jsonMap.put("three", new EntitySub(3, "three", new Date(3000)));
		entity.map = new HashMap<>();
		entity.map.put("one", new EntitySubJson(1, "one", new Date(1000)));
		entity.map.put("two", new EntitySubJson(2, "two", new Date(2000)));
		entity.map.put("three", new EntitySubJson(3, "three", new Date(3000)));

		entity.jsonList = new ArrayList<>();
		entity.jsonList.add(new EntitySub(11, "one", new Date(11000)));
		entity.jsonList.add(new EntitySub(12, "two", new Date(12000)));
		entity.jsonList.add(new EntitySub(13, "three", new Date(13000)));
		entity.list = new ArrayList<>();
		entity.list.add(new EntitySubJson(11, "one", new Date(11000)));
		entity.list.add(new EntitySubJson(12, "two", new Date(12000)));
		entity.list.add(new EntitySubJson(13, "three", new Date(13000)));
		// set Metadata fields
		entity.userId = userKey1;

		Key saveKey = sfy
				.create(entity)
				.now();

		// reload entity and check that only two properties were updated
		// we did not provide namespace on purpose - let default kick in
		Record record = client.get(null, saveKey);

		ObjectMapper objMapper = new ObjectMapper();

		Assert.assertEquals(record.bins.size(), 4);
		Assert.assertEquals(objMapper.writeValueAsString(entity.jsonMap), record.bins.get("jsonMap"));
		Map propertyMap = (Map) record.bins.get("map");
		for (String key : entity.map.keySet()) {
			EntitySubJson original = entity.map.get(key);
			EntitySubJson saved = objMapper.readValue((String) propertyMap.get(key), EntitySubJson.class);
			Assert.assertTrue(propertyMap.containsKey(key));
			Assert.assertEquals(original.first, saved.first);
			Assert.assertEquals(original.second, saved.second);
			Assert.assertNull(saved.date); // EntitySubJson.date was excluded from JSON serialization
		}

		Assert.assertEquals(objMapper.writeValueAsString(entity.jsonList), record.bins.get("jsonList"));
		List<String> propertyList = (List) record.bins.get("list");
		Assert.assertEquals(entity.list.size(), propertyList.size());
		for (int i = 0; i < propertyList.size(); i++) {
			String item = propertyList.get(i);
			EntitySubJson saved = objMapper.readValue(item, EntitySubJson.class);
			Assert.assertEquals(entity.list.get(i).first, saved.first);
			Assert.assertEquals(entity.list.get(i).second, saved.second);
			Assert.assertNull(saved.date);  // EntitySubJson.date was excluded from JSON serialization
		}
	}

	/**
	 * Backwards compatibility test - checks the fallback if @AsJson marked collection classes were
	 * previously saved without @AsJson via plain Java serialization
	 *
	 * @throws IOException
	 */
	@Test
	public void loadLegacySerializedObjects() throws IOException {

		EntityParent entity = new EntityParent();
		entity.jsonMap = new HashMap<>();
		entity.jsonMap.put("one", new EntitySub(1, "one", new Date(1000)));
		entity.jsonMap.put("two", new EntitySub(2, "two", new Date(2000)));
		entity.jsonMap.put("three", new EntitySub(3, "three", new Date(3000)));
		entity.map = new HashMap<>();
		entity.map.put("one", new EntitySubJson(1, "one", new Date(1000)));
		entity.map.put("two", new EntitySubJson(2, "two", new Date(2000)));
		entity.map.put("three", new EntitySubJson(3, "three", new Date(3000)));

		entity.jsonList = new ArrayList<>();
		entity.jsonList.add(new EntitySub(11, "one", new Date(11000)));
		entity.jsonList.add(new EntitySub(12, "two", new Date(12000)));
		entity.jsonList.add(new EntitySub(13, "three", new Date(13000)));
		entity.list = new ArrayList<>();
		entity.list.add(new EntitySubJson(11, "one", new Date(11000)));
		entity.list.add(new EntitySubJson(12, "two", new Date(12000)));
		entity.list.add(new EntitySubJson(13, "three", new Date(13000)));
		// set Metadata fields
		entity.userId = userKey1;

		Key saveKey = sfy
				.create(entity)
				.now();

		Record record = client.get(null, saveKey);
		List listBin = (List) record.bins.get("list");
		for (Object listEntry : listBin) {
			Assert.assertTrue(listEntry instanceof String);
		}
		Map mapBin = (Map) record.bins.get("map");
		for (Object mapValue : mapBin.values()) {
			Assert.assertTrue(mapValue instanceof String);
		}

		// change a "list" bin
		List newList = new ArrayList<>();
		newList.add(new EntitySubJson(11, "one", new Date(11000)));
		newList.add(new EntitySubJson(12, "two", new Date(12000)));
		newList.add(new EntitySubJson(13, "three", new Date(13000)));
		//
		Map newMap = new HashMap<>();
		newMap.put("one", new EntitySubJson(1, "one", new Date(1000)));
		newMap.put("two", new EntitySubJson(2, "two", new Date(2000)));
		newMap.put("three", new EntitySubJson(3, "three", new Date(3000)));

		record.bins.put("list", newList);
		record.bins.put("map", newMap);

		Bin[] bins = new Bin[record.bins.size()];
		int pos = 0;
		for (String binName : record.bins.keySet()) {
			bins[pos++] = new Bin(binName, record.bins.get(binName));
		}

		// save natively - the "list" bin should be saved as serialized java object
		client.put(null, saveKey, bins);

		// reload as raw to check that values were saved as serialized POJOs
		Record recordChanged = client.get(null, saveKey);
		List pojoListBin = (List) recordChanged.bins.get("list");
		for (Object listEntry : pojoListBin) {
			Assert.assertTrue(listEntry instanceof EntitySubJson);
		}
		Map pojoMapBin = (Map) record.bins.get("map");
		for (Object mapValue : pojoMapBin.values()) {
			Assert.assertTrue(mapValue instanceof EntitySubJson);
		}

		// reload via Spikeify - Map/List values will be correctly loaded, regardless if thay are stored
		// as POJOs or JSON Strings
		EntityParent reloaded = sfy.get(EntityParent.class).key(saveKey).now();

		Assert.assertEquals(entity.list, reloaded.list);
		Assert.assertEquals(entity.map, reloaded.map);
	}

	/**
	 * If annotation @AsJson is on Map field should be serialized to JSON string and mapped as
	 * it would be mapped when normal custom field is used
	 */
	@Test
	public void testSavingMapAsJson() {
		Long timestamp = System.currentTimeMillis();
		DemoMapAsJson demoMapAsJson = new DemoMapAsJson();
		demoMapAsJson.id = "1";
		demoMapAsJson.data = new HashMap<>();
		demoMapAsJson.data.put("test", new DemoMapAsJson.CustomObject(timestamp));
		demoMapAsJson.listData = new HashMap<>();
		List<String> listData = new ArrayList<>();
		listData.add("krneki");
		listData.add("seneki");
		demoMapAsJson.listData.put("one", listData);
		Key saveKey = sfy.create(demoMapAsJson).now();
		Assert.assertEquals(demoMapAsJson.data.get("test").timestamp, timestamp);

		Record record = client.get(null, saveKey);

		DemoMapAsJson out = sfy.get(DemoMapAsJson.class).key("1").now();
		Assert.assertEquals(out.data.get("test").timestamp, timestamp);
		Assert.assertEquals(out.listData.get("one").get(0), "krneki");
		Assert.assertEquals(out.listData.get("one").get(1), "seneki");
	}

	public static class DemoMapAsJson {
		@UserKey
		public String id;

		@Generation
		public Integer generation;

		@AsJson
		public Map<String, CustomObject> data;

		@AsJson
		public Map<String, List<String>> listData;

		public static class CustomObject {
			public Long timestamp;

			public CustomObject() {
			}

			public CustomObject(Long timestamp) {
				this.timestamp = timestamp;
			}
		}
	}

	/**
	 * If annotation @AsJson is on List field should be serialized to JSON string and mapped as
	 * it would be mapped when normal custom field is used
	 */
	@Test
	public void testSavingListAsJson() {
		Long timestamp = System.currentTimeMillis();
		DemoListAsJson demoListAsJson = new DemoListAsJson();
		demoListAsJson.id = "1";
		demoListAsJson.data = new ArrayList<>();
		demoListAsJson.data.add(new DemoListAsJson.CustomObject(timestamp));
		sfy.create(demoListAsJson).now();
		Assert.assertEquals(demoListAsJson.data.get(0).timestamp, timestamp);

		DemoListAsJson out = sfy.get(DemoListAsJson.class).key("1").now();
		Assert.assertEquals(out.data.get(0).timestamp, timestamp);
	}

	public static class DemoListAsJson {
		@UserKey
		public String id;

		@Generation
		public Integer generation;

		@AsJson
		public List<CustomObject> data;

		public static class CustomObject {
			public Long timestamp;

			public CustomObject() {
			}

			public CustomObject(Long timestamp) {
				this.timestamp = timestamp;
			}
		}
	}

}
