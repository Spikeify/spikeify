package com.spikeify;

import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.spikeify.entity.EntityParent;
import com.spikeify.entity.EntitySub;
import com.spikeify.entity.EntitySubJson;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.*;

@SuppressWarnings({"unchecked", "UnusedAssignment"})
public class JsonCOllectionTest {

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

	@SuppressWarnings("UnusedAssignment")
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

}
