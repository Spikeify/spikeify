package com.spikeify;

import com.aerospike.client.IAerospikeClient;
import com.spikeify.Spikeify;
import com.spikeify.SpikeifyService;
import com.spikeify.Work;
import com.spikeify.annotations.AsJson;
import com.spikeify.annotations.Generation;
import com.spikeify.annotations.SetName;
import com.spikeify.annotations.UserKey;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BugTest {
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

	/**
	 * If we try to call update on non-existing object in transaction wrapper, new object should be created,
	 * existing should be updated, .update function outside of transaction wrapper creates / update object in database it
	 * should do the same in transaction wrapper
	 */
	@Test
	public void testCreationOfEntityInsideTransactionViaUpdate() {
		DemoEntity out = sfy.transact(5, new Work<DemoEntity>() {
			@Override
			public DemoEntity run() {
				DemoEntity demoEntity = sfy.get(DemoEntity.class).key("1").now();
				if (demoEntity == null) {
					demoEntity = new DemoEntity();
					demoEntity.id = "1";
				}
				demoEntity.timestamp = System.currentTimeMillis();
				sfy.update(demoEntity).now();
				return demoEntity;
			}
		});
		Assert.assertEquals(out.id, "1");
	}

	/**
	 * If trying to call create inside transaction wrapper we should be able to create new object in database
	 */
	@Test
	public void testCreationOfEntityInsideTransactionViaCreate() {
		DemoEntity out = sfy.transact(5, new Work<DemoEntity>() {
			@Override
			public DemoEntity run() {
				DemoEntity demoEntity = sfy.get(DemoEntity.class).key("1").now();
				boolean _new = false;
				if (demoEntity == null) {
					demoEntity = new DemoEntity();
					demoEntity.id = "1";
					_new = true;
				}
				demoEntity.timestamp = System.currentTimeMillis();
				if (_new) {
					sfy.create(demoEntity).now();
				} else {
					sfy.update(demoEntity).now();
				}
				return demoEntity;
			}
		});
		Assert.assertEquals(out.id, "1");
	}

	public static class DemoEntity {
		@UserKey
		public String id;

		@Generation
		public Integer generation;

		public Long timestamp;
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
		sfy.create(demoMapAsJson).now();
		Assert.assertEquals(demoMapAsJson.data.get("test").timestamp, timestamp);

		DemoMapAsJson out = sfy.get(DemoMapAsJson.class).key("1").now();
		Assert.assertEquals(out.data.get("test").timestamp, timestamp);
	}

	public static class DemoMapAsJson {
		@UserKey
		public String id;

		@Generation
		public Integer generation;

		@AsJson
		public Map<String, CustomObject> data;

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

	/**
	 * Inner object of Map should support automatic conversion from Serializable object to mapped @AsJson
	 */
	@Test
	public void testSavingMapWithValuesAsJsonBeforeSerializable() {
		Long timestamp = System.currentTimeMillis();
		DemoMapValuesSerializable demoMapAsJson = new DemoMapValuesSerializable();
		demoMapAsJson.id = "1";
		demoMapAsJson.data = new HashMap<>();
		demoMapAsJson.data.put("test", new DemoMapValuesSerializable.CustomObject(timestamp));
		sfy.create(demoMapAsJson).now();
		Assert.assertEquals(demoMapAsJson.data.get("test").timestamp, timestamp);

		// still open as serializable as it was saved like that
		DemoMapValuesSerializable out = sfy.get(DemoMapValuesSerializable.class).key("1").now();
		Assert.assertEquals(out.data.get("test").timestamp, timestamp);

		// all serializable should be possible to parse even if we start using @AsJson and then be converted from serializable to JSON string on next write
		DemoMapValuesAsJson out2 = sfy.get(DemoMapValuesAsJson.class).setName(DemoMapValuesSerializable.class.getSimpleName()).key("1").now();
		Assert.assertNotNull(out2);
		Assert.assertEquals(out2.data.get("test").timestamp, timestamp);
	}

	public static class DemoMapValuesAsJson {
		@UserKey
		public String id;

		@Generation
		public Integer generation;

		public Map<String, CustomObject> data;

		@AsJson
		public static class CustomObject {
			public Long timestamp;

			public CustomObject() {
			}

			public CustomObject(Long timestamp) {
				this.timestamp = timestamp;
			}
		}
	}

	public static class DemoMapValuesSerializable {
		@UserKey
		public String id;

		@Generation
		public Integer generation;

		public Map<String, CustomObject> data;

		public static class CustomObject implements Serializable {
			public Long timestamp;

			public CustomObject() {
			}

			public CustomObject(Long timestamp) {
				this.timestamp = timestamp;
			}
		}
	}
}
