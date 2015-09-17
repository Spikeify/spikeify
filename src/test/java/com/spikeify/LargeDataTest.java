package com.spikeify;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Value;
import com.aerospike.client.large.LargeList;
import com.aerospike.client.policy.RecordExistsAction;
import com.aerospike.client.policy.WritePolicy;
import com.spikeify.entity.EntityLDT;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.*;

public class LargeDataTest {

	private final Long userKey1 = new Random().nextLong();
	private final String namespace = "test";
	private final String setName = "newTestSet";
	private Spikeify sfy;
	private AerospikeClient client;

	@Before
	public void dbSetup() {
		SpikeifyService.globalConfig(namespace, 3000, "localhost");
		sfy = SpikeifyService.sfy();
		client = new AerospikeClient("localhost", 3000);
	}

	@After
	public void dbCleanup() {
		sfy.truncateNamespace(namespace);
	}


	@Test
	public void testLargeList() {

		Random rnd = new Random();

		WritePolicy wp = new WritePolicy();
		wp.recordExistsAction = RecordExistsAction.UPDATE;

		Key key1 = new Key(namespace, setName, userKey1);

		int step = 100;

		Bin[] bins = new Bin[1];
		bins[0] = new Bin("one", rnd.nextLong());
		client.put(wp, key1, bins);

		LargeList llist = new LargeList(client, wp, key1, "lmap");

		for (int n = 0; n < 1; n++) {

			System.out.println("loop:" + n);
			List values = new ArrayList();
			for (int i = 0; i < step; i++) {
				int key = step * n + i;
				Map valmap = new HashMap();
				valmap.put("key", key);
				valmap.put("value", 1_000_000 + key);
				values.add(Value.get(valmap));
				if (i % 100 == 0) {
					System.out.println(" key:"+key);
					llist.add(values);
					values.clear();
				}
			}
			llist.add(values);
		}

		System.out.println("RANGE:");
		List range = llist.range(Value.get(15), Value.get(25));
		for (Object r : range) {
			System.out.print(" " + r);
		}
		System.out.println();

		System.out.println("find: " + llist.find(Value.get(25)));

		llist.remove(Value.get(25), Value.get(35));

//			System.out.println("capacity:" + lmap.getCapacity());

		System.out.println();
		System.out.println("----------------------------------");

		Map confmap = llist.getConfig();
		for (Object key : confmap.keySet()) {
			System.out.println(key + " : " + confmap.get(key));
		}
	}

	@Test
	public void testBigIndexedList() {

		Random rnd = new Random();

		WritePolicy wp = new WritePolicy();
		wp.recordExistsAction = RecordExistsAction.UPDATE;

		Key key1 = new Key(namespace, setName, userKey1);

		int step = 100;

		EntityLDT entity = new EntityLDT();
		entity.userId = userKey1;
		sfy.create(entity).now();

		System.out.println();

//		for (int n = 0; n < 1; n++) {
//
//			System.out.println("loop:" + n);
//			List values = new ArrayList();
//			for (int i = 0; i < step; i++) {
//				int key = step * n + i;
//				Map valmap = new HashMap();
//				valmap.put("key", key);
//				valmap.put("value", 1_000_000 + key);
//				values.add(Value.get(valmap));
//				if (i % 100 == 0) {
//					System.out.println(" key:"+key);
//					llist.add(values);
//					values.clear();
//				}
//			}
//			llist.add(values);
//		}
//
//		System.out.println("RANGE:");
//		List range = llist.range(Value.get(15), Value.get(25));
//		for (Object r : range) {
//			System.out.print(" " + r);
//		}
//		System.out.println();
//
//		System.out.println("find: " + llist.find(Value.get(25)));
//
//		llist.remove(Value.get(25), Value.get(35));
//
////			System.out.println("capacity:" + lmap.getCapacity());
//
//		System.out.println();
//		System.out.println("----------------------------------");
//
//		Map confmap = llist.getConfig();
//		for (Object key : confmap.keySet()) {
//			System.out.println(key + " : " + confmap.get(key));
//		}
	}


	@Test
	public void countRec() {
		int recCount = sfy.info().getRecordCount("test", "User");
		System.out.println("count:" + recCount);
	}
}
