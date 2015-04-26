package com.spikeify;

import com.aerospike.client.*;
import com.aerospike.client.policy.GenerationPolicy;
import com.aerospike.client.policy.WritePolicy;
import com.spikeify.entity.EntityOne;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.*;

public class TransactionTest {

	private final Random random = new Random();
	private String namespace = "test";
	private String setName = "txSet";
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
	public void testTransactions() {

		EntityOne in = TestUtils.randomEntityOne(setName);
		in.one = 0;
		final Key inKey = sfy.create(in).now();

		ExecutorService executor = Executors.newFixedThreadPool(5, Executors.defaultThreadFactory());
		List<Future> futures = new ArrayList<>();

		int increaseCount = 1000;

		for (int x = 0; x < increaseCount; x++) {

			Future<?> future = executor.submit(new Runnable() {
				@Override
				public void run() {

					try {
						sfy.transact(15, new Work<EntityOne>() {
							@Override
							public EntityOne run() {
								EntityOne out = sfy.get(EntityOne.class).key(inKey).now();
								out.one++;
								sfy.update(out).now();
								return null;
							}
						});
					} catch (Exception e) {
						System.out.println(e.getMessage());
					}
				}
			});

			futures.add(future);
		}

		for (Future future : futures) {
			try {
				future.get();
			} catch (InterruptedException | ExecutionException e) {
				e.printStackTrace();
			}
		}

		EntityOne out = sfy.get(EntityOne.class).key(inKey).now();

		Assert.assertEquals(increaseCount, out.one);
		executor.shutdown();
	}

	public void testTransactionsDirect() {
		Long start = System.currentTimeMillis();

		EntityOne in = TestUtils.randomEntityOne(setName);
		in.one = 0;
		final Key inKey = sfy.create(in).now();

		ExecutorService executor = Executors.newFixedThreadPool(5, Executors.defaultThreadFactory());
		List<Future> futures = new ArrayList<>();

		// 100 threads
		for (int x = 0; x < 10000; x++) {

			futures.add(executor.submit(new Runnable() {
				@Override
				public void run() {

					int maxCount = 10;
					int count = 0;
					while (count < maxCount) {
						EntityOne out = sfy.get(EntityOne.class).key(inKey).now();
						out.one++;

						WritePolicy wp = new WritePolicy();
						wp.generation = out.generation;
						wp.generationPolicy = GenerationPolicy.EXPECT_GEN_EQUAL;
						try {
							TestUtils.counter.incrementAndGet();
							sfy.update(out).policy(wp).now();
							TestUtils.counter2.incrementAndGet();
							count = maxCount;
						} catch (AerospikeException ae) {
							count++;
							try {
								Thread.sleep(10 + random.nextInt(5 * count));
							} catch (InterruptedException e) {
								e.printStackTrace();
							}
							if (count == maxCount) {
								System.out.println("Error: could not update");
							}
						}
					}

				}
			}));
		}

		for (Future future : futures) {
			try {
				future.get();
			} catch (InterruptedException | ExecutionException e) {
				e.printStackTrace();
			}
		}

		EntityOne out = sfy.get(EntityOne.class).key(inKey).now();
		System.out.println("gen:" + out.generation + " one:" + out.one);
		System.out.println("counter:" + TestUtils.counter.get());
		System.out.println("counter2:" + TestUtils.counter2.get());
		System.out.println("duration:" + (System.currentTimeMillis() - start));

	}

	public void testTxNative() {
		Long start = System.currentTimeMillis();

		Bin one = new Bin("one", 0);
		final Key key = new Key(namespace, setName, "1234");
		client.put(null, key, one);

		ExecutorService executor = Executors.newFixedThreadPool(5, Executors.defaultThreadFactory());
		List<Future> futures = new ArrayList<>();

		for (int i = 0; i < 10000; i++) {

			futures.add(executor.submit(new Callable<Object>() {
				@Override
				public Object call() throws Exception {

					int maxCount = 10;
					int count = 0;
					while (count < maxCount) {
						Record rec = client.get(null, key);

						int val = ((Long) rec.bins.get("one")).intValue();
						Bin updated = new Bin("one", ++val);

						WritePolicy wp = new WritePolicy();
						wp.sendKey = true;
						wp.generation = rec.generation;
						wp.generationPolicy = GenerationPolicy.EXPECT_GEN_EQUAL;
						try {
							TestUtils.counter.incrementAndGet();
							client.put(wp, key, updated);
							count = maxCount;
						} catch (AerospikeException ae) {
							count++;
							Thread.sleep(10 + random.nextInt(5 * count));
							if (count == maxCount) {
								System.out.println("Error: could not update");
							}
						}
					}
					return null;
				}
			}));
		}

		for (Future future : futures) {
			try {
				future.get();
			} catch (InterruptedException | ExecutionException e) {
				e.printStackTrace();
			}
		}

		Record res = client.get(null, key);

		System.out.println("gen:" + res.generation + " one:" + res.bins.get("one"));
		System.out.println("counter:" + TestUtils.counter.get());
		System.out.println("duration:" + (System.currentTimeMillis() - start));

	}

}