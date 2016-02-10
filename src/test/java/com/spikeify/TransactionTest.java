package com.spikeify;

import com.aerospike.client.*;
import com.aerospike.client.policy.GenerationPolicy;
import com.aerospike.client.policy.WritePolicy;
import com.spikeify.annotations.Generation;
import com.spikeify.annotations.UserKey;
import com.spikeify.entity.EntityOne;
import com.spikeify.entity.EntityTx;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.*;

import static org.junit.Assert.assertTrue;

public class TransactionTest extends SpikeifyTest {

	private final Random random = new Random();
	private final String setName = "txSet";

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

					sfy.transact(15, new Work<EntityOne>() {
						@Override
						public EntityOne run() {

							EntityOne out = sfy.get(EntityOne.class).key(inKey).now();
							out.one++;
							sfy.update(out).now();
							return null;
						}
					});
				}
			});

			futures.add(future);
		}

		for (Future future : futures) {
			try {
				future.get();
			}
			catch (InterruptedException | ExecutionException e) {
				e.printStackTrace();
			}
		}

		EntityOne out = sfy.get(EntityOne.class).key(inKey).now();

		Assert.assertEquals(increaseCount, out.one);
		executor.shutdown();
	}

	@Test(expected = SpikeifyError.class)
	public void testTransactionsWithoutGenerationField() {

		// generate counter entity
		EntityTx in = new EntityTx();
		in.theSetName = setName;
		in.userId = random.nextLong();
		in.one = 0;

		final Key inKey = sfy.create(in).now();

		sfy.transact(15, new Work<EntityTx>() {
			@Override
			public EntityTx run() {

				EntityTx out = sfy.get(EntityTx.class).key(inKey).now();
				out.one++;
				sfy.update(out).now();
				return null;
			}
		});
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
						}
						catch (AerospikeException ae) {
							count++;
							try {
								Thread.sleep(10 + random.nextInt(5 * count));
							}
							catch (InterruptedException e) {
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
			}
			catch (InterruptedException | ExecutionException e) {
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
						}
						catch (AerospikeException ae) {
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
			}
			catch (InterruptedException | ExecutionException e) {
				e.printStackTrace();
			}
		}

		Record res = client.get(null, key);

		System.out.println("gen:" + res.generation + " one:" + res.bins.get("one"));
		System.out.println("counter:" + TestUtils.counter.get());
		System.out.println("duration:" + (System.currentTimeMillis() - start));

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
				}
				else {
					sfy.update(demoEntity).now();
				}
				return demoEntity;
			}
		});
		Assert.assertEquals(out.id, "1");
	}

	@Test
	public void testTransactionWhenWorkerThrowsException() {

		try {

			sfy.transact(5, new Work<DemoEntity>() {
				@Override
				public DemoEntity run() {

					throw new IllegalArgumentException("Bang!");
				}
			});

			assertTrue("Should not get here", false);
		}
		catch (IllegalArgumentException e) {
			// ok we got it ... let's try again
		}

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
				}
				else {
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

}
