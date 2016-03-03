package com.spikeify;

import com.aerospike.client.query.IndexCollectionType;
import com.spikeify.annotations.BinName;
import com.spikeify.annotations.Generation;
import com.spikeify.annotations.Indexed;
import com.spikeify.annotations.UserKey;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class QueryListTest extends SpikeifyTest {


	public static class LongEntity {

		@UserKey
		public String id;

		@Generation
		public Integer generation;

		@Indexed
		@BinName("sKey")
		public String sourceBucketAndKey;

		@Indexed(collection = IndexCollectionType.LIST)
		@BinName("list")
		public List<Long> listLong = new ArrayList<>();
	}

	@Test
	public void testTranslatedFieldNameForListIndex() throws InterruptedException {

		SpikeifyService.register(LongEntity.class);
		Thread.sleep(3000); // make sure indexes are up to speed

		LongEntity test = new LongEntity();
		test.id = "1";
		test.listLong.add(10L);
		test.listLong.add(11L);
		test.listLong.add(12L);
		sfy.create(test).now();

		test = new LongEntity();
		test.id = "2";
		test.listLong.add(10L);
		test.listLong.add(20L);
		test.listLong.add(30L);
		sfy.create(test).now();

		test = new LongEntity();
		test.id = "3";
		test.listLong.add(11L);
		test.listLong.add(21L);
		test.listLong.add(31L);
		sfy.create(test).now();

		// this filter by actual field name should be replaced with name in annotation @BinName
		List<LongEntity> listShort = sfy.query(LongEntity.class)
				.filter("list", 10L)  // finds records if translated name "list" is used
				.now()
				.toList();

		assertEquals(2, listShort.size());

		List<LongEntity> listLong = sfy.query(LongEntity.class)
				.filter("listLong", 11L)  // finds data also if original field name is used
				.now()
				.toList();
		assertEquals(2, listLong.size());



	}

}
