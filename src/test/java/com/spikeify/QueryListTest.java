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
	public void testTooLongFieldNameForIndex() {

		SpikeifyService.register(LongEntity.class);

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
				.filter("listLong", 10L)
				.now()
				.toList();

		List<LongEntity> all = sfy.scanAll(LongEntity.class).now();

//		List<LongEntity> listLong = sfy.query(LongEntity.class)
//				.filter("listTooLongName", 10L)
//				.now()
//				.toList();

		assertEquals(2, listShort.size());

	}

}
