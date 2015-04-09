//package com.spikeify;
//
//import org.junit.Assert;
//import org.junit.Test;
//
//import java.util.Map;
//
//public class PropertiesTest {
//
//	@Test
//	public void loadProps() {
//
//		EntityOne entityLoad = new EntityOne();
//		entityLoad.one = 123;
//		entityLoad.two = "a test";
//		entityLoad.three = 123.0d;
//		entityLoad.four = 123.0f;
//		entityLoad.setFive((short) 234);
//		entityLoad.setSix((byte) 100);
//		entityLoad.ignored = "should be ignored";
//
//		ClassMapper mapper = new ClassMapper(EntityOne.class);
//		Map<String, Object> props = mapper.getProperties(entityLoad);
//
//		Assert.assertEquals(123, props.get("one"));
//		Assert.assertEquals("a test", props.get("two"));
//		Assert.assertEquals(123.0d, props.get("three"));
//		Assert.assertEquals(123.0f, props.get("four"));
//		Assert.assertEquals((short) 234, props.get("five"));
//		Assert.assertEquals((byte) 100, props.get("six"));
//		Assert.assertFalse(props.containsKey("ignore"));
//
//		EntityOne entitySave = new EntityOne();
//		mapper.setFieldValues(entitySave, props);
//
//		// ignored field was not saved
//		Assert.assertNull(entitySave.ignored);
//
//		// set ignored field and compare with original
//		entitySave.ignored = "should be ignored";
//		Assert.assertEquals(entityLoad, entitySave);
//	}
//}
