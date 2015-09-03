package com.spikeify.entity;

import com.aerospike.client.query.IndexType;
import com.spikeify.annotations.Generation;
import com.spikeify.annotations.Indexed;
import com.spikeify.annotations.UserKey;

/**
 * For testing indexing service
 */
public class EntityIndexed2 {

	@UserKey
	@Indexed // should be ignored
	public String key;

	@Generation
	public Integer generation;

	// specific index name and type
	@Indexed(name = "index_number", type = IndexType.NUMERIC)
	public int someOtherField;
/*
	// default index type
	@Indexed
	public String text;

	@Indexed(collection = IndexCollectionType.LIST)
	public List<String> list;

	@Indexed(collection = IndexCollectionType.MAPKEYS)
	public Map<String, String> mapKeys;

	@Indexed(collection = IndexCollectionType.MAPVALUES)
	public Map<String, String> mapValues;

	@Ignore
	@Indexed // should be ignored
	public String ignored;*/
}
