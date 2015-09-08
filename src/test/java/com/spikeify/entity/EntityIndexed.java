package com.spikeify.entity;

import com.aerospike.client.query.IndexCollectionType;
import com.spikeify.annotations.Generation;
import com.spikeify.annotations.Ignore;
import com.spikeify.annotations.Indexed;
import com.spikeify.annotations.UserKey;

import java.util.List;
import java.util.Map;

/**
 * For testing indexing service
 */
public class EntityIndexed {

	@UserKey
	@Indexed // should be ignored
	public String key;

	@Generation
	public Integer generation;

	// specific index name and type
	@Indexed(name = "index_number")
	public int number;

	@Indexed
	public boolean aboolean;

	@Indexed
	public long along;

	@Indexed
	public byte abyte;

	@Indexed
	public short ashort;

	@Indexed
	public double adouble;

	@Indexed
	public float afloat;

	@Indexed
	public Boolean aBoolean;

	@Indexed
	public Integer aInteger;

	@Indexed
	public Long aLong;

	@Indexed
	public Byte aByte;

	@Indexed
	public Short aShort;

	@Indexed
	public Float aFloat;

	@Indexed
	public Double aDouble;

/*	TODO: uncomment when supported
	@Indexed
	public BigDecimal aBigDecimal;

	@Indexed
	public BigInteger aBigInteger;

	@Indexed
	public Character aCharacter;

	@Indexed
	public char achar;
*/

	// default index type
	@Indexed
	public String text;


	@Indexed // (collection = IndexCollectionType.LIST) - is default
	public List<String> list;

	@Indexed // (collection = IndexCollectionType.MAPKEYS) - is default
	public Map<String, String> mapKeys;

	@Indexed(collection = IndexCollectionType.MAPVALUES)
	public Map<String, String> mapValues;

	@Ignore
	@Indexed // should be ignored
	public String ignored;
}
