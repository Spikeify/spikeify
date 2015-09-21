package com.spikeify.annotations;

import com.aerospike.client.query.IndexCollectionType;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.FIELD})
public @interface Indexed {

	/**
	 * @return Specific index name. Default is an automatically-generated value.
	 */
	String name() default "";

	/**
	 * @return Type of index. The default is IndexCollectionType.DEFAULT, indicating a scalar type.
	 */
	IndexCollectionType collection() default IndexCollectionType.DEFAULT;
}
