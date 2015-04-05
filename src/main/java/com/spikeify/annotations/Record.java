package com.spikeify.annotations;

import java.lang.annotation.*;

/**
 * Used on classes that you wish to map to DB records.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE})
@Inherited
public @interface Record
{
	/**
	 * The set name used in the DB.
	 */
	String set() default "";

}
