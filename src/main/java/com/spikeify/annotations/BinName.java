package com.spikeify.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Flags a field should be serialised to JSON
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.FIELD})
public @interface BinName {
	/**
	 * The bin name to which this filed is mapped
	 * @return Name of the bin. Default value is empty string indicating to use the field name as bin name.
	 */
	String value() default "";
}

