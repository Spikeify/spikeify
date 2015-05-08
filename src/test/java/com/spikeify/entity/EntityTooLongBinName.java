package com.spikeify.entity;

import com.spikeify.annotations.BinName;

public class EntityTooLongBinName {

	@BinName("thisBinNameIsAlsoTooLong")
	public String thisIsAFieldWithATooLongName;
}
