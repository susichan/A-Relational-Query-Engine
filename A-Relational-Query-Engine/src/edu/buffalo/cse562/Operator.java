package edu.buffalo.cse562;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;

import net.sf.jsqlparser.schema.Table;

public interface Operator {

	Tuple readNextTuple();
	void resetStream();
	void close();
	HashMap<String, LinkedHashMap<String,Schema>> getHashMap();
    Table getTable();
	String getTableName();
	String getSwapDir();
	long getTableSize();
}
