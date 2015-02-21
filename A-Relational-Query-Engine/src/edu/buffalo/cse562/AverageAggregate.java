package edu.buffalo.cse562;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;

import net.sf.jsqlparser.expression.Expression;

public class AverageAggregate {
	LinkedHashMap<String,Schema>queryHashMap = new LinkedHashMap<String,Schema>();
	int index;
	int count=0;
	int tupleIndex=0;
	Tuple groupTuple = new Tuple();
	Tuple currentTuple = new Tuple();
	static HashMap<String,Integer> countMap = new HashMap<String,Integer>();
	
	public AverageAggregate(String TableName,String keyInHashMap,int tupleindex,Expression expr,Tuple groupTuple,Tuple currentTuple,LinkedHashMap<String,Schema> queryHashMap)
	{
		this.groupTuple = groupTuple;
		this.currentTuple=currentTuple;
		this.tupleIndex=tupleindex;
		index= new LinkedList<String>(queryHashMap.keySet()).indexOf(expr.toString());
		
		if(countMap.containsKey(keyInHashMap))
		{
			count=countMap.get(keyInHashMap);
			countMap.put(keyInHashMap, count+1);
		}
		else
		{
			count=0;
			countMap.put(keyInHashMap, 1);
		}
		
		
	}

	public void SetAverage() throws Exception
	{
		if(tupleIndex>=groupTuple.getTuppleArray().size())
		{
			groupTuple.add(new Double(0.0));
		}
		double average=(double) Double.parseDouble(groupTuple.getTuppleArray().get(tupleIndex).toString());
	
		if(index==-1)
			throw new Exception("No such column exists,Exception from Average Aggregate");
		average=(average*count+(double)Double.parseDouble(currentTuple.getTuppleArray().get(index).toString()))/++count;
		groupTuple.setValue(tupleIndex, (Double)average);
		
	
	}
	

}
