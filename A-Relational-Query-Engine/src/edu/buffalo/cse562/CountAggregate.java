package edu.buffalo.cse562;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import net.sf.jsqlparser.expression.Expression;

public class CountAggregate {
	Tuple groupTuple = new Tuple();
	Tuple currentTuple = new Tuple();
	int index;
	String columnName;
	static HashMap<String,HashSet<String>> distinctHashMap = new HashMap <String,HashSet<String>>();
	int tupleIndex=0;
	boolean distinctFlag=false;
	String keyInHashMap="";
	
	
	public CountAggregate(String TableName,String keyInHashMap,int tupleIndex,Expression expr,Tuple grouptuple,Tuple currentTuple,LinkedHashMap<String,Schema> queryHashMap,boolean distinctFlag)
	{
		//System.out.println("HIIIIIII");
		//System.out.println("groupTuple"+grouptuple.getTuppleArray());
		//System.out.println("CurrentTuple"+ currentTuple.getTuppleArray());
		//System.out.println(expr.toString());
		this.keyInHashMap=keyInHashMap;
		this.groupTuple = grouptuple;
		this.currentTuple=currentTuple;
		this.tupleIndex=tupleIndex;
		if(expr==null)
			columnName="*";
		else
			columnName=expr.toString();
		this.distinctFlag=distinctFlag;
		index= new LinkedList<String>(queryHashMap.keySet()).indexOf(columnName);
	}

	public void SetCount()
	{
		if(tupleIndex>=groupTuple.getTuppleArray().size())
			groupTuple.add(new Integer(0));
		int count=(int) Integer.parseInt(groupTuple.getTuppleArray().get(tupleIndex).toString());
		if(index==-1)
		{	
			//System.out.println("never come here");
			count ++;
			groupTuple.setValue(tupleIndex,(Integer)count);
			//System.out.println(groupTuple.getTuppleArray());
			return;
		}
			
		Object temp = currentTuple.getTuppleArray().get(index);
		if(temp!=null)
		{
				if(distinctFlag==true)
				{
					//System.out.println("I came here");
					if(distinctHashMap.containsKey(keyInHashMap+columnName))
					{
						if(!(distinctHashMap.get(keyInHashMap+columnName).contains(temp.toString())))
						{
							distinctHashMap.get(keyInHashMap+columnName).add(temp.toString());
							count=(int)Integer.parseInt(groupTuple.get(tupleIndex).toString())+1;
							groupTuple.setValue(tupleIndex,(Integer)count);
						return;
						}
					}
					else
					{
						HashSet<String> tempSet = new HashSet<String>();
						tempSet.add(temp.toString());
						distinctHashMap.put(keyInHashMap+columnName, tempSet);
						groupTuple.setValue(tupleIndex,(Integer)1);
						
					}
					
				}
				else
				{
					//System.out.println("never touch here");
					count=(int) Integer.parseInt(groupTuple.get(tupleIndex).toString())+1;
					groupTuple.setValue(tupleIndex,(Integer)count);
					return;
				}
		}
		
		//System.out.println(distinctHashMap.entrySet());
	
	}
	

}
