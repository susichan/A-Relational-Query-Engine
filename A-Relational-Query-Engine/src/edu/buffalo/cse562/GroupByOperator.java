package edu.buffalo.cse562;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.TreeMap;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;

public class GroupByOperator implements Operator {
	
	
	
	
	Operator input;
	LinkedHashMap<String,Schema> queryHashMap= new LinkedHashMap<String,Schema>();
	LinkedHashMap<String,Schema> ChangeHashMap=new LinkedHashMap<String,Schema>();
	ArrayList <String>ColumnStrings= new ArrayList<String>();
	List<SelectItem> aggregateFunctions = new LinkedList<SelectItem>();
	List<Integer>indexList= new LinkedList<Integer>();
	TreeMap<String,Tuple> groupByHashMap = new TreeMap<String,Tuple> ();
	int tupleIndex=0;
	Tuple returnTuple= new Tuple();
	Tuple groupTuple = new Tuple();
	String tableName="";
	
	public GroupByOperator(List<Column> groupBycolumnNames,Operator input,HashMap<String,LinkedHashMap<String,Schema>> 
									GqueryHashMap,List<SelectItem> aggregateFunctions)
	{
		
		
		this.input = input;
		this.aggregateFunctions=aggregateFunctions;
		queryHashMap = GqueryHashMap.get(input.getTableName());
		ColumnStrings= new ArrayList<String>();
		Tuple t;
		this.tableName=input.getTableName();
			
		if(groupBycolumnNames!=null && !groupBycolumnNames.isEmpty())
			{
				for(Column col:groupBycolumnNames)
					ColumnStrings.add(col.toString());
				
				for(String key:queryHashMap.keySet())
				{
					if(ColumnStrings.contains(key))
						ChangeHashMap.put(key, queryHashMap.get(key));
				}
				
				List<String> columnNames= new ArrayList(queryHashMap.keySet());
				
				for(String str:columnNames)
					if(ColumnStrings.contains(str))
						indexList.add(columnNames.indexOf(str));
				
									
				while((t=input.readNextTuple())!=null)
						{
							String keyInHashMap = "";
							for(int i=0;i<indexList.size();i++)
								keyInHashMap=keyInHashMap+t.tuppleArray.get(indexList.get(i));
							if(groupByHashMap.containsKey(keyInHashMap))
							{
								tupleIndex=indexList.size();
								groupTuple=groupByHashMap.get(keyInHashMap);
								generateAggregates(groupTuple,t, tupleIndex,keyInHashMap);

								
								
							}
							else
							{
								tupleIndex=indexList.size();
								groupTuple= new Tuple();
							//	System.out.println("Index List" + indexList);
								for(int i:indexList)
									groupTuple.add(t.getTuppleArray().get(i));
							//	System.out.println("else group Tuple" +groupTuple.getTuppleArray());
								generateAggregates(groupTuple,t, tupleIndex,keyInHashMap);
								groupByHashMap.put(keyInHashMap, groupTuple);
								
								
								
						}
					
						}
				
			}
		
		else
		{
			while((t=input.readNextTuple())!=null)
			{
				tupleIndex=0;
				groupTuple= new Tuple();
				generateAggregates(t, groupTuple, tupleIndex,"All is Group");
			
			}
			
			groupByHashMap.put("All Group", groupTuple);
			
			
		}
		
		for (int i=0;i<aggregateFunctions.size();i++)
		{ 
		Function arg0=(Function)(((SelectExpressionItem)aggregateFunctions.get(i)).getExpression());
		if(((SelectExpressionItem)aggregateFunctions.get(i)).getAlias()!=null)
			ChangeHashMap.put(((SelectExpressionItem)aggregateFunctions.get(i)).getAlias(),Schema.FLOAT);
		else
			ChangeHashMap.put(arg0.getName()+arg0.getParameters(),Schema.FLOAT);
		}
	
			
		
	}

	
	public void generateAggregates(Tuple groupTuple,Tuple currentTuple,int tupleIndex,String keyInHashMap)
	{
		for (int i=0;i<aggregateFunctions.size();i++)
		{ 
		Function arg0=(Function)(((SelectExpressionItem)aggregateFunctions.get(i)).getExpression());
		String aggrName=arg0.getName();
		Expression exprtemp;
		if(arg0.getParameters()!=null)
		exprtemp=(Expression)arg0.getParameters().getExpressions().get(0);
		else
		exprtemp=null;
		boolean distinctFlag = arg0.isDistinct();
				if(aggrName.equalsIgnoreCase("count"))
					{
						CountAggregate countAggr = new CountAggregate(tableName,keyInHashMap,tupleIndex ,exprtemp,groupTuple,currentTuple, queryHashMap,distinctFlag);
						countAggr.SetCount();
						tupleIndex++;
					}
				else if(aggrName.equalsIgnoreCase("sum"))
				{
					try{
					SumAggregate sumAggr = new SumAggregate(tableName,tupleIndex,exprtemp, groupTuple,currentTuple,queryHashMap);
					sumAggr.SetSum();
					tupleIndex++;											
					}
					catch(Exception e)
					{
						e.printStackTrace();
					}
									
				}
				else if(aggrName.equalsIgnoreCase("avg"))
				{
					try
					{
					AverageAggregate avgAggregate = new AverageAggregate(tableName,keyInHashMap+aggregateFunctions.get(i).toString(), tupleIndex,exprtemp, groupTuple,currentTuple, queryHashMap);
					avgAggregate.SetAverage();
					tupleIndex++;
					}
					catch(Exception e)
					{
						e.printStackTrace();
					}
									
				}
		
			
		
		
	}
		
		
		
	}
	
	@Override
	public Tuple readNextTuple() {
		// TODO Auto-generated method stub
		try
		{
		return groupByHashMap.remove(groupByHashMap.firstKey());
		}
		catch(Exception e)
		{
		return null;
		}
		/*Iterator<String> groupByIterator = groupByHashMap.keySet().iterator();
		while(groupByIterator.hasNext())
		{
		returnTuple = groupByHashMap.get(groupByIterator.next());
		groupByIterator.remove();
		return returnTuple;
		}
		groupByHashMap=null;
		//System.gc();
		return null;*/
		
	}

	@Override
	public void resetStream() {
		// TODO Auto-generated method stub
		
		input.resetStream();
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub
		input.close();
	}

	@Override
	public HashMap<String, LinkedHashMap<String, Schema>> getHashMap() {
		// TODO Auto-generated method stub
		HashMap<String,LinkedHashMap<String,Schema>> changeQueryHashMap=new HashMap<String,LinkedHashMap<String,Schema>>(input.getHashMap());
		changeQueryHashMap.put(input.getTableName(), ChangeHashMap);
	//	System.out.println("keySet"+ChangeHashMap.keySet());
		return changeQueryHashMap;
	}

	@Override
	public Table getTable() {
		// TODO Auto-generated method stub
		return input.getTable();
	}

	@Override
	public String getTableName() {
		// TODO Auto-generated method stub
		return input.getTableName();
	}

	@Override
	public String getSwapDir() {
		// TODO Auto-generated method stub
		return input.getSwapDir();
	}


	@Override
	public long getTableSize() {
		// TODO Auto-generated method stub
		return input.getTableSize();
	}

}
