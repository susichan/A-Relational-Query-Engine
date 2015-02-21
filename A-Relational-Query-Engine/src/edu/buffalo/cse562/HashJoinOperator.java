package edu.buffalo.cse562;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.StringReader;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.zip.InflaterInputStream;

import javax.swing.plaf.basic.BasicInternalFrameTitlePane.MaximizeAction;







import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.parser.CCJSqlParserManager;


public class HashJoinOperator implements Operator{
	
	static int count=0;
	boolean swap;
	int k=0;
	int globalLeftHashBlockCounter=0;
	int globalRightTableCount=0;
	boolean writeFlag=false;
	

	boolean reload=true;
	List<Integer> leftColumnIds = new ArrayList<Integer>(); 
	//HashSet<String> primaryHashSet = new HashSet<String>();
	List<Integer> rightColumnIds = new ArrayList<Integer>(); 
	List<String> leftColumns = new ArrayList<String>(); 
	List<String> rightColumns = new ArrayList<String>(); 
	LinkedHashMap<String,Schema> leftTable= new LinkedHashMap<String,Schema>();
	LinkedHashMap<String,Schema> rightTable= new LinkedHashMap<String,Schema>();
	LinkedHashMap<String,ArrayList<Tuple>> hybridHashJoin=new LinkedHashMap<String,ArrayList<Tuple>>();
	LinkedHashMap<String,ArrayList<Tuple>> leftGlobalHashTable=new LinkedHashMap<String,ArrayList<Tuple>>();
	ArrayList<Tuple> rightTableBlock = new ArrayList<Tuple>();
	HashMap<Tuple,Operator> smJoin;
	boolean sortMerge;

	EqualsTo leftExp = null;
	EqualsTo rightExp = null;
	Operator right;
	Operator left;
	String leftCol;
	String rightCol;
	Tuple t ;
	HashMap<String,LinkedHashMap<String,Schema>> joinHashMap;
	ArrayList<Tuple> joinList = new ArrayList<Tuple>();
	

	public HashJoinOperator(Operator left ,Operator right , Expression expr)
	{  
		
		if(left.getTableSize()>right.getTableSize())
		{
			Operator temp=left;
			left=right;
			right=temp;
		}
		count++;
		this.right=right;
		this.left=left;
		if(left.getSwapDir()!=null)
		swap=true;
		leftTable=left.getHashMap().get(left.getTableName());
	
		leftColumns.addAll(leftTable.keySet());
		rightTable=right.getHashMap().get(right.getTableName());
		rightColumns.addAll(rightTable.keySet());
		
		
		if (expr.getClass()==AndExpression.class)
		{
			//Created Hybrid Hash
			AndExpression andExp = (AndExpression)expr;
			//left expression
			leftExp= (EqualsTo)andExp.getLeftExpression();
			leftCol=leftExp.getLeftExpression().toString();
			rightCol = leftExp.getRightExpression().toString();
			if(leftColumns.indexOf(leftCol)!=-1)
			leftColumnIds.add(leftColumns.indexOf(leftCol));
			else
			rightColumnIds.add(rightColumns.indexOf(leftCol));
			if(rightColumns.indexOf(rightCol)!=-1)
			rightColumnIds.add(rightColumns.indexOf(rightCol));
			else
			leftColumnIds.add(leftColumns.indexOf(rightCol));

			//rightExpresssion
			rightExp=(EqualsTo)andExp.getRightExpression();
			leftCol=rightExp.getLeftExpression().toString();
			rightCol = rightExp.getRightExpression().toString();
			if(leftColumns.indexOf(leftCol)!=-1)
			leftColumnIds.add(leftColumns.indexOf(leftCol));
			else
			rightColumnIds.add(rightColumns.indexOf(leftCol));
			if(rightColumns.indexOf(rightCol)!=-1)
			rightColumnIds.add(rightColumns.indexOf(rightCol));
			else
			leftColumnIds.add(leftColumns.indexOf(rightCol));
					
		}
		else
		{
		EqualsTo equalsExp = (EqualsTo)expr;
		leftCol= equalsExp.getLeftExpression().toString();
		rightCol = equalsExp.getRightExpression().toString();
		if(leftColumns.indexOf(leftCol)!=-1)
		leftColumnIds.add(leftColumns.indexOf(leftCol));
		else
		rightColumnIds.add(rightColumns.indexOf(leftCol));
		if(rightColumns.indexOf(rightCol)!=-1)
		rightColumnIds.add(rightColumns.indexOf(rightCol));
		else
		leftColumnIds.add(leftColumns.indexOf(rightCol));
		//Hybrid Hash Table for left table
		

		}
			
		while((t=left.readNextTuple())!=null)
			{
				//System.out.println(t.getTuppleArray());
				String key="";
				//System.out.println(leftColumnIds);
				for(int a:leftColumnIds)
				key=key+t.getTuppleArray().get(a).toString();
				if(hybridHashJoin.containsKey(key))
					hybridHashJoin.get(key).add(t);
				else
				{
					ArrayList<Tuple> valueList = new ArrayList<Tuple>();
					valueList.add(t);
					hybridHashJoin.put(key, valueList);
				}
			}
		
	
		
		//System.out.println("All blocks loaded");
		
		//System.out.println("Right block loaded in memory");
		
		String JoinTable=left.getTableName()+"|"+right.getTableName();
		LinkedHashMap<String,Schema> JoinTableColumns= new LinkedHashMap<String,Schema>();
		LinkedHashMap<String, Schema> leftTableCol=left.getHashMap().get(left.getTableName());
		LinkedHashMap<String, Schema> rightTableCol=right.getHashMap().get(right.getTableName());
		for (Map.Entry<String,Schema> entry : leftTableCol.entrySet())
			JoinTableColumns.put(entry.getKey(), entry.getValue());
		for (Map.Entry<String,Schema> entry : rightTableCol.entrySet())
			JoinTableColumns.put(entry.getKey(), entry.getValue());
		
		joinHashMap= new HashMap <String,LinkedHashMap<String,Schema>>(left.getHashMap());
		joinHashMap.put(JoinTable, JoinTableColumns);
		
				
	}
	
	@Override
	public Tuple readNextTuple() 
	{	
			Tuple returnTuple= new Tuple() ;
			ArrayList<Object> returnTupleList = new ArrayList<Object>();
			if(!joinList.isEmpty())
				{
					returnTupleList.addAll(joinList.get(0).getTuppleArray());
					joinList.remove(0);
					returnTupleList.addAll(t.getTuppleArray());
					returnTuple.setTuppleArray(returnTupleList);
					return returnTuple;
				}
			else
				{
					
					while((t=right.readNextTuple())!=null)
					{
						
						String key="";
						for(int a:rightColumnIds)
							key=key+t.getTuppleArray().get(a).toString();
						if(hybridHashJoin.containsKey(key))
						{
							joinList=new ArrayList<Tuple>(hybridHashJoin.get(key));
							returnTupleList.addAll(joinList.get(0).getTuppleArray());
							joinList.remove(0);
							returnTupleList.addAll(t.getTuppleArray());
							returnTuple.setTuppleArray(returnTupleList);
							return returnTuple;
						}
						else continue;
						
					}
					return null;
				
				
				}
				
		}
		
		
		
	
	
	
	@Override
	public void resetStream() {
		// TODO Auto-generated method stub
		left.resetStream();
		right.resetStream();
	}
	
	public void resetRightStream() {
		// TODO Auto-generated method stub
		right.resetStream();
	}
	

	@Override
	public void close() {
		// TODO Auto-generated method stub
		left.close();
		right.close();
	}

	

	@Override
	public String getTableName() {
		// TODO Auto-generated method stub
		return left.getTableName()+"|"+right.getTableName();
	}

	@Override
	public HashMap<String, LinkedHashMap<String,Schema>> getHashMap() {
		
			return joinHashMap;
		
	}

	@Override
	public Table getTable() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getSwapDir() {
		// TODO Auto-generated method stub
		return left.getSwapDir();
	}


	@Override
	public long getTableSize() {
		// TODO Auto-generated method stub
		if(left.getTableSize()>right.getTableSize())
			return left.getTableSize();
		else
			return right.getTableSize();
	}
	
	
	public Tuple getValue(Operator input){
		for(Entry<Tuple,Operator> etr:smJoin.entrySet()){
			if(etr.getValue()==input)
				return etr.getKey();
		}
		return null;
	}
	
	
	
	public int getLow(Object t1,Object t2){
		Integer i1=(Integer)Integer.parseInt(t1.toString());
		Integer i2=(Integer)Integer.parseInt(t2.toString());
		return i1.compareTo(i2);
	}
}
