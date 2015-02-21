package edu.buffalo.cse562;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Column;

public class SumAggregate {
	ArrayList<Tuple> tupleList= new ArrayList<Tuple>();
	LinkedHashMap<String, Schema> currHashMap;
	Expression expr;
	int tupleIndex=0;
	Tuple groupTuple = new Tuple();
	Tuple currentTuple = new Tuple();
	
	
	public SumAggregate(String TableName,int tupleIndex,Expression expr,Tuple groupTuple,Tuple CurrentTuple,LinkedHashMap<String, Schema> hashMap)
	{
		
		this.groupTuple = groupTuple;	
		this.currentTuple=CurrentTuple;
		currHashMap=hashMap;
		this.tupleIndex=tupleIndex;
		this.expr=expr;
	}

	public void SetSum() throws Exception
	{
		if(tupleIndex>=groupTuple.getTuppleArray().size())
			groupTuple.add(new Double(0.0));
		double sum=0;
		double currentSum = (double)Double.parseDouble(groupTuple.getTuppleArray().get(tupleIndex).toString());
		AgregateExpression aExpr;
		aExpr=new AgregateExpression(currHashMap,currentTuple);
		expr.accept(aExpr);
		sum=aExpr.getValue();
		currentSum= currentSum+sum;
		
		groupTuple.setValue(tupleIndex,(Double)currentSum);
		
	}
	

}