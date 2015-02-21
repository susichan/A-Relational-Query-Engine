package edu.buffalo.cse562;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;

public class ProjectOperator implements Operator{
	
	
	Operator input;
	List<SelectItem> columnNames;
	HashMap<String, LinkedHashMap<String, Schema>> globalHashMap;
	LinkedHashMap<String, Schema> modColumnStructure;
	boolean flag;
	String tableName;
	ProjectExpressionExtend1 projTemp;
	
	
	public ProjectOperator(List<Object> orderByList,List<Column> groupByList,List<SelectItem> columnNames,Operator input){
		this.input=input;
		//long start = System.nanoTime();
		this.columnNames=columnNames;
		boolean aggrFlag=false;
		List<SelectItem> temp=new LinkedList<SelectItem>();
		for(SelectItem sr:columnNames){
			SelectExpressionItem tempItem=(SelectExpressionItem)sr;
			if(tempItem.getExpression().getClass()==Function.class)
			{
				aggrFlag=true;
				temp.add(sr);
			}
		}
		if(aggrFlag || groupByList!=null){
			this.input=new GroupByOperator(groupByList, input, input.getHashMap(), temp);
			
		}
		if(orderByList!=null)
			this.input=new OrderOperator(this.input, this.input.getHashMap(), orderByList);

		globalHashMap=this.input.getHashMap();
		tableName=this.input.getTableName();
		ProjectExpressionExtend2 tempProject=new ProjectExpressionExtend2(globalHashMap,tableName);
		for(SelectItem sr:columnNames){
			if(sr.toString().equals("*")){
				flag=true;
				break;
			}
			else{
				sr.accept(tempProject);
			}
			if(modColumnStructure==null)
				modColumnStructure=tempProject.get();
		}

	 projTemp=new ProjectExpressionExtend1(this.globalHashMap,this.tableName);
	
	}
	
	public Tuple readNextTuple(){
		if(flag==true){
			Tuple t=new Tuple();
			if((t=input.readNextTuple())!=null)
				return t;
		}
		else{
			Tuple t=input.readNextTuple();
			if(t==null)
				return null;
			else{
				projTemp.clearTuple();
				projTemp.setTuple(t);
			for(SelectItem si:columnNames){
				si.accept(projTemp);
				
			}
			return projTemp.getTuple();
			
			}
		}
		
		return null;
		
		
	}
	
	
	
	public HashMap<String, LinkedHashMap<String,Schema>> getHashMap(){
		HashMap<String, LinkedHashMap<String,Schema>> newHashMap=new HashMap<String, LinkedHashMap<String,Schema>>(input.getHashMap());
		if(modColumnStructure!=null)
		newHashMap.put(input.getTableName(),modColumnStructure);
		else
			return input.getHashMap();
		return newHashMap;
	}
	
	public void resetStream(){
		input.resetStream();
	}
	
	public void close(){
		input.close();
	}
	
	public Table getTable(){
		return input.getTable();
	}
	
	public String getTableName(){
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
