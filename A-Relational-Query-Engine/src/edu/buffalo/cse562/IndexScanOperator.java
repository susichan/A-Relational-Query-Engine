package edu.buffalo.cse562;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeSet;

import jdbm.PrimaryHashMap;
import jdbm.PrimaryMap;
import jdbm.PrimaryTreeMap;
import jdbm.RecordManager;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.schema.Table;

public class IndexScanOperator implements Operator {
	

	HashMap<String, LinkedHashMap<String,Schema>> tables;
	ArrayList<Tuple> returnTuppleList= new ArrayList<Tuple>();
	Table table;
	boolean flag=false;
	HashMap<String, LinkedHashMap<String,Schema>> currentTableStructures;
	HashMap<String, LinkedHashMap<String,Schema>> returnTableStructures;
	LinkedHashMap<String, Schema> columnStructure;
	Date lowerDate;
	Date upperDate;
	PrimaryTreeMap<Integer, ArrayList<Tuple>> indexTreeMap;
	PrimaryTreeMap<Integer, ArrayList<Tuple>> indexSubTreeMap;
	PrimaryTreeMap<Date,Integer> indexHashMap;
	Set<Integer> values;
	int upper;
	int lower;
	ArrayList<Date> dateList;
	Set <Date> tempSet;
	ArrayList<Date> datesList;
	ArrayList<Tuple> tempTuples = new ArrayList<Tuple>();
	boolean type;
	ArrayList<String> dateInString;
	//SortedMap<Integer, string>
	int index=0;
	int count=0;
	
	
	public IndexScanOperator(Table table,HashMap<String, LinkedHashMap<String,Schema>> tables,boolean multipleJoin,
			String writeDir,RecordManager recordManager, Expression expr,boolean flag,HashSet<String> projList) throws IOException, ParseException {
		
		long start = System.nanoTime();
		//System.out.println("Hello");
		this.flag=flag;
		this.tables=tables;
		this.table=table;
		returnTableStructures=new HashMap<String, LinkedHashMap<String,Schema>>();
		currentTableStructures=new HashMap<String, LinkedHashMap<String,Schema>>();
		columnStructure= new LinkedHashMap<String,Schema>();
		LinkedHashMap<String,Schema> returnStructure=new LinkedHashMap<String,Schema>();
		String tablename=table.getName().toLowerCase();
		if(table.getAlias()!=null){
			for (Map.Entry<String,Schema> entry : tables.get(table.getName()).entrySet()) {
				  String key = table.getAlias()+"."+entry.getKey();
				  Schema schema=entry.getValue();
				  columnStructure.put(key, schema);		
				}
			currentTableStructures.put(table.getAlias(), columnStructure);
		}
		else if(multipleJoin){
			for (Map.Entry<String,Schema> entry : tables.get(table.getName()).entrySet()) {
				  String key = table.getName()+"."+entry.getKey();
				  Schema schema=entry.getValue();
				  columnStructure.put(key, schema);		
				}
			currentTableStructures.put(tablename, columnStructure);
		}
		else{
			currentTableStructures.put(tablename, tables.get(tablename));
			columnStructure=currentTableStructures.get(tablename);

		}
		
		for(Map.Entry<String, Schema> etr:columnStructure.entrySet()){
			if(projList.contains(etr.getKey()))
				returnStructure.put(etr.getKey(), etr.getValue());
		}
		if(table.getAlias()!=null)
		returnTableStructures.put(table.getAlias(), returnStructure);
		else
		returnTableStructures.put(tablename, returnStructure);
		
	
			
			GetUpperAndLowerBounds obj = new GetUpperAndLowerBounds();
			Date[] Dates=obj.getDates(expr);
			upperDate = Dates[0];
			lowerDate=Dates[1];
			indexTreeMap= recordManager.treeMap(obj.getIndexColumn());
			indexHashMap=recordManager.treeMap(obj.getIndexColumn()+"Association");
			upper=indexHashMap.get(upperDate);
			lower=indexHashMap.get(lowerDate);
			//System.out.println("neat");

			/*
			System.out.println(lowerDate + "   "+ upperDate);*/		
			
			
			if(lowerDate==null)
			{
				indexSubTreeMap=(PrimaryTreeMap<Integer,ArrayList<Tuple>>) indexTreeMap.subMap(indexTreeMap.firstKey(), upper);
			}
			else if(upperDate==null)
			{
				indexSubTreeMap= (PrimaryTreeMap<Integer,ArrayList<Tuple>>) indexTreeMap.subMap(lower,indexTreeMap.lastKey());
			}
			else
			{
				indexSubTreeMap=(PrimaryTreeMap<Integer,ArrayList<Tuple>>) indexTreeMap.subMap(lower,upper);
			}
			
			//dateList.addAll(indexTreeTempMap.keySet());
			//System.out.println(keySet);
			//dateList = new ArrayList<>(indexTreeTempMap.keySete
			//dateList= indexTreeTempMap.keySet().toArray(new Date[0]);
			//dateList=new ArrayList<Date>(indexTreeTempMap.keySet());
			//dateInString = new ArrayList<String>(indexTreeTempMap.keySet());
			values=indexSubTreeMap.keySet();
			
			//System.out.println();
			for(int v:values)
			{
				tempTuples.addAll(indexSubTreeMap.get(v));
			}
			
			//System.out.println(tempTuples.size());
			//System.out.println(values.size());
			/*for(int i=0;i<731;i++)
				tempTuples=indexHashMap.get(values.get(i));*/
			//System.out.println((System.nanoTime()-start)/1000000000.0);
			//System.out.println("Bye");
			//System.out.println(values);
	}
	 
	
	public HashMap<String, LinkedHashMap<String,Schema>> getHashMap(){
		return returnTableStructures;
	}
	
	
	
	public Tuple readNextTuple()
	{
		//System.out.println("hi");
		
	/*	try{
			while(true)
			tempTuples.remove(0);
		}
		catch(Exception e)
		{
			System.out.println("Bhokat");
		return null;
		}*/
		try
		{	
			if(!tempTuples.isEmpty())
				return tempTuples.remove(0);
	
			return null;
		
		}
		catch (Exception e)
		{
			e.printStackTrace();
			return null;
		}
		
		//return null;
	}		
		
		
		//System.out.println("Hiiiiii");
		/*if(returnTuppleList.isEmpty()){
			try{
				returnTuppleList=new ArrayList<Tuple>(indexTreeTempMap.get(indexTreeTempMap.firstKey()));
				indexTreeTempMap.remove(indexTreeTempMap.firstKey());
			}
			catch(Exception e){
				return null;
			}
		
		}
		
		if(!returnTuppleList.isEmpty()){
			return returnTuppleList.remove(0);
		}
		
		
		
		return null;*/
	
	
		
		
	
	
	
	
	public void resetStream(){
		
		
	}
	
	public void close(){
		
	}


	@Override
	public String getTableName() {
		// TODO Auto-generated method stub
		if(table.getAlias()!=null)
			return table.getAlias();
		else
		return table.getName().toLowerCase();
	}
	
	
	
	public Table getTable(){
		return table;
	}


	@Override
	public String getSwapDir() {
		// TODO Auto-generated method stub
	return null;
	}


	@Override
	public long getTableSize() {
		// TODO Auto-generated method stub
		return 0;
	}
	
	public LinkedList<String> cSpit(String temp){
	LinkedList<String> tupleList=new LinkedList<>();
	int j=0;
	for(int i=j;i<temp.length();i++)
	{
		if(temp.charAt(i)=='|')
		{
			tupleList.add(temp.substring(j, i));
			j=i+1;
		}

	}
	
	tupleList.add(temp.substring(j, temp.length()));
	return tupleList;
	
	}
}
