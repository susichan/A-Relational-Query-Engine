package edu.buffalo.cse562;

import java.io.*;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.Map.Entry;

import net.sf.jsqlparser.schema.Table;

public class FileScanOperator implements Operator{
	
	BufferedReader input;
	String fileName,swapDir=null;
	HashMap<String, LinkedHashMap<String,Schema>> tables;
	Table table;
	HashMap<String, LinkedHashMap<String,Schema>> currentTableStructures;
	HashMap<String, LinkedHashMap<String,Schema>> returnTableStructures;
	LinkedHashMap<String, Schema> columnStructure;
	long table_size;
	HashSet<String> projList;


	


	public FileScanOperator(String fileName,Table table,HashMap<String, LinkedHashMap<String,Schema>> tables,boolean multipleJoin,String writeDir,HashSet<String> projList) throws IOException {
		swapDir=writeDir;
		File f=new File(fileName);
		table_size=f.length();
		
		input=new BufferedReader(new FileReader(fileName),1024*80);
		this.projList=projList;
		this.fileName=fileName;
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
	}
	 
	
	public HashMap<String, LinkedHashMap<String,Schema>> getHashMap(){
		return returnTableStructures;
	}
	
	
	
	public Tuple readNextTuple() {
	
	try {


		String temp=input.readLine();
		if(temp!=null)
		{
			String columnName;
			if(temp.charAt(temp.length()-1)=='|')
			temp=temp.substring(0,temp.length()-1);
			String tuppleLine[]=temp.split("\\|");
			int index=0;
			Tuple tuple = new Tuple();
			Iterator<String> iter=columnStructure.keySet().iterator();
			while(iter.hasNext())
			{
				columnName=iter.next();
				if(!projList.contains(columnName))
					index++;
				
				else{
				if(tuppleLine[index].equals(""))
					tuple.add(null);
				tuple.add(tuppleLine[index++]);
				}
				
			}
			
		    return tuple;
		}
		else
			return null;
	} catch (Exception e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
	return null;
	}
	
	
	
	public void resetStream(){
		try {
			input.close();
			input=new BufferedReader(new FileReader(fileName));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	public void close(){
		try {
			input.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
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
		return swapDir;
	}


	@Override
	public long getTableSize() {
		// TODO Auto-generated method stub
		return table_size;
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
