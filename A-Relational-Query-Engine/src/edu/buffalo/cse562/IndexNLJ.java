package edu.buffalo.cse562;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import jdbm.PrimaryHashMap;
import jdbm.RecordManager;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.schema.Table;

public class IndexNLJ implements Operator{
	Operator input;
	RecordManager rm;
	String lookUpColumn;
	HashMap<String, LinkedHashMap<String,Schema>> joinHash;
	String tableName;
	int index;
	List<Tuple> tList;
	Tuple left_t;
	int listSize=0;
	int currentP=0;

	PrimaryHashMap<String, List<Tuple>> curHashMap;
	
	public IndexNLJ(Operator input,RecordManager rm,Expression expr,boolean left,HashMap<String,LinkedHashMap<String,Schema>> tables){
		this.input=input;
		this.rm=rm;
		tList=new ArrayList<>();
		LinkedHashMap<String,Schema> columnStructure=new LinkedHashMap<>();
		String lookupOtherCol=new String();
		if(expr instanceof EqualsTo){
			EqualsTo eqExpr=(EqualsTo)expr;
			if(left){
				 lookUpColumn=eqExpr.getLeftExpression().toString();
				 tableName=lookUpColumn.substring(0,lookUpColumn.indexOf(".")).toLowerCase();
				
				 for (Map.Entry<String,Schema> entry : tables.get(tableName).entrySet()) {
					  String key = tableName+"."+entry.getKey();
					  Schema schema=entry.getValue();
					  columnStructure.put(key, schema);		
					}
				 lookupOtherCol=eqExpr.getRightExpression().toString();
			}
			else{
				lookUpColumn=eqExpr.getRightExpression().toString();
				tableName=lookUpColumn.substring(0,lookUpColumn.indexOf(".")).toLowerCase();
				String actualTableName;
				if(tableName.equals("n1") || tableName.equals("n2"))
				actualTableName="nation";
				 else
				actualTableName=tableName;		 
				 
				 for (Map.Entry<String,Schema> entry : tables.get(actualTableName).entrySet()) {
					  String key = tableName+"."+entry.getKey();
					  Schema schema=entry.getValue();
					  columnStructure.put(key, schema);		
					}
				 
					 if(tableName.equals("n1") || tableName.equals("n2"))
						 lookUpColumn="nation.nationkey";
					 
				 lookupOtherCol=eqExpr.getLeftExpression().toString();
			}
			
		}
		List<String> tempList=new LinkedList<>(input.getHashMap().get(input.getTableName()).keySet());
		
		index=tempList.indexOf(lookupOtherCol);
		//joined HashMap
		tableName=input.getTableName()+"|"+tableName;
		joinHash=new HashMap<>();
		LinkedHashMap<String, Schema> joinColumnStrucutre=new LinkedHashMap<>();
		LinkedHashMap<String, Schema> leftCol=input.getHashMap().get(input.getTableName());
		LinkedHashMap<String, Schema> rightCol=new LinkedHashMap<>(columnStructure);
		for(Entry<String,Schema> etr:leftCol.entrySet())
			joinColumnStrucutre.put(etr.getKey(), etr.getValue());
		for(Entry<String,Schema> etr:rightCol.entrySet())
			joinColumnStrucutre.put(etr.getKey(), etr.getValue());
		joinHash.put(tableName, joinColumnStrucutre);
		curHashMap=rm.hashMap(lookUpColumn);
		//System.out.println(lookUpColumn);
	}

	@Override
	public Tuple readNextTuple() {
		// TODO Auto-generated method stub

		
		String lookup_key;
		
		if(currentP<listSize && left_t!=null){

			Tuple cur_Right=tList.get(currentP++);
			ArrayList<Object> tListObj=new ArrayList<Object>(left_t.getTuppleArray());
			tListObj.addAll(cur_Right.getTuppleArray());
			Tuple retTuple=new Tuple(tListObj);
			return retTuple;
		}
		
		do{
			
			if((left_t=input.readNextTuple())==null)
				return null;
			lookup_key=left_t.get(index).toString();
		}while(curHashMap.get(lookup_key)==null);
		
		 tList=curHashMap.get(lookup_key);
		 listSize=tList.size();
		 currentP=0;
		Tuple right_t=tList.get(currentP++);
		ArrayList<Object> tListObj=new ArrayList<Object>(left_t.getTuppleArray());
		tListObj.addAll(right_t.getTuppleArray());
		
		Tuple retTuple=new Tuple(tListObj);
		
		return retTuple;
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
		return joinHash;
	}

	@Override
	public Table getTable() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getTableName() {
		// TODO Auto-generated method stub
		return tableName;
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
