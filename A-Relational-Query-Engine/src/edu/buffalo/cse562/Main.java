package edu.buffalo.cse562;




import java.io.*;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import jdbm.PrimaryHashMap;
import jdbm.PrimaryMap;
import jdbm.PrimaryTreeMap;
import jdbm.RecordManager;
import jdbm.RecordManagerFactory;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.parser.ParseException;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.create.table.Index;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.statement.select.SelectItem;

public class Main {

	static HashSet<String> indexHashSet = new HashSet<>();
	/**
	 * @param args
	 * @throws IOException 
	 * @throws ClassNotFoundException 
	 * @throws java.text.ParseException 
	 */
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, java.text.ParseException {
		// TODO Auto-generated method st
		//long start=System.nanoTime();
		String dataDir=null,swapDir=null,indexDir=null;
		boolean build=false;
		HashMap<String, LinkedHashMap<String,Schema>> tables=new HashMap<String, LinkedHashMap<String,Schema>>();

		ArrayList<File> sqlFiles=new ArrayList<File>();
		
		
		for (int i=0;i<args.length;i++){
			if(args[i].trim().equals("--data")){
				dataDir=args[i+1];
				i++;
			}
			else if(args[i].trim().equals("--index")){
				indexDir=args[i+1];
				i++;
			}
			else if(args[i].trim().equals("--build")){
				build=true;
			}
			else if(args[i].trim().equals("--swap")){
				swapDir=args[i+1];
				i++;
			}
			else{
				sqlFiles.add(new File(args[i]));
			}
		}
		
		RecordManager recMan=null;
		if(indexDir!=null && !build){
			recMan=new RecordManagerFactory().createRecordManager(indexDir+File.separator+"BullsEyeIndex");
			

			FileInputStream hashSetInput = new FileInputStream(indexDir+File.separator+"HashSet.txt");
			ObjectInputStream hashSetObject = new ObjectInputStream(hashSetInput);
			indexHashSet=(HashSet<String>) hashSetObject.readObject();
	
		}
		
		if(build){
			recMan= new RecordManagerFactory().createRecordManager(indexDir+File.separator+"BullsEyeIndex");
		}
		
		
		
		for(File sql: sqlFiles){
			try{
				FileReader stream= new FileReader(sql);
				CCJSqlParser parser=new CCJSqlParser(stream);
				Statement stmt;
				while((stmt=parser.Statement())!=null){

					LinkedHashMap<String, Schema> colDef=new LinkedHashMap<String, Schema>();
					if(stmt instanceof CreateTable){
						CreateTable table=(CreateTable)stmt;
						for(ColumnDefinition cdef:(List<ColumnDefinition>)table.getColumnDefinitions())
						{
							if(cdef.getColDataType().getDataType().equalsIgnoreCase("int"))
									{
										colDef.put(cdef.getColumnName(), Schema.INT);
									}
							else if(cdef.getColDataType().getDataType().equalsIgnoreCase("string"))
									{
										colDef.put(cdef.getColumnName(), Schema.STRING);
									}
							else if(cdef.getColDataType().getDataType().equalsIgnoreCase("date"))
									{
										colDef.put(cdef.getColumnName(), Schema.DATE);
									}
							else if(cdef.getColDataType().getDataType().matches("VARCHAR(.*)") || cdef.getColDataType().getDataType().matches("CHAR(.*)"))
									{
										colDef.put(cdef.getColumnName(), Schema.STRING);
									}
							else if(cdef.getColDataType().getDataType().equalsIgnoreCase("decimal"))
									{
										colDef.put(cdef.getColumnName(), Schema.FLOAT);
									}
							else if(cdef.getColDataType().getDataType().equalsIgnoreCase("float"))
									{
										colDef.put(cdef.getColumnName(), Schema.FLOAT);
									}
							else if(cdef.getColDataType().getDataType().matches("bool(.*)"))
									{
										colDef.put(cdef.getColumnName(), Schema.BOOL);
									}
							
						}
						
						tables.put(table.getTable().getName().toLowerCase(),colDef);
					if(build){
						LinkedList<String> tempColList=new LinkedList<>(colDef.keySet());
						for(Object index:table.getIndexes()){
							
								boolean treemap=false;
							Index tempIndex=(Index)index;
							List<String> indexList=tempIndex.getColumnsNames();
							List<Integer> indexInteger=new ArrayList<>();
							for(String str:indexList){
								if(str.contains("date"))
									treemap=true;
								indexInteger.add(tempColList.indexOf(str));
							}
							if(indexList.size()<=1)
							createIndexBullsEye(recMan,indexList,table.getTable().getName(),indexInteger,dataDir+File.separator+table.getTable().getName()+".dat",indexDir,treemap);
							//System.out.print(indexList+" "+indexInteger+" "+treemap);
							
						}
						//System.out.println();
					}

					}
					else if(stmt instanceof Select){
						if(build)
							continue;
						Select statement = (Select) stmt;
                        SelectBody slctBody = (SelectBody)statement.getSelectBody(); 
                        SelectDeParserExtend treeOperation=new SelectDeParserExtend(tables,dataDir.toString(),swapDir,recMan,indexHashSet);

                        slctBody.accept(treeOperation);

                          int limit=0;
                          if(((PlainSelect)slctBody).getLimit()!=null)
                          limit=(int) (((PlainSelect)slctBody).getLimit()).getRowCount();
                          
                          Operator input=treeOperation.getOperator();
                          Tuple t=new Tuple();
                          
                          //handling limit
                          int count=0;
                          if(limit==0)
                          while((t=input.readNextTuple())!=null){
                             t.print();
                          }
                          else
                          {
                        	  while((t=input.readNextTuple())!=null)
                        	  {
                        		  t.print();
                        		  count++;
                        		  if(count==limit)
                        			  break;
                        	  }
                          } 
                        //  System.out.println((System.nanoTime()-start)/1000000000.0);
					}
				}
				
			}
			catch(IOException e){
				e.printStackTrace();
			}
			catch(ParseException e){
				e.printStackTrace();
			}
		}
		if(build){
			/*FileOutputStream fos=new FileOutputStream(indexDir+File.separator+"record.txt");
			ObjectOutputStream os =new ObjectOutputStream(fos);
			os.writeObject(recordMan);
			fos.close();
			os.close();*/
			FileOutputStream hashIndexWritter = new FileOutputStream(indexDir+File.separator+"HashSet.txt");
			ObjectOutputStream hashIndexOutput = new ObjectOutputStream(hashIndexWritter);
			hashIndexOutput.writeObject(indexHashSet);
			hashIndexOutput.close();
			hashIndexWritter.close();
		}
		
	}
	
	
	
	
	public static void createIndexBullsEye(RecordManager recordManager,List<String>indexName,String tableName,List<Integer>indexList,String fileName,String indexPath,boolean indexType) throws IOException, java.text.ParseException
    {
       PrimaryMap<String, List<Tuple>> tempMap ;
       PrimaryMap<Integer, ArrayList<Tuple>> temptreeMap;
       PrimaryMap<Date, Integer> associationTreeMap;
       int count=0;
       int valueCount=0;

       DateFormat formatter= new SimpleDateFormat("yyyy-MM-dd");
       
      
        
        Tuple t;
        if(recordManager==null)
        {
            recordManager = new RecordManagerFactory().createRecordManager(indexPath+File.separator+"BullsEyeIndex");
            
        }
        
        String finalName ="";
        for(String temp:indexName)
        {	temp=tableName+"."+temp;
            finalName=finalName+temp;
        }
        finalName=finalName.toLowerCase();
        if(finalName.equalsIgnoreCase("lineitem.orderkey") || finalName.equalsIgnoreCase("orders.orderdate"))
        	return;

        indexHashSet.add(finalName);
        System.out.println(finalName);
        
        if(indexType==true)
        {
        	TreeMap<Date,ArrayList<Tuple>> initialTreeMap = new TreeMap<Date,ArrayList<Tuple>>();
            temptreeMap = recordManager.treeMap(finalName);
            associationTreeMap=recordManager.treeMap(finalName+"Association");
           //int value;

            
            FileScanOperatorIndex fileScanObj = new FileScanOperatorIndex(fileName);

            while((t=fileScanObj.readNextTuple())!=null)
            {                
            	
            	Date d =formatter.parse(t.get(4).toString());
            	if(initialTreeMap.containsKey(d))
            	{
            		ArrayList<Tuple> tempArray = initialTreeMap.get(d);
            		tempArray.add(t);
            		initialTreeMap.put(d,tempArray);
            		
            		        		
            	}
            	else
            	{
            		
            		ArrayList<Tuple> tempArray = new ArrayList<Tuple>();
            		tempArray.add(t);
            		initialTreeMap.put(d, tempArray);
           		
            	}
            	count++;
            	if(count>150)
            	{
            		System.gc();
            		count=0;
            	}

            	           
            }
            
            
            for(Date td:initialTreeMap.keySet())
            {
				associationTreeMap.put(td, ++valueCount);
				ArrayList<Tuple> templ = initialTreeMap.get(td);
				temptreeMap.put(valueCount, templ);
                        	
            }
            
            initialTreeMap.clear();
            initialTreeMap=null;
            
            recordManager.commit();
            temptreeMap=null;
            associationTreeMap=null;

        }
        else
        {
            tempMap=recordManager.hashMap(finalName);
           // System.out.println(Runtime.getRuntime().freeMemory()/(1024*1024));
        FileScanOperatorIndex fileScanObj = new FileScanOperatorIndex(fileName);
        String tempKey =new String();
        while((t=fileScanObj.readNextTuple())!=null)
                {                

                  
                 tempKey=t.get(indexList.get(0)).toString();
                  
                  if(tempMap.containsKey(tempKey))
                  {
                     ArrayList<Tuple> tempList=(ArrayList<Tuple>)tempMap.get(tempKey);
                     tempList.add(t);
                     tempMap.put(tempKey, tempList);
                  }
                  
                  else
                  {
                      ArrayList<Tuple> tempList = new ArrayList<Tuple>();
                      tempList.add(t);
                      tempMap.put(tempKey, tempList);
                  
                  }
                  

                  count++;
                  if(count>150){
                      System.gc();
                      count=0;
                  }
                } 
        	
            recordManager.commit();
            tempMap=null;
           
    }

	
	

    }}
