package edu.buffalo.cse562;


import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.lang.reflect.Array;
import java.nio.Buffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import javax.swing.event.TreeWillExpandListener;

import net.sf.jsqlparser.schema.Table;

public class OrderOperator implements Operator{
	
	Operator input;
	ArrayList<Schema> schemaArrayList = new ArrayList<Schema>();

	LinkedList<Object> OrderColumns;
	LinkedHashMap<String,Schema> queryHashMap;
	LinkedList<Integer>indexList= new LinkedList<Integer>();
	int n,i;
	Object keyInHashMap;
	Object keyInMap;
	Tuple valueInMap;
	List<Object> Columns= new LinkedList<Object>();
	List<Integer> index= new LinkedList<Integer>();
	ArrayList<Tuple> valueList;
	HashMap<Tuple, BufferedReader> extList;
	List<File> fList;
	boolean external;
	int k;
	
	public OrderOperator(Operator input,HashMap<String,LinkedHashMap<String,Schema>> tables,
			List<Object> Columns)
	{
		extList=new HashMap<>();
		this.input=input;
		this.Columns=Columns;
		this.queryHashMap=tables.get(input.getTableName());
		OrderColumns=new LinkedList<Object>();
		fList=new LinkedList<>();
		k=0;
		valueList=new ArrayList<>();		
		for(int i=0;i<Columns.size();i++){
			
			if (Columns.get(i).toString().trim().endsWith("DESC")||Columns.get(i).toString().trim().endsWith("desc")){
				OrderColumns.add(Columns.get(i).toString().trim().substring(0, Columns.get(i).toString().trim().length()-4).trim());
				index.add(1);
			}
			else{
				OrderColumns.add(Columns.get(i).toString());
				index.add(0);
			}
				
		}
		
		if(OrderColumns!=null && !OrderColumns.isEmpty())
		{
		
		List<String> columnNames= new ArrayList(input.getHashMap().get(input.getTableName()).keySet());
		for(Object str:OrderColumns)
				{
			if(columnNames.contains(str))
				indexList.add(columnNames.indexOf(str));
			schemaArrayList.add(queryHashMap.get(str.toString()));
				}
		}
		
		Tuple t=new Tuple();
		
		if(!(indexList.isEmpty()))
		{
			if(input.getSwapDir()==null)
			{
				while((t=input.readNextTuple())!=null)
						valueList.add(t);
				sortLogic();
			}
			else
			{
				int count=0;
				while((t=input.readNextTuple())!=null)
				{
					if(count<1000000){
					valueList.add(t);
					count++;
					}
					else{
						count=0;
						external=true;
						sortLogic();
						writeData();
						System.gc();
					}
				}
				
				if(external){
				writeData();
				initialize();
				}
				else
					sortLogic();
			}
		}
		
		
	}
	
	
	
	
	public void writeData(){
		try {
			k++;
			File f=new File(input.getSwapDir()+File.separator+k+".txt");
			fList.add(f);
			PrintWriter pw=new PrintWriter(new FileWriter(f));
			for(Tuple t:valueList){
				t.print(pw);
			}
			
			pw.close();
			valueList=null;
			valueList=new ArrayList<>();
			
			
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
	}
	
	public void initialize(){
			 
			try {
				for(int j=0;j<k;j++)
				{
					BufferedReader br =new BufferedReader(new FileReader(fList.get(j)));
					String tempString;
					if((tempString=br.readLine())!=null){
					Tuple t = new Tuple(br.readLine().split("\\|"));
					extList.put(t, br);
					}

				}
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		
	}
	
	public Tuple mergeData(){
		if(extList.size()==0)
			return null;
		try {
			ArrayList<Tuple> tList=new ArrayList<>();
			tList.addAll(extList.keySet());
			Tuple temp=getFirst(tList);
			BufferedReader br=extList.remove(temp);
			String tempLine;
			if((tempLine=br.readLine())!=null)
			{
				Tuple t=new Tuple(tempLine.split("\\|"));
				extList.put(t, br);
			}
			return temp;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
		
	}
	
	@Override
	public Tuple readNextTuple() {
		
		if(!external)
		{
			
		if (!(valueList.isEmpty())){
			
			Tuple startTuple = valueList.get(0);
			valueList.remove(0);
								
				return startTuple;
			
		}
		else
			return null;
		}
		else{
			Tuple t;
			if((t=mergeData())!=null)
				return t;
			else
				return null;
		}

		
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
		return input.getHashMap();
	}


	@Override
	public String getTableName() {
		// TODO Auto-generated method stub
		return input.getTableName();
	}


	@Override
	public Table getTable() {
		// TODO Auto-generated method stub
		return input.getTable();
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
	//sort logic
		public void sortLogic(){
			Collections.sort(valueList, new Comparator<Object>()
					{	
				       @Override
				       public int compare(Object o1, Object o2)
				       {
				    	   Tuple temp1 = (Tuple)o1;
				    	   Tuple temp2 = (Tuple)o2;
				       	temp1.setIndex(indexList.get(0));
				       	temp1.setType(schemaArrayList.get(0));
				       	temp2.setIndex(indexList.get(0));
				       	temp2.setType(schemaArrayList.get(0));

				       	
				       	if(temp1.compareTo(temp2)==0 && indexList.size()>1)
					       {
				       		temp1.setIndex(indexList.get(1));
					       	temp1.setType(schemaArrayList.get(1));

					       	temp2.setIndex(indexList.get(1));
					       	temp2.setType(schemaArrayList.get(1));

					       	if(temp1.compareTo(temp2)==0 && indexList.size()>2)
					       	{
					       		temp1.setIndex(indexList.get(2));
						       	temp1.setType(schemaArrayList.get(2));

						       	temp2.setIndex(indexList.get(2));
						       	temp2.setType(schemaArrayList.get(2));

					       		
					       		if(temp1.compareTo(temp2)==0 && indexList.size()>3)
					       		{
					       			temp1.setIndex(indexList.get(3));
							       	temp1.setType(schemaArrayList.get(3));

							       	temp2.setIndex(indexList.get(3));
							       	temp2.setType(schemaArrayList.get(3));

					       			
					       			if(index.get(3)==1)
						       			return temp2.compareTo(temp1);
						       		else
						       			return temp1.compareTo(temp2);
					       		}
					       		else
					       		{
					       			if(index.get(2)==1)
						       			return temp2.compareTo(temp1);
						       		else
						       			return temp1.compareTo(temp2);
					       		}
					       	}
					       	else
					       	{	
					       		if(index.get(1)==1)
					       			return temp2.compareTo(temp1);
					       		else
					       			return temp1.compareTo(temp2);	
					       	}
					       }
					       
					       else
					       {
					    	   if(index.get(0)==1)
					       			return temp2.compareTo(temp1);
					       		else
					       			return temp1.compareTo(temp2);
					    	  
					       }
					       	
					       
					       }
					   

						});
		}
		
		
       public Tuple getFirst(ArrayList<Tuple> tupleList){
    	   Collections.sort(tupleList, new Comparator<Object>()
      {       
     @Override
     public int compare(Object o1, Object o2)
     {
             Tuple temp1 = (Tuple)o1;
             Tuple temp2 = (Tuple)o2;
             temp1.setIndex(indexList.get(0));
		     temp1.setType(schemaArrayList.get(0));

             temp2.setIndex(indexList.get(0));
		       	temp2.setType(schemaArrayList.get(0));

            
             if(temp1.compareTo(temp2)==0 && indexList.size()>1)
             {
                     temp1.setIndex(indexList.get(1));
				       	temp1.setType(schemaArrayList.get(1));

                     temp2.setIndex(indexList.get(1));
				       	temp2.setType(schemaArrayList.get(1));

                    
                     if(temp1.compareTo(temp2)==0 && indexList.size()>2)
                     {
                             temp1.setIndex(indexList.get(2));
     				       	temp1.setType(schemaArrayList.get(2));

                             temp2.setIndex(indexList.get(2));
     				       	temp2.setType(schemaArrayList.get(2));

                             
                             if(temp1.compareTo(temp2)==0 && indexList.size()>3)
                             {
                                     temp1.setIndex(indexList.get(3));
             				       	temp1.setType(schemaArrayList.get(3));

                                     temp2.setIndex(indexList.get(3));
             				       	temp2.setType(schemaArrayList.get(3));

                                     if(index.get(3)==1)
                                             return temp2.compareTo(temp1);
                                     else
                                             return temp1.compareTo(temp2);
                             }
                             else
                             {
                                     if(index.get(2)==1)
                                             return temp2.compareTo(temp1);
                                     else
                                             return temp1.compareTo(temp2);
                             }
                     }
                     else
                     {       
                             if(index.get(1)==1)
                                     return temp2.compareTo(temp1);
                             else
                                     return temp1.compareTo(temp2);       
                     }
             }
            
             else
             {
                     if(index.get(0)==1)
                                     return temp2.compareTo(temp1);
                             else
                                     return temp1.compareTo(temp2);
                   
             }
                    
             
             }
        

              });
       
        
        return tupleList.get(0);
}
}
