package edu.buffalo.cse562;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

public class FileScanOperatorIndex {
	BufferedReader input;
	boolean lineitem;
	List<Integer> intList;
	public FileScanOperatorIndex(String fileName) throws FileNotFoundException
	{
		this.input=new BufferedReader(new FileReader(fileName),1024*80);
		intList=new LinkedList<>();
		
		if(fileName.contains("LINEITEM")){
			intList.add(0);
			intList.add(2);
			intList.add(5);
			intList.add(6);
			intList.add(10);
			lineitem=true;
		}

	}
	
	public Tuple readNextTuple()
	{
		 try {
			 String t=input.readLine();
			if(t!=null)
			 {
				 Tuple temp = new Tuple();
				 String[] tuppleArray=t.split("\\|");
				 for(int i=0;i<tuppleArray.length;i++)
				 {
					 if(lineitem){
						 if(intList.contains(i))
							 temp.add(tuppleArray[i]);
					 }
					 else
						 temp.add(tuppleArray[i]);
				 }
				 
				  return temp;
			 }
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		 catch (OutOfMemoryError e) {
			// TODO: handle exception
			 System.out.println(Runtime.getRuntime().freeMemory()/(1024*1024));
		}
		return null;
	}

}
