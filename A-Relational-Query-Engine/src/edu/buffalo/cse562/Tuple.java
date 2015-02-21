package edu.buffalo.cse562;

import java.io.PrintWriter;
import java.io.Serializable;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;

import com.sun.xml.internal.bind.v2.runtime.unmarshaller.XsiNilLoader.Array;

public class Tuple implements Comparable<Tuple>,Serializable{
  ArrayList<Object> tuppleArray;
  int index;
  Schema type;
  

 


public Tuple(){
	tuppleArray=new ArrayList<Object>();
}

public Tuple(String[] temp){
	  tuppleArray=new ArrayList<Object>();
	  for(String str:temp){
		  tuppleArray.add(str);
	  }
}

 public Tuple(ArrayList<Object> objList){
	 tuppleArray=objList;
 }


public ArrayList<Object> getTuppleArray() {
	return tuppleArray;
}


public void setTuppleArray(ArrayList<Object> tuppleArray) {
	this.tuppleArray = tuppleArray;
} 
  public void setIndex(int index){
	  this.index=index;
  }
  
  
  public void setValue(int index,Object value){
	 
	  this.tuppleArray.set(index, value);
  }
  
  
  public void setType(Schema paramtype){
	  type=paramtype;
  }
  
 
  
  public void print(){

	  StringBuilder temp=new StringBuilder();
	  for(Object str:tuppleArray){
		  if(str instanceof Date){
			  DateFormat frmt=new SimpleDateFormat("yyyy-MM-dd");
			  str=frmt.format((Date)str);
		  }
		  if(str==null)
			  str="";
		  temp.append(str.toString()+"|");
	  }
	  String prString=new String();
	  if(temp!=null){
		  prString=temp.toString();
		
		  prString= prString.substring(0, prString.length()-1);
	  }
	  System.out.println(prString);
  }
  
  
  public void print(PrintWriter pw){

	  String temp=new String();
	  for(Object str:tuppleArray){
		  if(str instanceof Date){
			  DateFormat frmt=new SimpleDateFormat("yyyy-MM-dd");
			  str=frmt.format((Date)str);
		  }
		  if(str==null)
			  str="";
		  temp+=str.toString()+"|";
	  }
	  if(temp!=null)
	  temp=temp.substring(0, temp.length()-1);
	  pw.println(temp);
  }
  
  
  public void add(Object temp)
  {
	  tuppleArray.add(temp);
  }
  
  public Object get(int index){
	  int count=0;
	  for(Object tp:tuppleArray)
	  {
		  if(index==count)
			  return tp;
		  count++;
	  }
	  return null;
	  
	  
  }
  
  int getSize(){
	  return tuppleArray.size();
  }


@Override
public int compareTo(Tuple o) {
	// TODO Auto-generated method stub
	Object a=this.tuppleArray.get(index);
	Object b=o.tuppleArray.get(index);
	
	if(type==Schema.INT){
		Integer aInt=(Integer)(Integer.parseInt(a.toString()));
		Integer bInt=(Integer)(Integer.parseInt(b.toString()));
		return aInt.compareTo(bInt);
	}
	else if(type==Schema.FLOAT){
		Double aInt=(Double)(Double.parseDouble(a.toString()));
		Double bInt=(Double)(Double.parseDouble(b.toString()));
		return aInt.compareTo(bInt);
	}
	else if(type==Schema.DATE){
		DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
		  Date Dobj1,Dobj2;
		  
			  try {
				Dobj1 = formatter.parse(a.toString());
				Dobj2 = formatter.parse(b.toString());
				return Dobj1.compareTo(Dobj2);
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		  
	}
	else if(type==Schema.STRING){
		return a.toString().compareTo(b.toString());
	}
	else{
		Boolean aInt=(Boolean)(Boolean.parseBoolean(a.toString()));
		Boolean bInt=(Boolean)(Boolean.parseBoolean(b.toString()));
		return aInt.compareTo(bInt);
	}

	 return -1;
}
	  
  
  
  
  
  
}
