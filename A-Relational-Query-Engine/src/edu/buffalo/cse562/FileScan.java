package edu.buffalo.cse562;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.StringTokenizer;

public class FileScan {

	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
			BufferedReader br =new BufferedReader(new FileReader("files_db1/orders.dat"));
			String Temp;
			
			
			long start=System.nanoTime();
			while((Temp=br.readLine())!=null){
				//String[] tupArray=Temp.split("//|");
				StringTokenizer st=new StringTokenizer(Temp,"//|");
							
			}
			System.out.println((System.nanoTime()-start)/1000000000);
	}

}
