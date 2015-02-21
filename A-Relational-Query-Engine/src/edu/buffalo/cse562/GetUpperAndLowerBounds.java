package edu.buffalo.cse562;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import net.sf.jsqlparser.parser.ParseException;

public class GetUpperAndLowerBounds {
	
	String indexColumn=null;
	public Date[] getDates(Expression expr) throws java.text.ParseException
	{
		Date upper=null,lower=null;
		Date[] Dates = new Date[2];
		
		if(expr instanceof AndExpression)
		{
			Expression tempLeft=((AndExpression) expr).getLeftExpression();
			if(tempLeft instanceof GreaterThanEquals)
			{
				indexColumn=((GreaterThanEquals) tempLeft).getLeftExpression().toString();
				Expression greaterRight=((GreaterThanEquals) tempLeft).getRightExpression();
				if(greaterRight instanceof Function)
				{
					ExpressionList func=((Function) greaterRight).getParameters();
					String tempString1=func.toString();
					tempString1=tempString1.substring(2, tempString1.length()-2);
					DateFormat formatter= new SimpleDateFormat("yyyy-MM-dd");
					lower= formatter.parse(tempString1);
					//System.out.println("aaaa0: "+lower);
				}
			}
			Expression tempRight=((AndExpression) expr).getRightExpression();
			if(tempRight instanceof MinorThan)
			{
				indexColumn=((MinorThan) tempRight).getLeftExpression().toString();
				Expression minorRight=((MinorThan) tempRight).getRightExpression();
				if(minorRight instanceof Function)
				{
					ExpressionList func=((Function) minorRight).getParameters();
					String tempString1=func.toString();
					tempString1=tempString1.substring(2, tempString1.length()-2);
					DateFormat formatter= new SimpleDateFormat("yyyy-MM-dd");
					upper= formatter.parse(tempString1);
					//System.out.println("aaaa1: "+upper);
				}
			}
			else if(tempRight instanceof MinorThanEquals)
			{
				indexColumn=((MinorThanEquals) tempRight).getLeftExpression().toString();
				Expression minorEquals=((MinorThanEquals) tempRight).getRightExpression();
				if(minorEquals instanceof Function)
				{
					ExpressionList func=((Function) minorEquals).getParameters();
					String tempString1=func.toString();
					tempString1=tempString1.substring(2, tempString1.length()-2);
					DateFormat formatter= new SimpleDateFormat("yyyy-MM-dd");
					upper= formatter.parse(tempString1);
					Calendar c = Calendar.getInstance();
					c.setTime(upper);
					c.add(Calendar.DATE, 1);
					Date newDate = c.getTime();
					upper=newDate;
					//System.out.println("aaaa2: "+newDate);
				}
			}
		}
		else
		{
			if(expr instanceof MinorThanEquals)
			{
				lower=null;
				indexColumn=((MinorThanEquals) expr).getLeftExpression().toString();
				Expression minorEquals=((MinorThanEquals) expr).getRightExpression();
				if(minorEquals instanceof Function)
				{
					ExpressionList func=((Function) minorEquals).getParameters();
					String tempString1=func.toString();
					tempString1=tempString1.substring(2, tempString1.length()-2);
					DateFormat formatter= new SimpleDateFormat("yyyy-MM-dd");
					upper= formatter.parse(tempString1);
					Calendar c = Calendar.getInstance();
					c.setTime(upper);
					c.add(Calendar.DATE, 1);
					Date newDate = c.getTime();
					//System.out.println("aaaa3: "+newDate);
				}
			}
			else if(expr instanceof MinorThan)
			{
				lower=null;
				indexColumn=((MinorThan) expr).getLeftExpression().toString();
				Expression minor=((MinorThan) expr).getRightExpression();
				if(minor instanceof Function)
				{
					ExpressionList func=((Function) minor).getParameters();
					String tempString1=func.toString();
					tempString1=tempString1.substring(2, tempString1.length()-2);
					DateFormat formatter= new SimpleDateFormat("yyyy-MM-dd");
					upper= formatter.parse(tempString1);
					//System.out.println("aaaa4: "+upper);
				}
			}
			else if(expr instanceof GreaterThanEquals)
			{
				upper=null;
				indexColumn=((GreaterThanEquals) expr).getLeftExpression().toString();
				Expression greaterRight=((GreaterThanEquals) expr).getRightExpression();
				if(greaterRight instanceof Function)
				{
					ExpressionList func=((Function) greaterRight).getParameters();
					String tempString1=func.toString();
					tempString1=tempString1.substring(2, tempString1.length()-2);
					DateFormat formatter= new SimpleDateFormat("yyyy-MM-dd");
					lower= formatter.parse(tempString1);
					//System.out.println("aaaa5: "+lower);
				}
			}
		}
		
		Dates[0]=upper;
		Dates[1]=lower;
		return Dates;
	}
	
	public String getIndexColumn()
	{
		return indexColumn;
	}
}

