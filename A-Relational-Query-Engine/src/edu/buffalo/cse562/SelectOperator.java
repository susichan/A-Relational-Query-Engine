package edu.buffalo.cse562;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.arithmetic.Addition;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseAnd;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseOr;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseXor;
import net.sf.jsqlparser.expression.operators.arithmetic.Concat;
import net.sf.jsqlparser.expression.operators.arithmetic.Division;
import net.sf.jsqlparser.expression.operators.arithmetic.Multiplication;
import net.sf.jsqlparser.expression.operators.arithmetic.Subtraction;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.Between;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.ExistsExpression;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.InExpression;
import net.sf.jsqlparser.expression.operators.relational.IsNullExpression;
import net.sf.jsqlparser.expression.operators.relational.LikeExpression;
import net.sf.jsqlparser.expression.operators.relational.Matches;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import net.sf.jsqlparser.expression.operators.relational.NotEqualsTo;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.regex.Pattern;

import javax.swing.text.DateFormatter;

import net.sf.jsqlparser.statement.select.*;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import edu.buffalo.cse562.Operator;
import edu.buffalo.cse562.Tuple;
//import edu.buffalo.cse562.sql.expression.AbstractExpressionVisitor;

public class SelectOperator extends AbstractExpressionVisitor implements Operator 
{
	
	Expression expression;
	BinaryExpression myExpression;
	Expression myLeftExpression,myRightExpression;
	Operator input;
	Boolean tupleEvaluationValue;
	Tuple tuple;
	int intValue;
    String strValue;
    String myLeftExpInStr="";
    String myRightExpInStr="";
    Column column;
    Double doubleValue;
    Date dateValue;
    Boolean booleanValue=false;
    LinkedHashMap<String, Schema> colHashMap;
    String tableName;
    Schema type;
    String columnType=null;
    DateFormat formatter= new SimpleDateFormat("yyyy-MM-dd");
    Date leftDateValue=null,rightDateValue=null;
	int leftIntValue=0,rightIntValue=0;
	Double leftDoubleValue=0.0,rightDoubleValue=0.0;
	String leftStrValue=null,rightStrValue=null;
	HashSet<String> orList =new HashSet<>();
	int orFlag=0;
    


	public SelectOperator(Expression expression, Operator input)
	{
		this.expression=expression;
		this.input=input;
		this.tableName=input.getTableName();
		colHashMap=input.getHashMap().get(tableName);
		
		if(expression instanceof BinaryExpression)
		{
			orFlag=0;
			myExpression= (BinaryExpression) expression;
			myLeftExpression=myExpression.getLeftExpression();
			myRightExpression=myExpression.getRightExpression();
			myLeftExpInStr=myLeftExpression.toString();
			myRightExpInStr=myRightExpression.toString();
			
			try
			{
				if(LongValue.class==myLeftExpression.getClass() ||LongValue.class==myLeftExpression.getClass())
					columnType="int";
				if(DoubleValue.class==myRightExpression.getClass()||DoubleValue.class==myRightExpression.getClass())
					columnType="float";	
				
				if(colHashMap.containsKey(myLeftExpInStr))
				{
					
					if(colHashMap.get(myLeftExpInStr)==Schema.INT)
						columnType="int";
					else if (colHashMap.get(myLeftExpInStr)==Schema.FLOAT)
						columnType="float";
					else if (colHashMap.get(myLeftExpInStr)==Schema.DATE)
						columnType="date";
					else if (colHashMap.get(myLeftExpInStr)==Schema.STRING || colHashMap.containsKey(myRightExpInStr))
						columnType="string";
					
					
				}
				else if(colHashMap.containsKey(myRightExpInStr))
				{
					if(colHashMap.get(myRightExpInStr)==Schema.INT)
						columnType="int";
					else if (colHashMap.get(myRightExpInStr)==Schema.FLOAT)
						columnType="float";
					else if(colHashMap.get(myRightExpInStr)==Schema.STRING)
						columnType="string";
					else if (colHashMap.get(myRightExpInStr)==Schema.DATE)
						columnType="date";
					
				}
			}
			catch(Exception e)
			{
				e.printStackTrace();
			}
		}
		else if(expression instanceof Parenthesis)
		{
			Expression Parexpr=((Parenthesis) expression).getExpression();
			myLeftExpression=((EqualsTo)(((OrExpression)Parexpr).getRightExpression())).getLeftExpression();
			OrExpression orFinal=(OrExpression)Parexpr;
			boolean orFinalbool=true;
			while(orFinalbool){
				EqualsTo temp=(EqualsTo)orFinal.getRightExpression();
				String temp123=temp.getRightExpression().toString();
				temp123=temp123.replaceAll("'", "");
				orList.add(temp123);
				if(orFinal.getLeftExpression() instanceof OrExpression)
					orFinal=(OrExpression)orFinal.getLeftExpression();
				else
					orFinalbool=false;
			}
			
			if(orFinal.getLeftExpression() instanceof EqualsTo){
				String temp=((EqualsTo)(orFinal.getLeftExpression())).getRightExpression().toString();
				String temp1=temp.replaceAll("'", "");
				orList.add(temp1);
			}
			orFlag=1;
		}
	}
	
	public String getTableName(){
		return input.getTableName();
	}
	
	@Override
	public Tuple readNextTuple() {
		
		if(expression == null)
			return input.readNextTuple();
		
				
		do {
			if( (tuple= input.readNextTuple())!=null)
			{
				if(!evaluate(expression)) 
			 	{
					tuple = null;
			 	}
			}
			else
				return null;
		} while(tuple==null); 
			
		
		return tuple;
	}
	//Tuple tuple,
	public boolean  evaluate(Expression expression)
	{
		if(expression==null)
			return false;
		else{
			
			expression.accept(this);
			return tupleEvaluationValue;
			}
	}
	public int getIntValue(){
		return intValue;
	}
	
	public String getStrValue() {
		return strValue;
	}

	public Double getDoubleValue() {
		return doubleValue;
	}

	public Date getDateValue() {
		return dateValue;
	}

	public Boolean getBooleanValue() {
		return booleanValue;
	}

	@Override
	public void visit(DoubleValue doubleValue) {
		// TODO Auto-generated method stub
		this.doubleValue=(Double)doubleValue.getValue();
	}

	@Override
	public void visit(LongValue longValue) {
		// TODO Auto-generated method stub
		intValue=(int)longValue.getValue();
	}
	
	public void visit(NullValue arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(Function input) {
		// TODO Auto-generated method stub
		//ExpressionList list=input;;
		
		ExpressionList list1=input.getParameters();
		List<Expression> list2=list1.getExpressions();
		for(Expression currentItem : list2)
			currentItem.accept(this);
	}

	@Override
	public void visit(InverseExpression arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(JdbcParameter arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(DateValue input) 
	{
		// TODO Auto-generated method stub
		//System.out.println("in datewala "+"date"+input 	);
		this.dateValue=(Date)input.getValue();
	}

	@Override
	public void visit(TimeValue arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(TimestampValue arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(Parenthesis input) {
		// TODO Auto-generated method stub
		input.getExpression().accept(this);
		

	}

	@Override
	public void visit(StringValue input) {
		// TODO Auto-generated method stub
		strValue=input.toString();
	}

	@Override
	public void visit(Addition input) 
	{
		// TODO Auto-generated method stub
		int leftIntValue,rightIntValue=0;
		Double leftDoubleValue,rightDoubleValue;
		if(input.getLeftExpression().getClass()==Column.class && input.getRightExpression().getClass()==Column.class)
		{
			
			//if(this.getHashMap().get(this.getTableName()).get(input.getLeftExpression().toString()).equals(Schema.INT))
			if(colHashMap.get(input.getLeftExpression().toString())==Schema.INT)
			{
				input.getLeftExpression().accept(this);;
				leftIntValue=getIntValue();
				input.getRightExpression().accept(this);
				rightIntValue=getIntValue();
				intValue=leftIntValue+rightIntValue;
			}else if(colHashMap.get(input.getLeftExpression().toString())==Schema.FLOAT)
			{
				input.getLeftExpression().accept(this);
				leftDoubleValue=getDoubleValue();
				input.getRightExpression().accept(this);
				rightDoubleValue=getDoubleValue();
				doubleValue=leftDoubleValue+rightDoubleValue;
			}
		}else if(input.getLeftExpression().getClass()==Column.class && (input.getRightExpression().getClass()!=Column.class))
		{
			if(colHashMap.get(input.getLeftExpression().toString())==Schema.INT)
			{
				input.getLeftExpression().accept(this);;
				leftIntValue=getIntValue();
				input.getRightExpression().accept(this);
				rightIntValue=getIntValue();
				intValue=leftIntValue+rightIntValue;
			}else if(colHashMap.get(input.getLeftExpression().toString())==Schema.FLOAT)
			{
				input.getLeftExpression().accept(this);
				leftDoubleValue=getDoubleValue();
				input.getRightExpression().accept(this);
				rightDoubleValue=getDoubleValue();
				doubleValue=leftDoubleValue+rightDoubleValue;
			}	
		} else if(input.getLeftExpression().getClass()!=Column.class && input.getRightExpression().getClass()==Column.class)
		{
			//if(this.getHashMap().get(this.getTableName()).get(input.getRightExpression().toString()).equals(Schema.INT))
			if(colHashMap.get(input.getRightExpression().toString())==Schema.INT)
			{
				input.getLeftExpression().accept(this);;
				leftIntValue=getIntValue();
				input.getRightExpression().accept(this);
				rightIntValue=getIntValue();
				intValue=leftIntValue+rightIntValue;
			} else if(colHashMap.get(input.getRightExpression().toString())==Schema.FLOAT)
			{
				input.getLeftExpression().accept(this);
				leftDoubleValue=getDoubleValue();
				input.getRightExpression().accept(this);
				rightDoubleValue=getDoubleValue();
				doubleValue=leftDoubleValue+rightDoubleValue;
			}
		}else if(input.getLeftExpression().getClass()!=Column.class && input.getRightExpression().getClass()!=Column.class)
		{
			if(input.getLeftExpression().getClass()==LongValue.class)
			{
				input.getLeftExpression().accept(this);
				leftIntValue=getIntValue();
				input.getRightExpression().accept(this);
				rightIntValue=getIntValue();
				intValue=leftIntValue+rightIntValue;
			}else if(input.getLeftExpression().getClass()==DoubleValue.class)
			{
				input.getLeftExpression().accept(this);
				leftDoubleValue=getDoubleValue();
				input.getRightExpression().accept(this);
				rightDoubleValue=getDoubleValue();
				doubleValue=leftDoubleValue+rightDoubleValue;
			}
		}
	}

	@Override
	public void visit(Division input) 
	{
		// TODO Auto-generated method stub
		int leftIntValue,rightIntValue=0;
		Double leftDoubleValue,rightDoubleValue;
		//if(input.getLeftExpression().getClass().toString().equals("class net.sf.jsqlparser.schema.Column") && input.getRightExpression().getClass().toString().equals("class net.sf.jsqlparser.schema.Column"))
		if(input.getLeftExpression().getClass()==Column.class && input.getRightExpression().getClass()==Column.class)
		{
			//if(this.getHashMap().get(this.getTableName()).get(input.getLeftExpression().toString()).equals(Schema.INT))
			if(colHashMap.get(input.getLeftExpression().toString())==Schema.INT)
			{
				input.getLeftExpression().accept(this);;
				leftIntValue=getIntValue();
				input.getRightExpression().accept(this);
				rightIntValue=getIntValue();
				if(rightIntValue==0)
					System.out.println("Cannot divide by 0");
				else
					intValue=leftIntValue/rightIntValue;
			}else if(colHashMap.get(input.getLeftExpression().toString())==Schema.FLOAT)
			{
				input.getLeftExpression().accept(this);
				leftDoubleValue=getDoubleValue();
				input.getRightExpression().accept(this);
				rightDoubleValue=getDoubleValue();
				if(rightDoubleValue==0.0)
					System.out.println("Cannot divide by 0");
				else
				doubleValue=leftDoubleValue/rightDoubleValue;
			}
		}else if(input.getLeftExpression().getClass()==Column.class && input.getRightExpression().getClass()!=Column.class)
		{
//			if(this.getHashMap().get(this.getTableName()).get(input.getLeftExpression().toString()).equals(Schema.INT))
			if(colHashMap.get(input.getLeftExpression().toString())==Schema.INT)
			{
				input.getLeftExpression().accept(this);;
				leftIntValue=getIntValue();
				input.getRightExpression().accept(this);
				rightIntValue=getIntValue();
				if(rightIntValue==0)
					System.out.println("Cannot divide by 0");
				else
					intValue=leftIntValue/rightIntValue;
			}else if(colHashMap.get(input.getLeftExpression().toString())==Schema.FLOAT)
			{
				input.getLeftExpression().accept(this);
				leftDoubleValue=getDoubleValue();
				input.getRightExpression().accept(this);
				rightDoubleValue=getDoubleValue();
				if(rightDoubleValue==0.0)
					System.out.println("Cannot divide by 0");
				else
					doubleValue=leftDoubleValue/rightDoubleValue;
			}	
		}else if(input.getLeftExpression().getClass()!=Column.class && input.getRightExpression().getClass()==Column.class)
		{
			//if(this.getHashMap().get(this.getTableName()).get(input.getRightExpression().toString()).equals(Schema.INT))
			if(colHashMap.get(input.getRightExpression().toString())==Schema.INT)
			{
				input.getLeftExpression().accept(this);;
				leftIntValue=getIntValue();
				input.getRightExpression().accept(this);
				rightIntValue=getIntValue();
				if(rightIntValue==0)
					System.out.println("Cannot divide by 0");
				else
					intValue=leftIntValue/rightIntValue;
			}else if(colHashMap.get(input.getRightExpression().toString())==Schema.FLOAT)
			{
				input.getLeftExpression().accept(this);
				leftDoubleValue=getDoubleValue();
				input.getRightExpression().accept(this);
				rightDoubleValue=getDoubleValue();
				if(rightDoubleValue==0.0)
					System.out.println("Cannot divide by 0");
				else
					doubleValue=leftDoubleValue/rightDoubleValue;
			}
		}else if(input.getLeftExpression().getClass()!=Column.class && input.getRightExpression().getClass()!=Column.class)
		{
			if(input.getLeftExpression().getClass()==LongValue.class)
			{
				input.getLeftExpression().accept(this);
				leftIntValue=getIntValue();
				input.getRightExpression().accept(this);
				rightIntValue=getIntValue();
				if(rightIntValue==0)
					System.out.println("Cannot divide by 0");
				else
					intValue=leftIntValue/rightIntValue;
			}else if(input.getLeftExpression().getClass()==DoubleValue.class)
			{
				input.getLeftExpression().accept(this);
				leftDoubleValue=getDoubleValue();
				input.getRightExpression().accept(this);
				rightDoubleValue=getDoubleValue();
				if(rightDoubleValue==0.0)
					System.out.println("Cannot divide by 0");
				else
					doubleValue=leftDoubleValue/rightDoubleValue;
			}
		}
	}

	@Override
	public void visit(Multiplication input) 
	{
		// TODO Auto-generated method stub
		int leftIntValue,rightIntValue=0;
		Double leftDoubleValue,rightDoubleValue;
		//if(input.getLeftExpression().getClass().toString().equals("class net.sf.jsqlparser.schema.Column") && input.getRightExpression().getClass().toString().equals("class net.sf.jsqlparser.schema.Column"))
		if(input.getLeftExpression().getClass()==Column.class && input.getRightExpression().getClass()==Column.class)
		{
			//if(this.getHashMap().get(this.getTableName()).get(input.getLeftExpression().toString()).equals(Schema.INT))
			if(colHashMap.get(input.getLeftExpression().toString())==Schema.INT)
			{
				input.getLeftExpression().accept(this);;
				leftIntValue=getIntValue();
				input.getRightExpression().accept(this);
				rightIntValue=getIntValue();
				intValue=leftIntValue*rightIntValue;
			}else if(colHashMap.get(input.getLeftExpression().toString())==Schema.FLOAT)
			{
				input.getLeftExpression().accept(this);
				leftDoubleValue=getDoubleValue();
				input.getRightExpression().accept(this);
				rightDoubleValue=getDoubleValue();
				doubleValue=leftDoubleValue*rightDoubleValue;
			}
		}else if(input.getLeftExpression().getClass()==Column.class && input.getRightExpression().getClass()!=Column.class)
		{
			//if(this.getHashMap().get(this.getTableName()).get(input.getLeftExpression().toString()).equals(Schema.INT))
			if(colHashMap.get(input.getLeftExpression().toString())==Schema.INT)
			{
				input.getLeftExpression().accept(this);;
				leftIntValue=getIntValue();
				input.getRightExpression().accept(this);
				rightIntValue=getIntValue();
				intValue=leftIntValue*rightIntValue;
			}else if(colHashMap.get(input.getLeftExpression().toString())==Schema.FLOAT)
			{
				input.getLeftExpression().accept(this);
				leftDoubleValue=getDoubleValue();
				input.getRightExpression().accept(this);
				rightDoubleValue=getDoubleValue();
				doubleValue=leftDoubleValue*rightDoubleValue;
			}	
		}else if(input.getLeftExpression().getClass()!=Column.class && input.getRightExpression().getClass()==Column.class)
		{
			//if(this.getHashMap().get(this.getTableName()).get(input.getRightExpression().toString()).equals(Schema.INT))
			if(colHashMap.get(input.getRightExpression().toString())==Schema.INT)
			{
				input.getLeftExpression().accept(this);;
				leftIntValue=getIntValue();
				input.getRightExpression().accept(this);
				rightIntValue=getIntValue();
				intValue=leftIntValue*rightIntValue;
			}else if(colHashMap.get(input.getRightExpression().toString())==Schema.FLOAT)
			{
				input.getLeftExpression().accept(this);
				leftDoubleValue=getDoubleValue();
				input.getRightExpression().accept(this);
				rightDoubleValue=getDoubleValue();
				doubleValue=leftDoubleValue*rightDoubleValue;
			}
		}else if(input.getLeftExpression().getClass()!=Column.class && input.getRightExpression().getClass()!=Column.class)
		{
			if(input.getLeftExpression().getClass()==LongValue.class)
			{
				input.getLeftExpression().accept(this);
				leftIntValue=getIntValue();
				input.getRightExpression().accept(this);
				rightIntValue=getIntValue();
				intValue=leftIntValue*rightIntValue;
			}else if(input.getLeftExpression().getClass()==DoubleValue.class)
			{
				input.getLeftExpression().accept(this);
				leftDoubleValue=getDoubleValue();
				input.getRightExpression().accept(this);
				rightDoubleValue=getDoubleValue();
				doubleValue=leftDoubleValue*rightDoubleValue;
			}
		}
	}

	@Override
	public void visit(Subtraction input) {
		// TODO Auto-generated method stub
		
		int leftIntValue,rightIntValue=0;
		Double leftDoubleValue,rightDoubleValue;
		//if(input.getLeftExpression().getClass().toString().equals("class net.sf.jsqlparser.schema.Column") && input.getRightExpression().getClass().toString().equals("class net.sf.jsqlparser.schema.Column"))
		if(input.getLeftExpression().getClass()==Column.class && input.getRightExpression().getClass()==Column.class)
		{
			//if(this.getHashMap().get(this.getTableName()).get(input.getLeftExpression().toString()).equals(Schema.INT))
			if(colHashMap.get(input.getLeftExpression().toString())==Schema.INT)
			{
				input.getLeftExpression().accept(this);;
				leftIntValue=getIntValue();
				input.getRightExpression().accept(this);
				rightIntValue=getIntValue();
				intValue=leftIntValue-rightIntValue;
			}else if(colHashMap.get(input.getLeftExpression().toString())==Schema.FLOAT)
			{
				input.getLeftExpression().accept(this);
				leftDoubleValue=getDoubleValue();
				input.getRightExpression().accept(this);
				rightDoubleValue=getDoubleValue();
				doubleValue=leftDoubleValue-rightDoubleValue;
			}
		}else if(input.getLeftExpression().getClass()==Column.class && input.getRightExpression().getClass()!=Column.class)
		{
			//if(this.getHashMap().get(this.getTableName()).get(input.getLeftExpression().toString()).equals(Schema.INT))
			if(colHashMap.get(input.getLeftExpression().toString())==Schema.INT)
			{
				input.getLeftExpression().accept(this);;
				leftIntValue=getIntValue();
				input.getRightExpression().accept(this);
				rightIntValue=getIntValue();
				intValue=leftIntValue-rightIntValue;
			}else if(colHashMap.get(input.getLeftExpression().toString())==Schema.FLOAT)
			{
				input.getLeftExpression().accept(this);
				leftDoubleValue=getDoubleValue();
				input.getRightExpression().accept(this);
				rightDoubleValue=getDoubleValue();
				doubleValue=leftDoubleValue-rightDoubleValue;
			}	
		}else if(input.getLeftExpression().getClass()!=Column.class && input.getRightExpression().getClass()==Column.class)
		{
			//if(this.getHashMap().get(this.getTableName()).get(input.getRightExpression().toString()).equals(Schema.INT))
			if(colHashMap.get(input.getRightExpression().toString())==Schema.INT)
			{
				input.getLeftExpression().accept(this);;
				leftIntValue=getIntValue();
				input.getRightExpression().accept(this);
				rightIntValue=getIntValue();
				intValue=leftIntValue-rightIntValue;
			}else if(colHashMap.get(input.getRightExpression().toString())==Schema.FLOAT)
			{
				input.getLeftExpression().accept(this);
				leftDoubleValue=getDoubleValue();
				input.getRightExpression().accept(this);
				rightDoubleValue=getDoubleValue();
				doubleValue=leftDoubleValue-rightDoubleValue;
			}
		}else if(input.getLeftExpression().getClass()!=Column.class && input.getRightExpression().getClass()!=Column.class)
		{
			if(input.getLeftExpression().getClass()==LongValue.class)
			{
				input.getLeftExpression().accept(this);
				leftIntValue=getIntValue();
				input.getRightExpression().accept(this);
				rightIntValue=getIntValue();
				intValue=leftIntValue-rightIntValue;
			}else if(input.getLeftExpression().getClass()==DoubleValue.class)
			{
				input.getLeftExpression().accept(this);
				leftDoubleValue=getDoubleValue();
				input.getRightExpression().accept(this);
				rightDoubleValue=getDoubleValue();
				doubleValue=leftDoubleValue-rightDoubleValue;
			}
		}
	}

	@Override
	public void visit(AndExpression input) {
		// TODO Auto-generated method stub
		input.getLeftExpression().accept(this);
		boolean leftBoolean=this.tupleEvaluationValue;
		if(leftBoolean==false)
		{
			tupleEvaluationValue=false;
			return;
		}
		else
		input.getRightExpression().accept(this);
		boolean rightBoolean=this.tupleEvaluationValue;
		if(leftBoolean==true && rightBoolean==true)
			tupleEvaluationValue=true;
		else
			tupleEvaluationValue=false;

	}

	@Override
	public void visit(OrExpression input) {
		// TODO Auto-generated method stub
		input.getRightExpression().accept(this);
		boolean rightBoolean=this.tupleEvaluationValue;
		if(rightBoolean==true)
			return;
		input.getLeftExpression().accept(this);
		boolean leftBoolean=this.tupleEvaluationValue;
		if(leftBoolean==true || rightBoolean==true)
			tupleEvaluationValue=true;
		else
			tupleEvaluationValue=false;
	}

	@Override
	public void visit(Between input) {
		// TODO Auto-generated method stub
		System.out.println("in between");
		//System.out.println("LE "+input.getLeftExpression());
		//System.out.println("RE "+input.getBetweenExpressionEnd());
		input.getLeftExpression().accept(this);
		if(this.getHashMap().get(this.getTableName()).get(input.getLeftExpression().toString()).equals(Schema.INT))
		{
			
		}
 
	}

	@Override
	public void visit(EqualsTo input) {
		// TODO Auto-generated method stub
		if(orFlag==1)
		{
			myLeftExpression.accept(this);
			leftStrValue=getStrValue();

			if(orList.contains(leftStrValue))
			{
				tupleEvaluationValue=true;
			}
			else
			{
				tupleEvaluationValue=false;
			}
		}
		else
		{
			try
			{
				switch (columnType)
				{
					case "int":
						myLeftExpression.accept(this);
						leftIntValue=getIntValue();
						myRightExpression.accept(this);
						rightIntValue=getIntValue();
						if(leftIntValue==rightIntValue)
						{
							tupleEvaluationValue=true;
						}
						else
							tupleEvaluationValue=false;
						break;
						
					case "float":
						
						if(myRightExpression.getClass()==LongValue.class)
						{
							myLeftExpression.accept(this);
							leftDoubleValue=getDoubleValue();
							myRightExpression.accept(this);
							int tempIntValue=getIntValue();
							Integer i=(Integer)tempIntValue;
							rightDoubleValue=Double.parseDouble(i.toString());
						}
						else if(myLeftExpression.getClass()==LongValue.class)
						{
								myLeftExpression.accept(this);
								int tempIntValue=getIntValue();
								Integer i=(Integer)tempIntValue;
								leftDoubleValue=Double.parseDouble(i.toString());
								myRightExpression.accept(this);
								rightDoubleValue=getDoubleValue();
						}
						else
						{
							myLeftExpression.accept(this);
							leftDoubleValue=getDoubleValue();
							myRightExpression.accept(this);
							rightDoubleValue=getDoubleValue();
						}
						if(leftDoubleValue.equals(rightDoubleValue))
						{
							tupleEvaluationValue=true;
						}
						else
							tupleEvaluationValue=false;
						break;
						
					case "string":
						//if(this.tables.get(this.tableName).containsKey(myLeftExpInStr) && this.tables.get(this.tableName).containsKey(myRightExpInStr))
						if(colHashMap.containsKey(myLeftExpInStr) && colHashMap.containsKey(myRightExpInStr))
						{
							myLeftExpression.accept(this);
							leftStrValue=getStrValue();
							myRightExpression.accept(this);
							rightStrValue=getStrValue();
							if(leftStrValue.equals(rightStrValue))
								tupleEvaluationValue=true;
							else
								tupleEvaluationValue=false;
						}
						else
						{
							if(colHashMap.containsKey(myLeftExpInStr))//(this.getHashMap().get(this.getTableName()).containsKey(myLeftExpInStr))
							{
								myLeftExpression.accept(this);
								leftStrValue=getStrValue();
								myRightExpression.accept(this);
								rightStrValue=getStrValue();
								rightStrValue=rightStrValue.substring(1,rightStrValue.length()-1);
								if(leftStrValue.equals(rightStrValue))
									tupleEvaluationValue=true;
								else
									tupleEvaluationValue=false;
							}
							else if(colHashMap.containsKey(myRightExpInStr))//(this.getHashMap().get(this.getTableName()).containsKey(myRightExpInStr))
							{
								myLeftExpression.accept(this);
								leftStrValue=getStrValue();
								leftStrValue=leftStrValue.substring(1, leftStrValue.length()-1);
								myRightExpression.accept(this);
								rightStrValue=getStrValue();
								//rightStrValue=rightStrValue.substring(1,rightStrValue.length()-1);
								if(leftStrValue.equals(rightStrValue))
									tupleEvaluationValue=true;
								else
									tupleEvaluationValue=false;
							}
						}
						break;
					case "date":
						if (colHashMap.get(myRightExpInStr)==Schema.DATE && colHashMap.get(myLeftExpInStr)==Schema.DATE)
						{
							myLeftExpression.accept(this);
							leftDateValue=getDateValue();
							myRightExpression.accept(this);
							rightDateValue=getDateValue();
						}
						else if (colHashMap.get(myLeftExpInStr)==Schema.DATE && colHashMap.containsKey(myLeftExpInStr))
						{
							String tempDate=myRightExpInStr;
							tempDate=tempDate.substring(6, tempDate.length()-2);
							myLeftExpression.accept(this);
							leftDateValue=getDateValue();
							rightDateValue=formatter.parse(tempDate);
						}
						else if (colHashMap.get(myRightExpInStr)==Schema.DATE && colHashMap.containsKey(myRightExpInStr))
						{
							String tempDate=myRightExpInStr;
							tempDate=tempDate.substring(6, tempDate.length()-2);
							myLeftExpression.accept(this);
							leftDateValue=getDateValue();
							rightDateValue=formatter.parse(tempDate);
						}
						if(leftDateValue.equals(rightDateValue))
							tupleEvaluationValue=true;
						else
							tupleEvaluationValue=false;
						break;
						default :
							//System.out.println("invalid column name");
				}
			}
			catch(Exception e)
			{
				e.printStackTrace();
			}
		}
	}

	@Override
	public void visit(GreaterThan input) 
	{
		// TODO Auto-generated method stub
		try
		{
			switch (columnType)
			{
				case "int":
					myLeftExpression.accept(this);
					leftIntValue=getIntValue();
					myRightExpression.accept(this);
					rightIntValue=getIntValue();
					if(leftIntValue>rightIntValue)
						tupleEvaluationValue=true;
					else
						tupleEvaluationValue=false;
					break;
					
				case "float":
					if(myRightExpression.getClass()==LongValue.class)
					{
						myLeftExpression.accept(this);
						leftDoubleValue=getDoubleValue();
						myRightExpression.accept(this);
						int tempIntValue=getIntValue();
						Integer i=(Integer)tempIntValue;
						rightDoubleValue=Double.parseDouble(i.toString());
					}
					else if(myLeftExpression.getClass()==LongValue.class)
					{
							myLeftExpression.accept(this);
							int tempIntValue=getIntValue();
							Integer i=(Integer)tempIntValue;
							leftDoubleValue=Double.parseDouble(i.toString());
							myRightExpression.accept(this);
							rightDoubleValue=getDoubleValue();
					}
					else
					{
						myLeftExpression.accept(this);
						leftDoubleValue=getDoubleValue();
						myRightExpression.accept(this);
						rightDoubleValue=getDoubleValue();
					}
					if(leftDoubleValue>rightDoubleValue)
						tupleEvaluationValue=true;
					else
						tupleEvaluationValue=false;
					break;
					
				case "date":
					if (colHashMap.get(myRightExpInStr)==Schema.DATE && colHashMap.get(myLeftExpInStr)==Schema.DATE)
					{
						myLeftExpression.accept(this);
						leftDateValue=getDateValue();
						myRightExpression.accept(this);
						rightDateValue=getDateValue();
					}
					else if (colHashMap.get(myLeftExpInStr)==Schema.DATE && colHashMap.containsKey(myLeftExpInStr))
					{
						String tempDate=myRightExpInStr;
						tempDate=tempDate.substring(6, tempDate.length()-2);
						myLeftExpression.accept(this);
						leftDateValue=getDateValue();
						rightDateValue=formatter.parse(tempDate);
					}
					else if (colHashMap.get(myRightExpInStr)==Schema.DATE && colHashMap.containsKey(myRightExpInStr))
					{
						String tempDate=myRightExpInStr;
						tempDate=tempDate.substring(6, tempDate.length()-2);
						myLeftExpression.accept(this);
						leftDateValue=getDateValue();
						rightDateValue=formatter.parse(tempDate);
					}
					if(leftDateValue.after(rightDateValue))
						tupleEvaluationValue=true;
					else
						tupleEvaluationValue=false;
					break;
				default :
				//	System.out.println("invalid column name");
			}
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
	}

	@Override
	public void visit(GreaterThanEquals input) 
	{
		// TODO Auto-generated method stub
		try
		{
			switch (columnType)
			{
				case "int":
					myLeftExpression.accept(this);
					leftIntValue=getIntValue();
					myRightExpression.accept(this);
					rightIntValue=getIntValue();
					if(leftIntValue>=rightIntValue)
						tupleEvaluationValue=true;
					else
						tupleEvaluationValue=false;
					break;
				case "float":
					if(myRightExpression.getClass()==LongValue.class)
					{
						myLeftExpression.accept(this);
						leftDoubleValue=getDoubleValue();
						myRightExpression.accept(this);
						int tempIntValue=getIntValue();
						Integer i=(Integer)tempIntValue;
						rightDoubleValue=Double.parseDouble(i.toString());
					}
					else if(myLeftExpression.getClass()==LongValue.class)
					{
						myLeftExpression.accept(this);
						int tempIntValue=getIntValue();
						Integer i=(Integer)tempIntValue;
						leftDoubleValue=Double.parseDouble(i.toString());
						myRightExpression.accept(this);
						rightDoubleValue=getDoubleValue();
					}
					else
					{
						myLeftExpression.accept(this);
						leftDoubleValue=getDoubleValue();
						myRightExpression.accept(this);
						rightDoubleValue=getDoubleValue();
					}
					if(leftDoubleValue>=rightDoubleValue)
						tupleEvaluationValue=true;
					else
						tupleEvaluationValue=false;
					break;
				case "date":
					if (colHashMap.get(myRightExpInStr)==Schema.DATE && colHashMap.get(myLeftExpInStr)==Schema.DATE)
					{
						myLeftExpression.accept(this);
						leftDateValue=getDateValue();
						myRightExpression.accept(this);
						rightDateValue=getDateValue();
					}
					else if (colHashMap.get(myLeftExpInStr)==Schema.DATE && colHashMap.containsKey(myLeftExpInStr))
					{
						String tempDate=myRightExpInStr;
						tempDate=tempDate.substring(6, tempDate.length()-2);
						myLeftExpression.accept(this);
						leftDateValue=getDateValue();
						rightDateValue=formatter.parse(tempDate);
					}
					else if (colHashMap.get(myRightExpInStr)==Schema.DATE && colHashMap.containsKey(myRightExpInStr))
					{
						String tempDate=myRightExpInStr;
						tempDate=tempDate.substring(6, tempDate.length()-2);
						myLeftExpression.accept(this);
						leftDateValue=getDateValue();
						rightDateValue=formatter.parse(tempDate);
					}
					if(leftDateValue.after(rightDateValue)||leftDateValue.equals(rightDateValue))
						tupleEvaluationValue=true;
					else
						tupleEvaluationValue=false;
					break;
					default :
				//		System.out.println("invalid column name");
			}
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
	}

	@Override
	public void visit(InExpression arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(IsNullExpression input) {
		// TODO Auto-generated method stub
		System.out.println("Le "+input.getLeftExpression());

	}

	@Override
	public void visit(LikeExpression input) {
		// TODO Auto-generated method stub
		//System.out.println("mem is "+Runtime.getRuntime().maxMemory()/1048576);
		String matchingString,leftStrValue,rightStrValue;
		
		input.getLeftExpression().accept(this);
		leftStrValue=getStrValue();
		input.getRightExpression().accept(this);
		rightStrValue=getStrValue();
		rightStrValue=rightStrValue.substring(2, rightStrValue.length()-1);
		//System.out.println(leftStrValue+"  **** "+rightStrValue);
		if(colHashMap.containsKey(input.getLeftExpression().toString()))
		{
			if(colHashMap.get(input.getLeftExpression().toString())==Schema.STRING)
			{
				String pattern = ".*"+rightStrValue;
				boolean matches = Pattern.matches(pattern, leftStrValue);
			//	System.out.println("matches = " + matches);
				if(matches==true)
					tupleEvaluationValue=true;
				else
					tupleEvaluationValue=false;
			}
		}
	}

	@Override
	public void visit(MinorThan input) 
	{
		// TODO Auto-generated method stub
		try
		{
			switch (columnType)
			{
				case "int":
					myLeftExpression.accept(this);
					leftIntValue=getIntValue();
					myRightExpression.accept(this);
					rightIntValue=getIntValue();
					if(leftIntValue<rightIntValue)
						tupleEvaluationValue=true;
					else
						tupleEvaluationValue=false;
					break;
				case "float":
					if(myRightExpression.getClass()==LongValue.class)
					{
						myLeftExpression.accept(this);
						leftDoubleValue=getDoubleValue();
						myRightExpression.accept(this);
						int tempIntValue=getIntValue();
						Integer i=(Integer)tempIntValue;
						rightDoubleValue=Double.parseDouble(i.toString());
					}
					else if(myLeftExpression.getClass()==LongValue.class)
					{
							myLeftExpression.accept(this);
							int tempIntValue=getIntValue();
							Integer i=(Integer)tempIntValue;
							leftDoubleValue=Double.parseDouble(i.toString());
							myRightExpression.accept(this);
							rightDoubleValue=getDoubleValue();
					}
					else
					{
						myLeftExpression.accept(this);
						leftDoubleValue=getDoubleValue();
						myRightExpression.accept(this);
						rightDoubleValue=getDoubleValue();
					}
					if(leftDoubleValue<rightDoubleValue)
						tupleEvaluationValue=true;
					else
						tupleEvaluationValue=false;
					break;
				case "date":
					if (colHashMap.get(myRightExpInStr)==Schema.DATE && colHashMap.get(myLeftExpInStr)==Schema.DATE)
					{
						myLeftExpression.accept(this);
						leftDateValue=getDateValue();
						myRightExpression.accept(this);
						rightDateValue=getDateValue();
					}
					else if (colHashMap.get(myLeftExpInStr)==Schema.DATE && colHashMap.containsKey(myLeftExpInStr))
					{
						String tempDate=myRightExpInStr;
						tempDate=tempDate.substring(6, tempDate.length()-2);
						myLeftExpression.accept(this);
						leftDateValue=getDateValue();
						rightDateValue=formatter.parse(tempDate);
					}
					else if (colHashMap.get(myRightExpInStr)==Schema.DATE && colHashMap.containsKey(myRightExpInStr))
					{
						String tempDate=myRightExpInStr;
						tempDate=tempDate.substring(6, tempDate.length()-2);
						myLeftExpression.accept(this);
						leftDateValue=getDateValue();
						rightDateValue=formatter.parse(tempDate);
					}
					if(leftDateValue.before(rightDateValue))
						tupleEvaluationValue=true;
					else
						tupleEvaluationValue=false;
					break;
					default :
					//	System.out.println("invalid column name");
			}
		}
		catch (Exception e) 
		{
			e.printStackTrace();
		}
	}

	@Override
	public void visit(MinorThanEquals input) {
		// TODO Auto-generated method stub
		try
		{
			switch (columnType)
			{
				case "int":
					myLeftExpression.accept(this);
					leftIntValue=getIntValue();
					myRightExpression.accept(this);
					rightIntValue=getIntValue();
					if(leftIntValue<=rightIntValue)
					{
						tupleEvaluationValue=true;
					}
					else
						tupleEvaluationValue=false;
					break;
				case "float":
					
					if(myRightExpression.getClass()==LongValue.class)
					{
						myLeftExpression.accept(this);
						leftDoubleValue=getDoubleValue();
						myRightExpression.accept(this);
						int tempIntValue=getIntValue();
						Integer i=(Integer)tempIntValue;
						rightDoubleValue=Double.parseDouble(i.toString());
					}
					else if(myLeftExpression.getClass()==LongValue.class)
					{
						myLeftExpression.accept(this);
						int tempIntValue=getIntValue();
						Integer i=(Integer)tempIntValue;
						leftDoubleValue=Double.parseDouble(i.toString());
						myRightExpression.accept(this);
						rightDoubleValue=getDoubleValue();
					}
					else
					{
						myLeftExpression.accept(this);
						leftDoubleValue=getDoubleValue();
						myRightExpression.accept(this);
						rightDoubleValue=getDoubleValue();
					}
					if(leftDoubleValue<=rightDoubleValue)
						tupleEvaluationValue=true;
					else
						tupleEvaluationValue=false;
					break;
				case "date":
					if (colHashMap.get(myRightExpInStr)==Schema.DATE && colHashMap.get(myLeftExpInStr)==Schema.DATE)
					{
						myLeftExpression.accept(this);
						leftDateValue=getDateValue();
						myRightExpression.accept(this);
						rightDateValue=getDateValue();
					}
					else if (colHashMap.get(myLeftExpInStr)==Schema.DATE && colHashMap.containsKey(myLeftExpInStr))
					{
						String tempDate=myRightExpInStr;
						tempDate=tempDate.substring(6, tempDate.length()-2);
						myLeftExpression.accept(this);
						leftDateValue=getDateValue();
						rightDateValue=formatter.parse(tempDate);
					}
					else if (colHashMap.get(myRightExpInStr)==Schema.DATE && colHashMap.containsKey(myRightExpInStr))
					{
						String tempDate=myRightExpInStr;
						tempDate=tempDate.substring(6, tempDate.length()-2);
						myLeftExpression.accept(this);
						leftDateValue=getDateValue();
						rightDateValue=formatter.parse(tempDate);
					}
					if(leftDateValue.before(rightDateValue)||leftDateValue.equals(rightDateValue))
						tupleEvaluationValue=true;
					else
						tupleEvaluationValue=false;
					break;
					default :
				//		System.out.println("invalid column name");
			}
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
	}

	@Override
	public void visit(NotEqualsTo input) {
		// TODO Auto-generated method stub
		try
		{
			switch (columnType)
			{
				case "int":
					myLeftExpression.accept(this);
					leftIntValue=getIntValue();
					myRightExpression.accept(this);
					rightIntValue=getIntValue();
					if(leftIntValue!=rightIntValue)
					{
						tupleEvaluationValue=true;
					}
					else
						tupleEvaluationValue=false;
					break;
				case "float":
					if(myRightExpression.getClass()==LongValue.class)
					{
						myLeftExpression.accept(this);
						leftDoubleValue=getDoubleValue();
						myRightExpression.accept(this);
						int tempIntValue=getIntValue();
						Integer i=(Integer)tempIntValue;
						rightDoubleValue=Double.parseDouble(i.toString());
					}
					else if(myLeftExpression.getClass()==LongValue.class)
					{
						myLeftExpression.accept(this);
						int tempIntValue=getIntValue();
						Integer i=(Integer)tempIntValue;
						leftDoubleValue=Double.parseDouble(i.toString());
						myRightExpression.accept(this);
						rightDoubleValue=getDoubleValue();
					}
					else
					{
						myLeftExpression.accept(this);
						leftDoubleValue=getDoubleValue();
						myRightExpression.accept(this);
						rightDoubleValue=getDoubleValue();
					}
					if(leftDoubleValue!=rightDoubleValue)
						tupleEvaluationValue=true;
					else
						tupleEvaluationValue=false;
					break;
					
				case "string":
					if(colHashMap.containsKey(myLeftExpInStr) && colHashMap.containsKey(myRightExpInStr))
					{
						myLeftExpression.accept(this);
						leftStrValue=getStrValue();
						myRightExpression.accept(this);
						rightStrValue=getStrValue();
						if(leftStrValue.equals(rightStrValue))
							tupleEvaluationValue=true;
						else
							tupleEvaluationValue=false;
					}
					else
					{
						if(colHashMap.containsKey(myLeftExpInStr))
						{
							myLeftExpression.accept(this);
							leftStrValue=getStrValue();
							myRightExpression.accept(this);
							rightStrValue=getStrValue();
							rightStrValue=rightStrValue.substring(1,rightStrValue.length()-1);
							if(leftStrValue.equals(rightStrValue))
								tupleEvaluationValue=false;
							else
								tupleEvaluationValue=true;
						}
						else if(colHashMap.containsKey(myRightExpInStr))
						{
							myLeftExpression.accept(this);
							leftStrValue=getStrValue();
							leftStrValue=leftStrValue.substring(1, leftStrValue.length()-1);
							myRightExpression.accept(this);
							rightStrValue=getStrValue();
							if(leftStrValue.equals(rightStrValue))
								tupleEvaluationValue=false;
							else
								tupleEvaluationValue=true;
						}
					}
					break;
					default :
					//	System.out.println("invalid column name");
			}
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
	}

	@Override
	public void visit(Column column) {
		// TODO Auto-generated method stub
		List<String> temp=new LinkedList<String>(this.getHashMap().get(this.getTableName()).keySet());
		int columnIndex=temp.indexOf(column.toString());
		Date leftDateValue=null,rightDateValue=null;
		String tempColumn=column.getWholeColumnName();
		try
		{
			if(colHashMap.containsKey(tempColumn))
			{
				if(colHashMap.get(tempColumn)==Schema.INT)//(this.getHashMap().get(this.getTableName()).get(column.toString()).equals(Schema.INT))
				{
					this.intValue=Integer.parseInt(tuple.get(columnIndex).toString());
					this.type=Schema.INT;
				}
				else if (colHashMap.get(tempColumn)==Schema.FLOAT)//(this.getHashMap().get(this.getTableName()).get(column.toString()).equals(Schema.FLOAT)) 
				{
					this.doubleValue=Double.parseDouble(tuple.get(columnIndex).toString());
					this.type=Schema.FLOAT;
				}
				else if (colHashMap.get(tempColumn)==Schema.STRING)//(this.getHashMap().get(this.getTableName()).get(column.toString()).equals(Schema.STRING)) 
				{
					this.strValue=(String)tuple.get(columnIndex).toString();
					this.type=Schema.STRING;
				}
				else if (colHashMap.get(tempColumn)==Schema.DATE)//(this.getHashMap().get(this.getTableName()).get(column.toString()).equals(Schema.DATE)) 
				{
					//Date tempDate=new Date(tuple.get(columnIndex).toString());
					this.dateValue=formatter.parse(tuple.get(columnIndex).toString());
					this.type=Schema.DATE;
				}
				else if (colHashMap.get(tempColumn)==Schema.BOOL)//(this.getHashMap().get(this.getTableName()).get(column.toString()).equals(Schema.BOOL)) 
				{
					this.booleanValue=Boolean.valueOf(tuple.get(columnIndex).toString());
					this.type=Schema.BOOL;
				}
			}
			else
				System.out.println("Column name does not match any columns in the specified table ");		
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
	}

	@Override
	public void visit(SubSelect arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(CaseExpression arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(WhenClause arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(ExistsExpression arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(AllComparisonExpression arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(AnyComparisonExpression arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(Concat arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(Matches arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(BitwiseAnd arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(BitwiseOr arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(BitwiseXor arg0) {
		// TODO Auto-generated method stub

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
	public HashMap<String, LinkedHashMap<String,Schema>> getHashMap() {
		// TODO Auto-generated method stub
		return input.getHashMap();
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
	public long getTableSize()
	{
		// TODO Auto-generated method stub
		return input.getTableSize();
	}

}
