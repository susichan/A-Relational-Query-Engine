package edu.buffalo.cse562;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.Date;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map.Entry;

import jdbm.RecordManager;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.ExpressionVisitor;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.AllTableColumns;
import net.sf.jsqlparser.statement.select.FromItemVisitor;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.OrderByVisitor;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.select.SelectItemVisitor;
import net.sf.jsqlparser.statement.select.SelectVisitor;
import net.sf.jsqlparser.statement.select.SubJoin;
import net.sf.jsqlparser.statement.select.SubSelect;
import net.sf.jsqlparser.statement.select.Union;
import net.sf.jsqlparser.util.deparser.SelectDeParser;

public class SelectDeParserExtend  implements SelectVisitor,OrderByVisitor,SelectItemVisitor,FromItemVisitor{
	
	

	
	Operator input;
	HashMap<String, LinkedHashMap<String,Schema>> tables;
	String dataDir,swapDir;
	boolean multipleJoin,noSelect,globalBaseTableFlag=false;
	ColumnIdentifier c1=null;
	RecordManager recManager=null;
	HashSet<String> indexHash;
	
	public SelectDeParserExtend(){
		
	}
	public SelectDeParserExtend(HashMap<String, LinkedHashMap<String,Schema>> tables,String dataDir,String swapString, RecordManager rec,HashSet<String> indexHash){
		
		this.tables=tables;
		this.dataDir=dataDir;
		this.swapDir=swapString;
		this.recManager=rec;
		this.indexHash=indexHash;
		
	}
	
	
	
	Operator getOperator(){
		return input;
	}
	
	@Override
	public void visit(Table arg0) {
		// TODO Auto-generated method stub
		try {
			String tableName;
			if(arg0.getAlias()!=null)
				tableName=arg0.getAlias();
			else
				tableName=arg0.getName();
			input=new FileScanOperator(dataDir+File.separator+arg0.getName()+".dat",arg0,this.tables,multipleJoin,swapDir,c1.columnDetails.get(tableName));
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) 
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

	@Override
	public void visit(SubSelect arg0) {
		// TODO Auto-generated method stub
		if(arg0.getSelectBody()!=null)
			arg0.getSelectBody().accept(this);
		
		
	}

	@Override
	public void visit(SubJoin arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(AllColumns arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(AllTableColumns arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(SelectExpressionItem arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(OrderByElement arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(PlainSelect arg0) {
		// TODO Auto-generated method stub
		
		if(!(arg0.getFromItem() instanceof SubSelect))
		{
			c1=new ColumnIdentifier(arg0, tables);
			c1.visit(arg0);
		}
		
		if(arg0.getJoins()!=null)
		if(arg0.getJoins().size()>=1)
			multipleJoin=true;
		if(arg0.getFromItem()!=null)
		arg0.getFromItem().accept(this);
		//Join Operator
		if(arg0.getJoins()!=null)
		{
			try 
			{
				boolean nonIndexFlag=false,baseTableFlag=false;
				int indexEnum=0;
				String baseTable;
				Expression expr=arg0.getWhere();
				baseTable=input.getTableName();
				SelectExpression selTemp=new SelectExpression(input.getTableName());
				expr.accept(selTemp);
				Expression returnedExpression=null;
				if((returnedExpression=selTemp.getCustomizedExpression())!=null)
				{
						
					ArrayList<Expression>  tempList=returnArrExpres(returnedExpression);
					for(Expression exprtemp:tempList)
						input=new SelectOperator(exprtemp, input);
					baseTableFlag=true;
				}
			
				SelectExpression joinExpr=null;
		
				for(int i=0;i<arg0.getJoins().size();i++)
				{
					Table t=(Table)(((Join)(arg0.getJoins().get(i))).getRightItem());
					String tableName;
					if(t.getAlias()!=null)
						tableName=t.getAlias();
					else
						tableName=t.getName();
					Operator right=new FileScanOperator(dataDir+File.separator+t.getName()+".dat",t,this.tables,multipleJoin,swapDir,c1.columnDetails.get(tableName));
					SelectExpression selIter=new SelectExpression(right.getTableName());
					expr.accept(selIter);
					if((returnedExpression=selIter.getCustomizedExpression())!=null)
					{
						if(getIndexScan(returnedExpression))
						{
							if(returnedExpression instanceof EqualsTo)
							right=new IndexScanOperator( t, this.tables, multipleJoin, swapDir,  recManager, returnedExpression,true,c1.columnDetails.get(tableName));
							else
							right=new IndexScanOperator( t, this.tables, multipleJoin, swapDir,  recManager, returnedExpression,false,c1.columnDetails.get(tableName));
						
						}
						else{
						ArrayList<Expression>  tempList=returnArrExpres(returnedExpression);
						for(Expression exprtemp:tempList)
						{
							right=new SelectOperator(exprtemp, right);
						}
						}
						nonIndexFlag=true;
					}
					noSelect=true;
					if(t.getAlias()!=null)
						joinExpr=new SelectExpression(input.getTableName(), t.getAlias());
					else
						joinExpr=new SelectExpression(input.getTableName(), t.getName());
					
					expr.accept(joinExpr);
					if((returnedExpression=joinExpr.getCustomizedExpression())!=null)
					{
						indexEnum=findOperator(returnedExpression, tableName, nonIndexFlag,baseTable,baseTableFlag,t.getName(),t.getAlias());
						if(indexEnum==1)
							input=new IndexNLJ(input, recManager, returnedExpression, true, this.tables);
						else if(indexEnum==2)
							input=new IndexNLJ(input, recManager, returnedExpression, false, this.tables);
						else if(indexEnum==4)
							input=new IndexNLJ(right, recManager, returnedExpression, true, this.tables);
						else if(indexEnum==5)
							input=new IndexNLJ(right, recManager, returnedExpression, false, this.tables);
						else
							input=new HashJoinOperator(input,right, returnedExpression);
					}
					nonIndexFlag=false;
				}
				if(joinExpr.tempExpression!=null)
					input=new SelectOperator(joinExpr.tempExpression, input);
				} catch (FileNotFoundException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (ParseException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} 
			}
		
		//select operator
		if(arg0.getWhere()!=null && !noSelect)
			input=new SelectOperator(arg0.getWhere(), input);
		if(arg0.getSelectItems()!=null)
		{
			//Project Operator
			input=new ProjectOperator(arg0.getOrderByElements(),arg0.getGroupByColumnReferences(),arg0.getSelectItems(), input);
		}	
		
	}
	
	
	public boolean getIndexScan(Expression expr){
		if(expr instanceof AndExpression){
			AndExpression anExpr=(AndExpression)expr;
			Expression leftExpr=anExpr.getLeftExpression();
			if(leftExpr instanceof GreaterThan){
				
				if(indexHash.contains(((GreaterThan)leftExpr).getLeftExpression().toString()))
					return true;
			}
			else if(leftExpr instanceof MinorThan){
				if(indexHash.contains(((MinorThan)leftExpr).getLeftExpression().toString()))
					return true;;
			}
			else if(leftExpr instanceof GreaterThanEquals){
				if(indexHash.contains(((GreaterThanEquals)leftExpr).getLeftExpression().toString()))
					return true;
			}
			else if(leftExpr instanceof MinorThanEquals){
				if(indexHash.contains(((MinorThanEquals)leftExpr).getLeftExpression().toString()))
					return true;
			}
			
		}
		else{
			if(expr instanceof GreaterThan){
				
				if(indexHash.contains(((GreaterThan)expr).getLeftExpression().toString()))
					return true;
			}
			else if(expr instanceof MinorThan){
				if(indexHash.contains(((MinorThan)expr).getLeftExpression().toString()))
					return true;
			}
			else if(expr instanceof GreaterThanEquals){
				if(indexHash.contains(((GreaterThanEquals)expr).getLeftExpression().toString()))
						return true;
			}
			else if(expr instanceof MinorThanEquals){
				if(indexHash.contains(((MinorThanEquals)expr).getLeftExpression().toString()))
					return true;
			}
			else if(expr instanceof EqualsTo){
				if(indexHash.contains(((EqualsTo)expr).getLeftExpression().toString()))
					return true;
			}
		
	}
		return false;
	}
	
	
	
	
	public int findOperator(Expression expr,String tableName,boolean filterValue,String baseTable,boolean baseTableFlag,String table,String alias)
	{
		int returnValue=0;
		if(expr instanceof EqualsTo)
		{
			Expression leftExpression=null, rightExpression=null;
			leftExpression=((EqualsTo) expr).getLeftExpression();
			rightExpression=((EqualsTo) expr).getRightExpression();
			String right=rightExpression.toString();
			if(rightExpression.toString().startsWith(alias+"."))
			{
				String[] tempArray=right.split("\\.");
				right=table+"."+tempArray[1];
			}
			if(leftExpression.toString().startsWith(baseTable+".") && globalBaseTableFlag==false)
			{
				
				if(indexHash.contains(leftExpression.toString()) && baseTableFlag==false)
				{
					returnValue=4;
						globalBaseTableFlag=true;
				}
				else if(indexHash.contains(right) && filterValue==false)
				{
						returnValue=2;
				}
				else 
					returnValue=3;
			}
			else if(rightExpression.toString().startsWith(baseTable+".") && globalBaseTableFlag==false)
			{
				
				if(indexHash.contains(right) && baseTableFlag==false)
				{
						returnValue=5;
						globalBaseTableFlag=true;
				}
				else if(indexHash.contains(leftExpression.toString()) && filterValue==false)
				{
					returnValue=1;
				}
				else
					returnValue=3;
			}
			else if((!leftExpression.toString().startsWith(baseTable+".") &&  !rightExpression.toString().startsWith(baseTable+".")) || globalBaseTableFlag==true)
			{
				if(leftExpression.toString().startsWith(tableName+".") && indexHash.contains(leftExpression.toString()))
				{
						if(filterValue==false)
						{
							returnValue=1;
						}
						else
						{
							returnValue=3;
						}
				}
				else if(rightExpression.toString().startsWith(tableName+".") && indexHash.contains(right))
				{
						if(filterValue==false)
						{
							returnValue=2;
						}
						else
						{
							returnValue=3;
						}
				}
				else
				{
					returnValue=3;
				}
			}
		}
		return returnValue;
	}
	
	

	@Override
	public void visit(Union arg0) {
		// TODO Auto-generated method stub
	}
	
	
	public ArrayList<Expression> returnArrExpres(Expression paramExpr){
		ArrayList<Expression> retExpList=new ArrayList<>();
		if(!(paramExpr instanceof AndExpression))
		{
			retExpList.add(paramExpr);
			return retExpList;
		}
		while(paramExpr instanceof AndExpression){
			
		if(paramExpr instanceof AndExpression)
		{
			retExpList.add(((AndExpression) paramExpr).getRightExpression());
			paramExpr=((AndExpression) paramExpr).getLeftExpression();
		}
		
		}
		if(paramExpr!=null)
			retExpList.add(paramExpr);
		
		
		return retExpList;
	}
	


}
