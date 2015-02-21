package edu.buffalo.cse562;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;

import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.operators.arithmetic.Addition;
import net.sf.jsqlparser.expression.operators.arithmetic.Division;
import net.sf.jsqlparser.expression.operators.arithmetic.Multiplication;
import net.sf.jsqlparser.expression.operators.arithmetic.Subtraction;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.AllTableColumns;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItemVisitor;

public class ProjectExpressionExtend2 extends AbstractExpressionVisitor implements SelectItemVisitor {

	HashMap<String, LinkedHashMap<String, Schema>> originalColumnStructure;
	String tableName;
	LinkedHashMap<String,Schema> modColumnStructure;
	Schema type;
	LinkedList<String> tempList;
	
	
	public ProjectExpressionExtend2(HashMap<String, LinkedHashMap<String, Schema>> originalColumnStructure,String tableName){
		this.originalColumnStructure=originalColumnStructure;
		this.tableName=tableName;
		modColumnStructure=new LinkedHashMap<>();
		this.tempList=new LinkedList<String>(this.originalColumnStructure.get(this.tableName).keySet());
	}
	
	LinkedHashMap<String, Schema> get(){
		return modColumnStructure;
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
		if(arg0.getExpression()!=null)
			arg0.getExpression().accept(this);
		if(arg0.getAlias()!=null)
		{
			modColumnStructure.put(arg0.getAlias(),type);
		}
		else
		{
			modColumnStructure.put(arg0.toString(),type);
		}
		
	}
	
	@Override
	public void visit(DoubleValue arg0) {
		// TODO Auto-generated method stub
	}

	@Override
	public void visit(LongValue arg0) {
		// TODO Auto-generated method stub

	}
	
	@Override
	public void visit(Parenthesis arg0) {
		// TODO Auto-generated method stub
		arg0.getExpression().accept(this);
	}
	

	@Override
	public void visit(Division arg0) {
		// TODO Auto-generated method stub
		arg0.getLeftExpression().accept(this);
		arg0.getRightExpression().accept(this);
		
	}

	@Override
	public void visit(Multiplication arg0) {
		// TODO Auto-generated method stub
		arg0.getLeftExpression().accept(this);
		arg0.getRightExpression().accept(this);
		

	}

	@Override
	public void visit(Subtraction arg0) {
		// TODO Auto-generated method stub
		arg0.getLeftExpression().accept(this);
		arg0.getRightExpression().accept(this);
		
	}
	
	
	@Override
	public void visit(Column arg0) {
		// TODO Auto-generated method stub
		int index=tempList.indexOf(arg0.getWholeColumnName());
		type=this.originalColumnStructure.get(this.tableName).get(arg0.getWholeColumnName());
		
	}

}
