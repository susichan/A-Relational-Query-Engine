package edu.buffalo.cse562;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;

import net.sf.jsqlparser.expression.DoubleValue;
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

public class ProjectExpressionExtend1 extends AbstractExpressionVisitor implements SelectItemVisitor {

	LinkedHashMap<String, Schema> originalColumnStructure;
	Tuple t;
	Tuple currentTuple;
	String tableName;
	Schema type;
	Object value;
	String aggString;
	LinkedList<String> tempList;
	
	public ProjectExpressionExtend1(HashMap<String, LinkedHashMap<String, Schema>> originalColumnStructure,String tableName){
		
		currentTuple=new Tuple();
		this.tableName=tableName;
		this.originalColumnStructure=originalColumnStructure.get(this.tableName);
		tempList=new LinkedList<String>(this.originalColumnStructure.keySet());
	}
	
	void setTuple(Tuple t){
		this.t=t;
	}
	void clearTuple(){
		currentTuple=new Tuple();
	}
	Tuple getTuple(){
		return currentTuple;
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
		{
			if(arg0.getExpression() instanceof Function){
				if(arg0.getAlias()!=null)
				aggString=arg0.getAlias();
				else
				aggString=((Function)arg0.getExpression()).getName()+((Function)arg0.getExpression()).getParameters();
			}
			arg0.getExpression().accept(this);
		}
			currentTuple.add(value);
	
		
	}
	@Override
	public void visit(Function arg0) {
		// TODO Auto-generated method stub
		if(aggString!=null){
			
		int index=tempList.indexOf(aggString);
		type=this.originalColumnStructure.get(aggString);
		value=t.get(index);
		}

	}
	@Override
	public void visit(DoubleValue arg0) {
		// TODO Auto-generated method stub

		if(type==Schema.INT)
			value=new Integer((int)arg0.getValue());
		else if(type==Schema.FLOAT)
			value=new Double((double)arg0.getValue());
		
	}

	@Override
	public void visit(LongValue arg0) {
		// TODO Auto-generated method stub
		if(type==Schema.INT)
			value=new Integer((int)arg0.getValue());
		else if(type==Schema.FLOAT)
			value=new Double((double)arg0.getValue());
	}
	
	@Override
	public void visit(Parenthesis arg0) {
		// TODO Auto-generated method stub
		arg0.getExpression().accept(this);
	}
	@Override
	public void visit(Addition arg0) {
		// TODO Auto-generated method stub
		arg0.getLeftExpression().accept(this);
		Object leftTemp=value;
		arg0.getRightExpression().accept(this);
		Object rightTemp=value;
		if(type==Schema.INT){
			value=(Integer)(Integer.parseInt(leftTemp.toString()))+(Integer)(Integer.parseInt(rightTemp.toString()));
		}
		if(type==Schema.FLOAT){
			value=(Double)(Double.parseDouble(leftTemp.toString()))+(Double)(Double.parseDouble(rightTemp.toString()));
		}
	}

	@Override
	public void visit(Division arg0) {
		// TODO Auto-generated method stub
		arg0.getLeftExpression().accept(this);
		Object leftTemp=value;
		arg0.getRightExpression().accept(this);
		Object rightTemp=value;
		if(type==Schema.INT){
			value=(Integer)(Integer.parseInt(leftTemp.toString()))/(Integer)(Integer.parseInt(rightTemp.toString()));
		}
		if(type==Schema.FLOAT){
			value=(Double)(Double.parseDouble(leftTemp.toString()))/(Double)(Double.parseDouble(rightTemp.toString()));
		}
	}

	@Override
	public void visit(Multiplication arg0) {
		// TODO Auto-generated method stub
		arg0.getLeftExpression().accept(this);
		Object leftTemp=value;
		arg0.getRightExpression().accept(this);
		Object rightTemp=value;
		if(type==Schema.INT){
			value=(Integer)(Integer.parseInt(leftTemp.toString()))*(Integer)(Integer.parseInt(rightTemp.toString()));
		}
		if(type==Schema.FLOAT){
			value=(Double)(Double.parseDouble(leftTemp.toString()))*(Double)(Double.parseDouble(rightTemp.toString()));
		}

	}

	@Override
	public void visit(Subtraction arg0) {
		// TODO Auto-generated method stub
		arg0.getLeftExpression().accept(this);
		Object leftTemp=value;
		arg0.getRightExpression().accept(this);
		Object rightTemp=value;
		if(type==Schema.INT){
			
			value=(Integer)(Integer.parseInt(leftTemp.toString()))-(Integer)(Integer.parseInt(rightTemp.toString()));
		}
		if(type==Schema.FLOAT){
			value=(Double)(Double.parseDouble(leftTemp.toString()))-(Double)(Double.parseDouble(rightTemp.toString()));
		}
	}
	
	
	@Override
	public void visit(Column arg0) {
		// TODO Auto-generated method stub
		int index=tempList.indexOf(arg0.getWholeColumnName());
		type=this.originalColumnStructure.get(arg0.getWholeColumnName());
		value=t.get(index);
	}

}
