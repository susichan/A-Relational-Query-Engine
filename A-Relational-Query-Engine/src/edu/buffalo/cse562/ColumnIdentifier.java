package edu.buffalo.cse562;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import net.sf.jsqlparser.expression.AllComparisonExpression;
import net.sf.jsqlparser.expression.AnyComparisonExpression;
import net.sf.jsqlparser.expression.CaseExpression;
import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.ExpressionVisitor;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.InverseExpression;
import net.sf.jsqlparser.expression.JdbcParameter;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.NullValue;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.TimeValue;
import net.sf.jsqlparser.expression.TimestampValue;
import net.sf.jsqlparser.expression.WhenClause;
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
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.InExpression;
import net.sf.jsqlparser.expression.operators.relational.IsNullExpression;
import net.sf.jsqlparser.expression.operators.relational.LikeExpression;
import net.sf.jsqlparser.expression.operators.relational.Matches;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import net.sf.jsqlparser.expression.operators.relational.NotEqualsTo;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.AllTableColumns;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.FromItemVisitor;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.OrderByVisitor;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItemVisitor;
import net.sf.jsqlparser.statement.select.SelectVisitor;
import net.sf.jsqlparser.statement.select.SubJoin;
import net.sf.jsqlparser.statement.select.SubSelect;
import net.sf.jsqlparser.statement.select.Union;

public class ColumnIdentifier implements SelectVisitor, OrderByVisitor,
SelectItemVisitor, FromItem,ExpressionVisitor, FromItemVisitor{

	HashMap<String, HashSet<String>> columnDetails=new HashMap<>();
	PlainSelect query;
	ArrayList<String> tableNames=new ArrayList<>();
	HashMap<String, LinkedHashMap<String, Schema>> tables;
	HashSet<String> columnList;
	
	public ColumnIdentifier(PlainSelect query,HashMap<String, LinkedHashMap<String, Schema>> table) {
		this.query=query;
		this.tables=table;
	}
	@Override
	public void visit(NullValue arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(Function arg0) {
		// TODO Auto-generated method stub
		
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
	public void visit(DoubleValue arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(LongValue arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(DateValue arg0) {
		// TODO Auto-generated method stub
		
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
	public void visit(Parenthesis arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(StringValue arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(Addition arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(Division arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(Multiplication arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(Subtraction arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(AndExpression arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(OrExpression arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(Between arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(EqualsTo arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(GreaterThan arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(GreaterThanEquals arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(InExpression arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(IsNullExpression arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(LikeExpression arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(MinorThan arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(MinorThanEquals arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(NotEqualsTo arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(Column arg0) {
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
	public void visit(Table input) {
		// TODO Auto-generated method stub
		tableNames.add(input.toString());
	}

	@Override
	public void visit(SubSelect arg0) {
		// TODO Auto-generated method stub
		
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
	public void visit(PlainSelect input) {
		// TODO Auto-generated method stub
		HashMap<String, ArrayList<String>> tableAliases= new HashMap<String, ArrayList<String>>();
		ArrayList<String> a =new ArrayList<>();
		
		if(input.getFromItem().getAlias()!= null)
		{
			a.add(input.getFromItem().getAlias());
			tableAliases.put(input.getFromItem().toString(),a);
		}
		for (int i = 0; i < input.getJoins().size(); i++)
		{
			Table t = (Table) (((Join) (input.getJoins().get(i))).getRightItem());
			if(t.getAlias()!=null)
			{
				if(tableAliases.containsKey(t.getName()))
					tableAliases.get(t.getName()).add(t.getAlias());
				else
				{
					ArrayList<String> b=new ArrayList<>();
					b.add(t.getAlias());
					tableAliases.put(t.getName(), b);
				}
			}
		}
		String alias;
		List<String> tableNames= new ArrayList<>(tables.keySet());
		for(String tablename : tables.keySet())
		{		
			columnList=new HashSet<String>();
				if(tableAliases.containsKey(tablename))
				{
					for(String c : tableAliases.get(tablename))
					{	
						for(String columnName : tables.get(tablename).keySet())
						{
							int flag=0;
							if(input.getSelectItems()!=null && input.getSelectItems().toString().contains(c+"."+columnName))
								flag=1;
							if(input.getWhere()!=null && input.getWhere().toString().contains(c+"."+columnName))
								flag=1;
							if(input.getGroupByColumnReferences() !=null && input.getGroupByColumnReferences().toString().contains(c+"."+columnName) )
								flag=1;
							if(input.getOrderByElements()!=null && input.getOrderByElements().toString().contains(c+"."+columnName))
								flag=1;
							if(flag==1)
							{	
								columnList.add(c+"."+columnName);
							}
						}
						columnDetails.put(c,columnList);
						columnList=new HashSet<String>();
					}
				}
				else
				{
					for(String columnName : tables.get(tablename).keySet())
					{
						int flag=0;
						if(input.getSelectItems()!=null && input.getSelectItems().toString().contains(tablename+"."+columnName))
							flag=1;
						if(input.getWhere()!=null && input.getWhere().toString().contains(tablename+"."+columnName))
							flag=1;
						if(input.getGroupByColumnReferences() !=null && input.getGroupByColumnReferences().toString().contains(tablename+"."+columnName) )
							flag=1;
						if(input.getOrderByElements()!=null && input.getOrderByElements().toString().contains(tablename+"."+columnName))
							flag=1;
						if(flag==1)
						{
							columnList.add(tablename+"."+columnName);
						}
					}
					columnDetails.put(tablename,columnList);
					columnList=new HashSet<String>();
				}
			}
		
		//for(String d : columnList)
		//System.out.println("** "+d);
		
		
}
		
		
		
	

	@Override
	public void visit(Union arg0) {
		// TODO Auto-generated method stub
		
	}
	@Override
	public void accept(FromItemVisitor input) {
		// TODO Auto-generated method stub
		System.out.println("from "+input);
	}
	@Override
	public String getAlias() {
		// TODO Auto-generated method stub
		return null;
	}
	@Override
	public void setAlias(String arg0) {
		// TODO Auto-generated method stub
		
	}

}
