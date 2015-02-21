package edu.buffalo.cse562;

import java.io.ObjectInputStream.GetField;

import javax.print.DocFlavor.INPUT_STREAM;

import net.sf.jsqlparser.expression.AllComparisonExpression;
import net.sf.jsqlparser.expression.AnyComparisonExpression;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.CaseExpression;
import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
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
import net.sf.jsqlparser.statement.select.SubSelect;

public class SelectExpression extends AbstractExpressionVisitor
{
	String tableName1 = null;
	String tableName2 = null;
	Expression expression = null, tempExpression = null;
	Boolean andBoolean = false;
	Boolean orBoolean = false;
	Boolean globalBooleanValue = false;
	String superiorName = null;
	String[] table1Array;
	int parenthesisFlag = 0;

	public SelectExpression(String tableName)
	{
		this.tableName1 = tableName;
	}

	public SelectExpression(String tableName, String tableName2)
	{
		// System.out.println(tableName+"  tabela "+tableName2);
		if (tableName.indexOf("|") != -1)
		{
			this.table1Array = tableName.split("\\|");
			tableName = table1Array[table1Array.length - 1];

		}
		// System.out.println(tableName+"  tabela "+tableName2);
		this.tableName1 = tableName;
		this.tableName2 = tableName2;
	}

	public Expression getCustomizedExpression()
	{

		return this.expression;
	}

	@Override
	public void visit(NullValue arg0)
	{
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(Function arg0)
	{
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(InverseExpression arg0)
	{
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(JdbcParameter arg0)
	{
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(DoubleValue arg0)
	{
		// TODO Auto-generated method stub
	}

	@Override
	public void visit(LongValue arg0)
	{
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(DateValue arg0)
	{
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(TimeValue arg0)
	{
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(TimestampValue arg0)
	{
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(Parenthesis input)
	{
		// TODO Auto-generated method stub
		if (parenthesisFlag == 0)
		{
			tempExpression = input;
			parenthesisFlag = 1;
		}

		input.getExpression().accept(this);
		if(this.expression!=null)
			this.expression=new Parenthesis(this.expression);
	}

	@Override
	public void visit(StringValue arg0)
	{
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(Addition arg0)
	{
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(Division arg0)
	{
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(Multiplication arg0)
	{
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(Subtraction arg0)
	{
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(AndExpression input)
	{
		Boolean leftExpression = null, rightExpression = null;
		Expression leftTempExpression = null, rightTempExpression = null;

		// TODO Auto-generated method stub

		input.getLeftExpression().accept(this);
		leftTempExpression = this.expression;
		this.expression = null;

		input.getRightExpression().accept(this);
		rightTempExpression = this.expression;
		this.expression = null;

		if (leftTempExpression != null && rightTempExpression == null)
		{
			this.expression = leftTempExpression;
		} else if (leftTempExpression == null && rightTempExpression != null)
		{
			this.expression = rightTempExpression;
		} else if (leftTempExpression != null && rightTempExpression != null)
		{
			this.expression = new AndExpression(leftTempExpression,
					rightTempExpression);
		}
		/*
		 * else if(leftTempExpression==null && rightTempExpression==null) {
		 * this.expression=null; }
		 */

	}

	@Override
	public void visit(OrExpression input)
	{
		// TODO Auto-generated method stub

		Expression leftTempExpression = null, rightTempExpression = null;

		input.getLeftExpression().accept(this);
		leftTempExpression = this.expression;
		this.expression = null;

		input.getRightExpression().accept(this);
		rightTempExpression = this.expression;
		this.expression = null;
		if (leftTempExpression != null && rightTempExpression == null)
		{
			this.expression = leftTempExpression;
		} else if (leftTempExpression == null && rightTempExpression != null)
		{
			this.expression = rightTempExpression;
		} else if (leftTempExpression != null && rightTempExpression != null)
		{
			this.expression = new OrExpression(leftTempExpression,
					rightTempExpression);
		}
		/*
		 * else if(leftTempExpression==null && rightTempExpression==null) {
		 * this.expression=null; }
		 */
	}

	@Override
	public void visit(Between arg0)
	{
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(EqualsTo input)
	{
		// TODO Auto-generated method stub
		this.getExpressionStatus(input);
	}

	@Override
	public void visit(GreaterThan input)
	{
		// TODO Auto-generated method stub
		this.getExpressionStatus(input);
	}

	@Override
	public void visit(GreaterThanEquals input)
	{
		// TODO Auto-generated method stub
		this.getExpressionStatus(input);

	}

	@Override
	public void visit(InExpression arg0)
	{
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(IsNullExpression arg0)
	{
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(LikeExpression input)
	{
		// TODO Auto-generated method stub
		this.getExpressionStatus(input);

	}

	@Override
	public void visit(MinorThan input)
	{
		// TODO Auto-generated method stub
		this.getExpressionStatus(input);
	}

	@Override
	public void visit(MinorThanEquals input)
	{
		// TODO Auto-generated method stub
		this.getExpressionStatus(input);
	}

	@Override
	public void visit(NotEqualsTo input)
	{
		// TODO Auto-generated method stub
		// System.out.println("not equals:  "+input);
		this.getExpressionStatus(input);
	}

	@Override
	public void visit(Column arg0)
	{
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(SubSelect arg0)
	{
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(CaseExpression arg0)
	{
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(WhenClause arg0)
	{
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(ExistsExpression arg0)
	{
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(AllComparisonExpression arg0)
	{
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(AnyComparisonExpression arg0)
	{
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(Concat arg0)
	{
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(Matches arg0)
	{
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(BitwiseAnd arg0)
	{
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(BitwiseOr arg0)
	{
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(BitwiseXor arg0)
	{
		// TODO Auto-generated method stub

	}

	public void visit(Expression arg0)
	{
		// TODO Auto-generated method stub

	}

	public void getExpressionStatus(BinaryExpression input)
	{
		String leftExpression = input.getLeftExpression().toString();
		String rightExpression = input.getRightExpression().toString();
		// System.out.println(leftExpression+" = "+rightExpression);
		if (this.tableName2 == null)
		{
			// System.out.println("tbl "+tableName1);
			if ((leftExpression.startsWith(tableName1 + ".") && (input.getRightExpression().getClass() != Column.class))
					|| (rightExpression.startsWith(tableName1 + ".") && (input.getLeftExpression().getClass() != Column.class))
					|| (leftExpression.startsWith(tableName1 + ".") && rightExpression.startsWith(tableName1 + ".")))
			{
				globalBooleanValue = true;
				this.expression = input;
				// System.out.println(input.getLeftExpression().getClass()+" = "+input.getRightExpression().getClass());
			} else
			{
				globalBooleanValue = false;
				// this.expression=null;
			}
		} else
		{
			if (table1Array != null)
			{
				// System.out.println(tableName1+" *** "+tableName2);
				for (int i = 0; i < table1Array.length; i++)
				{
					this.tableName1 = table1Array[i];
					// System.out.println(tableName1+" *** "+tableName2+"  %%%%  "+leftExpression+"  ***   "+rightExpression);
					// tableName1=tableName1+".";
					// System.out.println("tab 1 "+);
					if ((leftExpression.startsWith(tableName1 + ".") && rightExpression.startsWith(tableName2 + "."))
							|| (leftExpression.startsWith(tableName2 + ".") && rightExpression.startsWith(tableName1 + ".")))
					{
						// System.out.println(leftExpression+"    "+rightExpression);
						// System.out.println("true");
						globalBooleanValue = true;
						this.expression = input;
						// System.out.println(" ** "+this.expression);
					} else
					{
						globalBooleanValue = false;
						// this.expression=null;
					}
				}
			} else
			{
				// System.out.println(tableName1+" *** "+tableName2);
				if ((leftExpression.startsWith(tableName1 + ".") && rightExpression.startsWith(tableName2 + "."))
						|| (leftExpression.startsWith(tableName2 + ".") && rightExpression.startsWith(tableName1 + ".")))
				{
					// System.out.println(tableName1+" *** "+tableName2);
					globalBooleanValue = true;
					this.expression = input;
				} else
				{
					globalBooleanValue = false;
					// this.expression=null;
				}
			}
		}
	}
}
