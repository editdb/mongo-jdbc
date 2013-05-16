// Executor.java

/**
 *      Copyright (C) 2008 10gen Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 *   
 *   ------------------------------------------------------------------------
 *   Changed by Nigel Maddocks, May 2013
 */

package com.mongodb.jdbc;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Pattern;

import org.bson.BSONException;
import org.bson.BSONObject;
import org.bson.types.ObjectId;

import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.schema.*;
import net.sf.jsqlparser.parser.*;
import net.sf.jsqlparser.statement.*;
import net.sf.jsqlparser.statement.select.*;
import net.sf.jsqlparser.statement.insert.*;
import net.sf.jsqlparser.statement.update.*;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.drop.*;

import com.mongodb.*;
import com.mongodb.util.JSON;

public class Executor {

    static final boolean D = false;

    final DB _db;
    final String _sql;
    final java.sql.Statement _sqlStatement;
    final Statement _statement;
    
    List _params;
    int _pos;
    Object _lastCreatedId = null;    
    String _lastOperation = null;
    String _noColumnValue = null; // The string value to indicate that the column is not present
    
    static final SimpleDateFormat sdfYYYYMMDD; static {sdfYYYYMMDD = new SimpleDateFormat("yyyy-MM-dd");}
    static final SimpleDateFormat sdfYYYYMMDDHHMM; static {sdfYYYYMMDDHHMM = new SimpleDateFormat("yyyy-MM-dd hh:mm");}
    static final SimpleDateFormat sdfYYYYMMDDHHMMSS; static {sdfYYYYMMDDHHMMSS = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");}
    
/** - prefer to have reference to the statement (see below)
    Executor( DB db , String sql )
        throws MongoSQLException {
        _db = db;
        _sql = sql;
        _statement = parse( sql );

        if ( D ) System.out.println( sql );
    }
*/    
    public Executor( MongoStatement stmt , String sql )
	    throws MongoSQLException {
    	_sqlStatement = stmt; 
    	_noColumnValue = stmt._conn._NoColumnValue;
	    _db = stmt._conn._db;
	    _sql = sql;
	    _statement = parse( sql );

	    if ( D ) System.out.println( sql );
	}

    void setParams( List params ){
        _pos = 1;
        _params = params;
    }

    DBCursor query()
        throws MongoSQLException {
        if ( ! ( _statement instanceof Select ) )
            throw new IllegalArgumentException( "not a query sql statement" );
        
        Select select = (Select)_statement;
        if ( ! ( select.getSelectBody() instanceof PlainSelect ) )
            throw new UnsupportedOperationException( "can only handle PlainSelect so far" );
        
        PlainSelect ps = (PlainSelect)select.getSelectBody();
        if ( ! ( ps.getFromItem() instanceof Table ) )
            throw new UnsupportedOperationException( "can only handle regular tables" );
        
        DBCollection coll = getCollection( (Table)ps.getFromItem() );

        BasicDBObject fields = new BasicDBObject();
        for ( Object o : ps.getSelectItems() ){
            SelectItem si = (SelectItem)o;
            if ( si instanceof AllColumns ){
                if ( fields.size() > 0 )
                    throw new UnsupportedOperationException( "can't have * and fields" );
                break;
            }
            else if ( si instanceof SelectExpressionItem ){
                SelectExpressionItem sei = (SelectExpressionItem)si;
                fields.put( toFieldName( sei.getExpression() ) , 1 );
            }
            else {
                throw new UnsupportedOperationException( "unknown select item: " + si.getClass() );
            }
        }
        
        // where
        DBObject query = parseWhere( ps.getWhere() );
        
        // done with basics, build DBCursor
        if ( D ) System.out.println( "\t" + "table: " + coll );
        if ( D ) System.out.println( "\t" + "fields: " + fields );
        if ( D ) System.out.println( "\t" + "query : " + query );
        DBCursor c = coll.find( query , fields );
        _lastOperation = "db." + coll.getName() + ".find(" + serializeToJSON(query) + ", " + serializeToJSON(fields) + ")";
        
        { // order by
            List orderBylist = ps.getOrderByElements();
            if ( orderBylist != null && orderBylist.size() > 0 ){
                BasicDBObject order = new BasicDBObject();
                for ( int i=0; i<orderBylist.size(); i++ ){
                    OrderByElement o = (OrderByElement)orderBylist.get(i);
//nima              order.put( o.getColumnReference().toString() , o.isAsc() ? 1 : -1 );
                    order.put( o.getExpression().toString() , o.isAsc() ? 1 : -1 );		//nima
                }
                c.sort( order );
                _lastOperation += ".sort(" + serializeToJSON(order) + ")";
            }
        }

        
        return c;
    }

    int writeop()
        throws MongoSQLException {
        
        if ( _statement instanceof Insert )
            return insert( (Insert)_statement );
        else if ( _statement instanceof Update )
            return update( (Update)_statement );
        else if ( _statement instanceof Drop )
            return drop( (Drop)_statement );
        else if ( _statement instanceof Delete )
        	return delete( (Delete)_statement );
            

        throw new RuntimeException( "unknown write: " + _statement.getClass() );
    }
    
    int insert( Insert in )
        throws MongoSQLException {
    	_lastCreatedId = null;
    	
        if ( in.getColumns() == null )
            throw new MongoSQLException.BadSQL( "have to give column names to insert" );
        
        DBCollection coll = getCollection( in.getTable() );
        if ( D ) System.out.println( "\t" + "table: " + coll );
        
        if ( ! ( in.getItemsList() instanceof ExpressionList ) )
            throw new UnsupportedOperationException( "need ExpressionList" );
        
        BasicDBObject o = new BasicDBObject();

        List valueList = ((ExpressionList)in.getItemsList()).getExpressions();
        if ( in.getColumns().size() != valueList.size() )
            throw new MongoSQLException.BadSQL( "number of values and columns have to match" );

        for ( int i=0; i<valueList.size(); i++ ){
            Object value = toConstant( (Expression)valueList.get(i) );
//          if (value instanceof String && value.toString().equals("º")) {            	
            if (value instanceof String && _noColumnValue != null && value.toString().equals(_noColumnValue)) {            	
            } else {
            	o.put( in.getColumns().get(i).toString() , toConstant( (Expression)valueList.get(i) ) );
            }
        }

        coll.insert( o );
        _lastCreatedId = o.get("_id");
        _lastOperation = "db." + coll.getName() + ".insert(" + serializeToJSON(o) + ")";

        return 1; // TODO - this is wrong
    }

    int update( Update up )
        throws MongoSQLException {
        
        DBObject query = parseWhere( up.getWhere() );
        
        BasicDBObject set = new BasicDBObject();
        BasicDBObject removalset = new BasicDBObject();
        
        for ( int i=0; i<up.getColumns().size(); i++ ){
            String k = up.getColumns().get(i).toString();
            Expression v = (Expression)(up.getExpressions().get(i));
            Object value = toConstant( v );
//          if (value instanceof String && value.toString().equals("º")) {
            if (value instanceof String && _noColumnValue != null && value.toString().equals(_noColumnValue)) {
            	removalset.put(k.toString(), 1);
            } else {
            	set.put( k.toString() , value );
            }
        }

        DBObject mod = new BasicDBObject( "$set" , set );

        DBCollection coll = getCollection( up.getTable() );
        coll.update( query , mod );
        _lastOperation = "db." + coll.getName() + ".update(\n\t" + serializeToJSON(query) + ",\n\t" + serializeToJSON(mod) + "\n)";
        
        if (removalset.size() > 0) {
        	mod = new BasicDBObject( "$unset" , removalset );
            coll.update( query , mod );
            _lastOperation += "\n\n" + "db." + coll.getName() + ".update(\n\t" + serializeToJSON(query) + ",\n\t" + serializeToJSON(mod) + "\n)";
        }
        
        return 1; // TODO
    }

    int drop( Drop d ){
        DBCollection c = _db.getCollection( d.getName() );
        c.drop();
        return 1;
    }

    int delete( Delete del )
        throws MongoSQLException {
        
        DBObject query = parseWhere( del.getWhere() );

        DBCollection coll = getCollection( del.getTable() );
        coll.remove( query );
        _lastOperation = "db." + coll.getName() + ".remove(" + serializeToJSON(query) + ")";
        return 1; // TODO
    }

    // ---- helpers -----

    String toFieldName( Expression e ){
        if ( e instanceof StringValue )
            return e.toString();
        if ( e instanceof Column )
            return e.toString();
        throw new UnsupportedOperationException( "can't turn [" + e + "] " + e.getClass() + " into field name" );
    }

    Object toConstant( Expression e ){
    	//-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
        if ( e instanceof StringValue ) {
        	Date date = parseDate(((StringValue) e).getValue());
        	if (date == null) {
	        	// nima: jsqlparser might be returning a JSON object as a string
	        	//       so see if we can turn it back, else just return the string
	        	Object objJson = null;
	        	try {
	        		objJson = JSON.parse(((StringValue) e).getValue());
	        	} catch (Exception ex) {
	        	}
	        	if (objJson instanceof BasicDBObject || objJson instanceof BasicDBList) {
	        		return objJson;
	        	} else {
	        		return ((StringValue)e).getValue();
	        	}
        	} else {
/*        		
        		DateValue dv = new DateValue("1980-01-01");
        		dv.setValue(new java.sql.Date(date.getTime()));
        		return dv;
*/
        		return date;
        	}
        } 
        else if ( e instanceof DoubleValue )
            return ((DoubleValue)e).getValue();
        else if ( e instanceof LongValue )
            return ((LongValue)e).getValue();
        else if ( e instanceof NullValue )
            return null;
        else if ( e instanceof JdbcParameter )
            return _params.get( _pos++ );
    	//-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
                 
        //nima - try to coerce the type - principally for _id objectids
        boolean bCoerceOK = false;
        Expression e2 = null; 
        if (!bCoerceOK) try {e2 = new LongValue(e.toString()); bCoerceOK = true; } catch (Exception ex) {}
        if (!bCoerceOK) try {e2 = new DoubleValue(e.toString()); bCoerceOK = true; } catch (Exception ex) {}
        if (!bCoerceOK) {
        	try {
        		String sValue = e.toString();
        		if (!sValue.startsWith("'") && !sValue.endsWith("'")) {
        			sValue = "'" + sValue + "'";
        		}
        		e2 = new StringValue(sValue); 
        		bCoerceOK = true; 
        	} catch (Exception ex) {}
        }
        if (bCoerceOK) {
        	e = e2;
        	// block copied from initial block in this method
        	//-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
            if ( e instanceof StringValue )
                return ((StringValue)e).getValue();
            else if ( e instanceof DoubleValue )
                return ((DoubleValue)e).getValue();
            else if ( e instanceof LongValue )
                return ((LongValue)e).getValue();
            else if ( e instanceof NullValue )
                return null;
            else if ( e instanceof JdbcParameter )
                return _params.get( _pos++ );
        	//-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
        }
        
        throw new UnsupportedOperationException( "can't turn [" + e + "] " + e.getClass().getName() + " into constant " );
    }


    DBObject parseWhere( Expression e ){
        DBObject o = new BasicDBObject();
        if ( e == null )
            return o;
        
        if ( e instanceof EqualsTo ){
            EqualsTo eq = (EqualsTo)e;
//nima      o.put( toFieldName( eq.getLeftExpression() ) , toConstant( eq.getRightExpression() ) );
            
//nima: Cater for _id fields containing an object id            
            String sLeftField = toFieldName(eq.getLeftExpression());
            Expression expRight = eq.getRightExpression();
            if (sLeftField.equals("_id") && expRight instanceof StringValue && expRight.toString().length() == 1+24+1) {
            	String sObjectID = toConstant(expRight).toString();
            	o.put( sLeftField, new ObjectId(sObjectID) );
// nima: All other fields            	
            } else {
            	o.put( sLeftField, toConstant( eq.getRightExpression() ) );
            }
            
        }
        // nima: new comparitors
        else if (e instanceof GreaterThan
          	  || e instanceof GreaterThanEquals
        	  || e instanceof MinorThan
        	  || e instanceof MinorThanEquals
        	  || e instanceof NotEqualsTo
        	  ) {
        	BinaryExpression op = (BinaryExpression) e;
        	String ex = "$eq";
        	if (e instanceof GreaterThan) 		ex = "$gt";
        	if (e instanceof GreaterThanEquals) ex = "$gte";
        	if (e instanceof MinorThan) 		ex = "$lt";
        	if (e instanceof MinorThanEquals) 	ex = "$lte";
        	if (e instanceof NotEqualsTo) 		ex = "$ne";
        	//Cater for _id fields containing an object id            
        	String sLeftField = toFieldName(op.getLeftExpression());
        	Expression expRight = op.getRightExpression();
        	if (sLeftField.equals("_id") && expRight instanceof StringValue && expRight.toString().length() == 1+24+1) {
        		String sObjectID = toConstant(expRight).toString();
        		o.put( sLeftField, new ObjectId(sObjectID) );
        	// nima: All other fields            	
        	} else {
        		o.put( sLeftField, new BasicDBObject(ex, toConstant( op.getRightExpression() )));
        	}        	
        }
/*        
        // nima: new comparitors
        else if (e instanceof AndExpression) {
        	AndExpression ex = (AndExpression)e;
        	BasicDBList oList = new BasicDBList();
        	if (ex.getLeftExpression() instanceof AndExpression) {
            	oList.add( parseWhere(ex.getLeftExpression()));
        	} else {
        		oList.add(parseWhere(ex.getLeftExpression()));
        	}
        	if (ex.getRightExpression() instanceof AndExpression) {
            	oList.add( parseWhere(ex.getRightExpression()));
        	} else {
        		oList.add(parseWhere(ex.getRightExpression()));
        	}
        	
        	o = new BasicDBObject("$and", oList);
    	}
        // nima: new comparitors
        else if (e instanceof OrExpression) {
        	OrExpression ex = (OrExpression)e;
        	BasicDBList oList = new BasicDBList();
        	if (ex.getLeftExpression() instanceof OrExpression) {
            	oList.add( parseWhere(ex.getLeftExpression()));
        	} else {
        		oList.add(parseWhere(ex.getLeftExpression()));
        	}
        	if (ex.getRightExpression() instanceof OrExpression) {
            	oList.add( parseWhere(ex.getRightExpression()));
        	} else {
        		oList.add(parseWhere(ex.getRightExpression()));
        	}
        	
        	o = new BasicDBObject("$or", oList);
    	}
*/    	
        // nima: new comparitors
        else if (e instanceof AndExpression) {
        	AndExpression ex = (AndExpression)e;
        	BasicDBList oList = new BasicDBList();
        	oList.add(parseWhere(ex.getLeftExpression()));
        	oList.add(parseWhere(ex.getRightExpression()));
        	o = new BasicDBObject("$and", oList);
    	}
        // nima: new comparitors
        else if (e instanceof OrExpression) {
        	OrExpression ex = (OrExpression)e;
        	BasicDBList oList = new BasicDBList();
        	oList.add(parseWhere(ex.getLeftExpression()));
        	oList.add(parseWhere(ex.getRightExpression()));
        	o = new BasicDBObject("$or", oList);
    	}
        // nima: new comparitors
        else if (e instanceof InExpression) {
        	InExpression ex = (InExpression)e;
        	ExpressionList expList = (ExpressionList)ex.getItemsList();
            String sLeftField = toFieldName(ex.getLeftExpression());
        	List inList = new ArrayList();
        	for (Object objExp : expList.getExpressions()) {
        		inList.add(toConstant((Expression) objExp));
        	}
        	
        	if (!ex.isNot()) {
        		o.put(sLeftField, new BasicDBObject("$in", inList));
        	} else {
        		o.put(sLeftField, new BasicDBObject("$nin", inList));        		
        	}
        }
        // nima: new comparitors
        else if (e instanceof LikeExpression) {
        	LikeExpression ex = (LikeExpression)e;
        	String sLeftField = toFieldName(ex.getLeftExpression());
        	Expression expRight = ex.getRightExpression();
        	String sRight = expRight.toString().substring(1);
        	sRight = sRight.substring(0, sRight.length()-1);
        	boolean bUseRegEx = false;
        	if (sRight.length() > 2 && sRight.startsWith("%") && sRight.endsWith("%")) {
        		sRight = sRight.replace("%", ".*");
        		bUseRegEx = true;
        	} else if (sRight.length() > 1) {
	        	if (sRight.startsWith("%")) {
	        		sRight = sRight.substring(1) + "$";
	        		bUseRegEx = true;
	        	} else if (sRight.endsWith("%")) {
	        		sRight = "^" + sRight.substring(0, sRight.length()-1);
	        		bUseRegEx = true;
	        	}
        	}
        	if (sRight.contains("_")) {
        		sRight = sRight.replace("_", ".");
        		bUseRegEx = true;
        	}
        	if (bUseRegEx) {
	        	Pattern pat = Pattern.compile(sRight);
	        	if (!ex.isNot()) {
		        	o.put(sLeftField, pat);
	        	} else {
	        		o.put(sLeftField, new BasicDBObject("$not", pat));        		
	        	}
	        	
        	}
        	
        }
        
        // nima: new comparitors
        else if (e instanceof IsNullExpression) {
        	IsNullExpression ex = (IsNullExpression) e;
        	String sLeftField = toFieldName(ex.getLeftExpression());
        	if (!ex.isNot()) {
        		o.put(sLeftField, null);
        	} else {
        		o.put(sLeftField, new BasicDBObject("$ne", null));
        	}
        }
        
        // nima: new comparitors
        else if (e instanceof Parenthesis) {
        	Parenthesis ex = (Parenthesis) e;
/*        	
        	BasicDBList oList = new BasicDBList();
        	oList.add("(");
        	oList.add(parseWhere(ex.getExpression()));
        	oList.add(")");
        	o = oList;
*/
        	if (!ex.isNot()) {
        		o = parseWhere(ex.getExpression());
        	} else {
        		List list = new ArrayList();
        		list.add(parseWhere(ex.getExpression()));
        		o = new BasicDBObject("$nor",list);
        	}
        }
        
        else {
            throw new UnsupportedOperationException( "can't handle: " + e.getClass() + " yet" );
        }

        return o;
    }

    Statement parse( String s )
        throws MongoSQLException {
        s = s.trim();
        
        try {
            return (new CCJSqlParserManager()).parse( new StringReader( s ) );
        }
        catch ( Exception e ){
            e.printStackTrace();
            throw new MongoSQLException.BadSQL( s );
        }
        
    }

    // ----
    
    DBCollection getCollection( Table t ){
        return _db.getCollection( t.toString() );
    }
    
    public static Date parseDate(String s) {
    	Date date = null;
    	try {
			date = sdfYYYYMMDDHHMMSS.parse(s);
    	} catch (Exception e1) {
    		try {
        		date = sdfYYYYMMDDHHMM.parse(s);
    		} catch (Exception e2) {
    			try {
    	    		date = sdfYYYYMMDD.parse(s);
    			} catch (Exception e3) {
    			}
    		}
    	}
    	return date;
    }
    
    public static String serializeToJSON(Object o) {
    	String sToken = "{ \"$oid\" : ";
    	String s = JSON.serialize(o);
    	int idx = -1;
    	int idxEndValue = -1;
    	while ((idx = s.indexOf(sToken, idx+1)) > -1) {
    		idxEndValue = s.indexOf("\"}}", idx);
    		s = s.substring(0, idx) + "ObjectId(" +  
    			s.substring(idx + sToken.length(), idxEndValue + 1) + 
    			")" + s.substring(idxEndValue+2);
    	}
    	return s;
    }
}
