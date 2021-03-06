// MongoConnection.java

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

import java.sql.*;
import java.util.*;
import java.util.concurrent.Executor;

import com.mongodb.*;

public class MongoConnection implements Connection {
    
	MongoClient _client;
	DB _db;
	MongoClientURI _uri;
    Properties _clientInfo;
    /** Connection user properties key to specify a value that indicates that a column is not present */
    public static final String NoColumnValue_Key = "NoColumnValue";
    String _NoColumnValue; // The string value to indicate that the column is not present

    public MongoConnection( MongoClient client, DB db, MongoClientURI uri) {
    	_client = client;
    	_db = db;
    	_uri = uri;
    }

    public SQLWarning getWarnings(){
        //throw new RuntimeException( "should do get last error" );
    	return null;
    }

    public void clearWarnings(){
        throw new RuntimeException( "should reset error" );
    }

    // ---- state -----
    
    public void close(){
    	if (_client != null) {
    		_client.close();
    		_client = null;
    	}
    	_client = null;
    }

    public boolean isClosed(){
        return _client == null;
    }

    // --- commit ----

    public void commit(){
        // NO-OP
    }

    public boolean getAutoCommit(){
        return true;
    }

    public void rollback(){
        throw new RuntimeException( "can't rollback" );
    }
    
    public void rollback(Savepoint savepoint){
        throw new RuntimeException( "can't rollback" );
    }

    public void setAutoCommit(boolean autoCommit){
        if ( ! autoCommit )
            throw new RuntimeException( "autoCommit has to be on" );
    }
    
    public void releaseSavepoint(Savepoint savepoint){
        throw new RuntimeException( "no savepoints" );
    }
    
    public Savepoint setSavepoint(){
        throw new RuntimeException( "no savepoints" );
    }

    public Savepoint setSavepoint(String name){
        throw new RuntimeException( "no savepoints" );
    }

    public void setTransactionIsolation(int level){
        throw new RuntimeException( "no TransactionIsolation" );
    }

    // --- create ----

    public Array createArrayOf(String typeName, Object[] elements){
        throw new RuntimeException( "no create*" );
    }
    public Struct createStruct(String typeName, Object[] attributes){
        throw new RuntimeException( "no create*" );
    }
    public Blob createBlob(){
        throw new RuntimeException( "no create*" );
    }
    public Clob createClob(){
        throw new RuntimeException( "no create*" );
    }
    public NClob createNClob(){
        throw new RuntimeException( "no create*" );
    }
    public SQLXML createSQLXML(){
        throw new RuntimeException( "no create*" );
    }
    
    // ------- meta data ----

    public String getCatalog(){
        return null;
    }
    public void setCatalog(String catalog){
    	_db = _client.getDB(catalog);
    }
    
    public Properties getClientInfo(){
        return _clientInfo;
    }
    public String getClientInfo(String name){
        return (String)_clientInfo.get( name );
    }

    public void setClientInfo(String name, String value){
        _clientInfo.put( name , value );
        if (NoColumnValue_Key.equals(name)) {
        	_NoColumnValue = value;
        }
    }
    public void setClientInfo(Properties properties){
        _clientInfo = properties;
        if (properties.containsKey(NoColumnValue_Key)) {
        	_NoColumnValue = properties.getProperty(NoColumnValue_Key);
        }
    }


    public int getHoldability(){
        return ResultSet.HOLD_CURSORS_OVER_COMMIT;
    }
    public void setHoldability(int holdability){
    }

    public int getTransactionIsolation(){
        throw new RuntimeException( "does not do it yet" );
    }
    
    public DatabaseMetaData getMetaData(){
    	DatabaseMetaData dmd = new MongoDatabaseMetaData(_db, _uri);
			
        return dmd;
    }

    public boolean isValid(int timeout){
        return _client != null;
    }

    public boolean isReadOnly(){
        return false;
    }
    
    public void setReadOnly(boolean readOnly){
        if ( readOnly )
            throw new RuntimeException( "no read only mode" );
    }


    public Map<String,Class<?>> getTypeMap(){
        throw new RuntimeException( "not done yet" );
    }
    public void setTypeMap(Map<String,Class<?>> map){
        throw new RuntimeException( "not done yet" );
    }
    
    // ---- Statement -----
    
    public Statement createStatement(){
        return createStatement( 0 , 0 , 0 );
    }
    public Statement createStatement(int resultSetType, int resultSetConcurrency){
        return createStatement( resultSetType , resultSetConcurrency, 0 );
    }
    public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability){
        return new MongoStatement( this , resultSetType , resultSetConcurrency , resultSetHoldability );
    }
    
    // --- CallableStatement


    public CallableStatement prepareCall(String sql){
        return prepareCall( sql , 0 , 0 , 0 );
    }
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency){
        return prepareCall( sql , resultSetType , resultSetConcurrency , 0 );
    }
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability){
        throw new RuntimeException( "CallableStatement not supported" );
    }

    // ---- PreparedStatement 
    public PreparedStatement prepareStatement(String sql)
        throws SQLException {
        return prepareStatement( sql , 0 , 0 , 0 );
    }
    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys){
        throw new RuntimeException( "no PreparedStatement yet" );
    }
    public PreparedStatement prepareStatement(String sql, int[] columnIndexes){
        throw new RuntimeException( "no PreparedStatement yet" );
    }
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency)
        throws SQLException {
        return prepareStatement( sql , resultSetType , resultSetConcurrency , 0 );
    }
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability)
        throws SQLException {
        return new MongoPreparedStatement( this , resultSetType , resultSetConcurrency , resultSetHoldability , sql );
    }
    public PreparedStatement prepareStatement(String sql, String[] columnNames){
        throw new RuntimeException( "no PreparedStatement yet" );
    }


    // ---- random ----

    public String nativeSQL(String sql){
        return sql;
    }

    public <T> T unwrap(Class<T> iface)
        throws SQLException {
        throw new UnsupportedOperationException();
    }

    public boolean isWrapperFor(Class<?> iface)
        throws SQLException {
        throw new UnsupportedOperationException();
    }

    public DB getDatabase(){
        return _db;
    }

    public DBCollection getCollection( String name ){
    	return _client.getDB(_db.getName()).getCollection(name);
        //return _db.getCollection( name );
    }

    /** nima: disposes of the connection */
    public void finalize() {
    	if (!isClosed()) {
    		close();
    	}
    }
    
    //@Override
	public void abort(Executor arg0) throws SQLException {
		// TODO Auto-generated method stub
		
	}

	//@Override
	public int getNetworkTimeout() throws SQLException {
		// TODO Auto-generated method stub
		return 0;
	}

	//@Override
	public String getSchema() throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	//@Override
	public void setNetworkTimeout(Executor arg0, int arg1) throws SQLException {
		// TODO Auto-generated method stub
		
	}

	//@Override
	public void setSchema(String arg0) throws SQLException {
		// TODO Auto-generated method stub
		
	}
}
