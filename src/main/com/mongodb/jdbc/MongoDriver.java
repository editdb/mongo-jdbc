// MongoDriver.java

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
import java.util.logging.Logger;

import com.mongodb.*;

public class MongoDriver implements Driver {

    static final String PREFIX = "mongodb://";

    public MongoDriver(){
    }

    public boolean acceptsURL(String url){
        return url.startsWith( PREFIX );
    }
    
    public Connection connect(String url, Properties info)
        throws SQLException {
//nima    	
//      if ( info != null && info.size() > 0 )
//          throw new UnsupportedOperationException( "properties not supported yet" );

        if ( url.startsWith( PREFIX ) )
            url = url.substring( PREFIX.length() );
        if ( url.indexOf( "/" ) < 0 )
            throw new MongoSQLException( "bad url: " + url );            
        
        try {
/*        	
            DBAddress addr = new DBAddress( url );
            MongoConnection conn = new MongoConnection( Mongo.connect( addr ) );
*/
        	//  nima: altered connection mechanism as it didn't work with user:pwd in the connection string
        	MongoURI uri = new MongoURI("mongodb://" + url);
        	DB db = uri.connectDB();
        	MongoConnection conn = new MongoConnection(db);
        	conn.setClientInfo(info);
            //  nima: try an operation to check that the connection is good.
            conn._db.getMongo().getDatabaseNames();
            return conn;
        }
        catch ( java.net.UnknownHostException uh ){
            throw new MongoSQLException( "bad url: (" + url + ")" + uh );
        } catch ( Exception e ){
        	throw new MongoSQLException( "Connection problem with url: (" + url + "): " + e );
        }
    }
    
    public int getMajorVersion(){
        return 0;
    }
    public int getMinorVersion(){
        return 1;
    }

    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info){
        throw new UnsupportedOperationException( "getPropertyInfo doesn't work yet" );
    }

    public boolean jdbcCompliant(){
        return false;
    }

    public static void install(){
        // NO-OP, handled in static
    }

    static {
        try {
            DriverManager.registerDriver( new MongoDriver() );
        }
        catch ( SQLException e ){
            throw new RuntimeException( e );
        }
    }

    //@Override
	public Logger getParentLogger() throws SQLFeatureNotSupportedException {
		// TODO Auto-generated method stub
		return null;
	}
}
