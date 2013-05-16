/**
 *      Copyright (C) 2013 Nigel Maddocks
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
*/

package com.mongodb.jdbc;

import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Set;

import com.mongodb.*;

public class MongoDatabaseMetaData extends NoImplDatabaseMetaData {
	
	DB _db;
	
	public MongoDatabaseMetaData(DB db) {
		_db = db;
	}
	
	public String getURL() throws SQLException {
		ServerAddress srvaddr = _db.getMongo().getAddress();
		String url = srvaddr.getHost() + ":" + srvaddr.getPort();
		
		return url;
	}
	
	public ResultSet getTables(String catalog, String schemaPattern,
			String tableNamePattern, String[] types) throws SQLException {
		_db = _db.getMongo().getDB(catalog);
		Set<String> collNames = _db.getCollectionNames();
		List<String> colNames = Arrays.asList(new String[]{"TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "TABLE_TYPE"});
		List<List<Object>> lstCollections = new ArrayList<List<Object>>();
		for (String collName : collNames) {
			List row = Arrays.asList(new String[]{catalog, null, collName, "TABLE"});
			lstCollections.add(row);
		}
		ResultSet rs = new ListToResultSet(colNames, lstCollections);
		return rs;
	}
	
	public String getDatabaseProductVersion() throws SQLException {
		return Mongo.getMajorVersion() + "." + Mongo.getMinorVersion();
	}
	
	public String getDatabaseProductName() throws SQLException {
		return "MongoDB";
	}
	
	public ResultSet getCatalogs() throws SQLException {
		String[] arrDBNames = (String[])_db.getMongo().getDatabaseNames().toArray(new String[]{});
		CaseInsensitiveComparator cmp = new CaseInsensitiveComparator();
		Arrays.sort(arrDBNames, cmp);
		List dbNames = Arrays.asList(arrDBNames);
		
		ResultSet rs = new ListToResultSet("TABLE_CAT", dbNames);
		return rs;
	}
	
	public String getCatalogTerm() throws SQLException {
		return "Database";
	}
	
	private class CaseInsensitiveComparator implements Comparator<String> {
		public int compare(String strA, String strB) {
		    return strA.compareToIgnoreCase(strB);
		}
	}
}
