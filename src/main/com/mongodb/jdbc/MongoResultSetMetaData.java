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

import java.sql.ResultSetMetaData;
import java.util.ArrayList;
import java.util.HashSet;

import com.mongodb.DBCursor;

public class MongoResultSetMetaData extends NoImplResultSetMetaData {
	MongoResultSet _rs;
	int _colCount;
	ArrayList<String> _colNames = new ArrayList<String>();
	ArrayList<String> _colTypeNames = new ArrayList<String>();
	HashSet<String> _hsColTypeNames = new HashSet<String>();
	public MongoResultSetMetaData(MongoResultSet rs) {
		_rs = rs;
		
		while (_rs.next()) {
			for (String sColName :_rs._cur.keySet()) {
				if (!_colNames.contains(sColName)) {
					_colNames.add(sColName);
					Object obj = _rs.getObject(sColName);
					if (obj != null) {
						_colTypeNames.add(obj.getClass().getSimpleName());
						_hsColTypeNames.add(sColName);
					} else {
						_colTypeNames.add("");
					}
				} else if (!_hsColTypeNames.contains(sColName)) {
					int idxColName = _colNames.indexOf(sColName);
					if (_colTypeNames.get(idxColName).length() == 0) {
						Object obj = _rs.getObject(sColName);
						if (obj != null) {
							_colTypeNames.set(idxColName, obj.getClass().getSimpleName());
							_hsColTypeNames.add(sColName);
						}
					}
				}
			}
			//break; //temp quit to test 1 rec
		}
		_colCount = _colNames.size();
		
		DBCursor cursorToCopyAndClose = _rs._cursor;
		_rs._cursor = _rs._cursor.copy();
		cursorToCopyAndClose.close();
	}
	
	@Override
	public int getColumnCount() {
		return _colCount;
	}
	
	@Override
	public String getColumnName(int iCol) {
		return _colNames.get(iCol-1);
	}
	
	@Override
	public String getColumnTypeName(int iCol) {
		return _colTypeNames.get(iCol-1);
	}
	
	@Override
	public boolean isWritable(int iCol) {
		return !_colNames.get(iCol-1).equals("_id");
	}
	
	@Override
	public int isNullable(int iCol) {
		return _colNames.get(iCol-1).equals("_id") ? ResultSetMetaData.columnNoNulls : ResultSetMetaData.columnNullable;
	}
	

}
