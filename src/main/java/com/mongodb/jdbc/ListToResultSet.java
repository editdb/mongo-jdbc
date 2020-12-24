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
import java.util.List;

public class ListToResultSet extends NoImplResultSet {

	List<String> _columns;
	List<List<Object>> _rows;
	int row1 = 0;
	public ListToResultSet (List<String> columns, List<List<Object>> rows) {
		_columns = columns;
		_rows = rows;
	}

	public ListToResultSet (String column, List<Object> rows) {
		_columns = Arrays.asList(new String[]{column});
		_rows = new ArrayList<List<Object>>();
		for (Object obj : rows) {
			_rows.add(Arrays.asList(new Object[]{obj}));
		}
	}
	
	public boolean next() {
		if (row1 < _rows.size()) {
			row1++;
			return true;
		} else {
			return false;
		}
	}
	
	public String getString(int iCol) {
		return _rows.get(row1-1).get(iCol-1).toString();
	}
	
	public String getString(String sCol) throws SQLException {
		int iCol = 0;
		for (String col : _columns) {
			if (col.equals(sCol)) {
				List<Object> row = _rows.get(row1-1);
				return row.get(iCol).toString(); 
			}
			iCol++;
		}
		
		throw new SQLException("No column found with name \"" + sCol + "\"");
	}
}
