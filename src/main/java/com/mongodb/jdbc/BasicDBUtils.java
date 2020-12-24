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

import java.util.ArrayList;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;

public class BasicDBUtils {
	public static String toHtml(Object obj, String tableClass, String headerClass, String oddRowClass, String evenRowClass, String noColumnValue) {
		String s = "";
		RenderEngine renderEngine = new RenderEngine(tableClass, headerClass, oddRowClass, evenRowClass, noColumnValue);
		if (obj instanceof BasicDBList) {
			ArrayList<String> arlKeys = new ArrayList<String>();
			s += renderEngine.tableHeaderHtml((BasicDBList)obj, arlKeys);
			s += renderEngine.listToHtml((BasicDBList)obj, arlKeys);
			s += renderEngine.tableFooterHtml();
		} else 
		if (obj instanceof BasicDBObject) {
			s += renderEngine.objToHtml((BasicDBObject)obj, null, true);
		}
		return s;
	}
	
	static class RenderEngine {
		String tableClass;
		String headerClass;
		String oddRowClass;
		String evenRowClass;
		String _noColumnValue = null;
		
		public RenderEngine(String tableClass, String headerClass, String oddRowClass, String evenRowClass, String noColumnValue) {
			this.headerClass = headerClass;
			this.oddRowClass = oddRowClass;
			this.evenRowClass = evenRowClass;
			this._noColumnValue = noColumnValue;
		}
		
		private String listToHtml(BasicDBList bdbl, ArrayList<String> arlKeys) {
			String s = "";
			boolean bOddRow = false;
			for (Object obj : bdbl) {
				bOddRow = !bOddRow;
				s += objToHtml(obj, arlKeys, bOddRow);
			}
			return s;
		}
		
		private String objToHtml(Object bObj, ArrayList<String> arlKeys, boolean bOddRow) {
			BasicDBObject bdbo = null;
			
			if (bObj instanceof BasicDBObject) {
				bdbo = (BasicDBObject)bObj;
			}
			String s = "";
			boolean bArlKeysWasNull = false;
			if (bObj instanceof BasicDBObject && arlKeys == null) {
				arlKeys = new ArrayList<String>();
				for (String sKey : bdbo.keySet()) {
					arlKeys.add(sKey);
				}
				s += tableHeaderHtml(null, arlKeys);				
				bArlKeysWasNull = true;
			}
			
			s += "<tr class=\"" + (bOddRow?oddRowClass:evenRowClass) + "\"> \n";
			if (bObj instanceof BasicDBObject) {
				for (String sKey : arlKeys) {
					s += "<td>";
					if (bdbo.keySet().contains(sKey)) {
						Object obj = bdbo.get(sKey);
						if (obj instanceof BasicDBList) {
							ArrayList<String> arlKeys2 = new ArrayList<String>();
							s += tableHeaderHtml((BasicDBList)obj, arlKeys2);
							s += listToHtml((BasicDBList)obj, arlKeys2);
							s += tableFooterHtml();
						} else
						if (obj instanceof BasicDBObject) {
							s += objToHtml((BasicDBObject)obj, null, true);
						} else {
							s += obj; 
						}
					} else {
	//					s += MongoResultSet.HTML_NO_ENTRY;
						s += _noColumnValue;
					}
					s += "</td>";
				}
			} else {
				s += "<td>" + bObj + "</td>";			
			}
			s += "</tr> \n";
			
			if (bArlKeysWasNull) {
				s += tableFooterHtml();
			}
			
			return s;
		}
		
		private String tableHeaderHtml(BasicDBList bdbl, ArrayList<String> arlKeys) {
			if (bdbl != null) {
				if (arlKeys == null) {
					arlKeys = new ArrayList<String>();
					for (Object obj : bdbl) {
						if (obj instanceof BasicDBObject) {
							BasicDBObject bdbo = (BasicDBObject)obj;
							for (String sKey : bdbo.keySet()) {
								if (arlKeys.indexOf(sKey) == -1) {
									arlKeys.add(sKey);
								}
							}
						}
					}
				} else {
					arlKeys.clear();
				}
				for (Object obj : bdbl) {
					if (obj instanceof BasicDBObject) {
						BasicDBObject bdbo = (BasicDBObject)obj;
						for (String sKey : bdbo.keySet()) {
							if (arlKeys.indexOf(sKey) == -1) {
								arlKeys.add(sKey);
							}
						}
					}
				}
			}
	
			String s = "";
			s += "<table width=\"100%\" cellpadding=\"0\" cellspacing=\"1\" class=\"" + tableClass + "\"> \n";
			s += "<tr>";
			for (String sKey : arlKeys) {
				s += "<th>" + sKey + "</th>"; 
			}
			s += "</tr> \n";
			return s;
		}
		
		private String tableFooterHtml() {
			return "</table> \n";
		}
	
	}
}
