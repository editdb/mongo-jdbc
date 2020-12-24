### Mongo JDBC Driver editdb version (May 2013)

Home: http://github.com/editdb/mongo-jdbc

License: Apache 2

In addition to =, this version makes available the following WHERE clause operations:
!=, <>, >, >=, <, <=, IN, LIKE, (, ), AND, OR, NOT, IS NULL, IS NOT NULL

In addition to SELECT, the following statements are now available:
`INSERT`, `UPDATE`, `DELETE`.
The following javascript-syntax commands are allowed:
`insertOne`, `insertMany`, `updateOne`, `updateMany`, `deleteOne`, `deleteMany`, `drop`.

There is now also some DatabaseMetaData and ResultSetMetaData available. 
Please bear in mind for ResultSetMetaData that the same column in different 
records/documents can be of a differing type.

Dates and datetimes should be input in formats
- 'yyyy-MM-dd'
- 'yyyy-MM-dd hh:mm'
- 'yyyy-MM-dd hh:mm:ss'

MongoStatment.getLastOperation() returns the MongoDB verbage equivalent to the 
last successful SQL operation which can be useful when learning.

When a "column" value is null, null is returned.

When a "column" does not exist, but is requested (perhaps because other 
records/documents contain it making it expected) then a customisable is returned.
This value can be set in the connection properties collection using the key defined 
in MongoConnection.NoColumnValue_Key.  If an update is requested on a column which sets
it to this value, then the column is removed from the document.

### Building
Using [Maven](https://maven.apache.org/) issue the command
```
mvn install
```
The generated jar files will be found in the `target` folder.
Use the jar file called `MongoJDBC-<version>-fat.jar` unless you want to manually include all the dependency jars.

### Connecting to a MongoDB database

The connection string takes the format
```
mongodb://<user>:<password>@<server>:<port>/<database>?<options>
```
e.g. in the following java code
```
Class.forName("com.mongodb.jdbc.MongoDriver");
String connStr = "mongodb://tennis_user:tennis_pass@192.168.99.100:27017/tennis_players?w=1&wtimeoutMS=30000";

// Display this string when the "column" does not exist in a "record"
java.util.Properties connProps.setProperty("NoColumnValue", "º"); 

Connection con = DriverManager.getConnection(connStr, connProps);
```
e.g. the connection string





------------- original erh version -------------
__EXPERIMENTAL__

This is an experimental JDBC driver for MongoDB.  It attempts to map some basic SQL to MongoDB syntax.  
One of the interesting things is that if you use prepared statements, you can actually use embedded objects, etc... quite nicely. 
See examples/ for more info, ideas.

Home: http://github.com/erh/mongo-jdbc/

License: Apache 2

### Supported
 - SELECT
   - field selector
   - order by
 - INSERT
 - UPDATE
   - basics
 - DROP

### TODO
 - create index
 - insert & getLastError
 - embedded objects  (foo.bar)
 - prepared statements
 - (s|g)etObject( 0 )
