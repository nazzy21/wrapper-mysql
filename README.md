# @wrapper/mysql

A simple MVC wrapper for MySQL database.

### Methods
##### name
 The name of the databae type.

##### assert(Object config)
 Validates the configuration options.

#### Collection
 A class object use to execute database transactions.

### Collection Class

###### create(Object options)
 Creates new table collection in the database if it does not exist.

###### alter(Object oldSchema, Object newSchema, Object options)
 Change the table collection's structure.

###### drop
 Remove table collection from the database.

###### insert(Object columns)

###### insertMany(Array columns)

###### update(Object columns, Object conditions)

###### delete(Object conditions)

###### find(Object conditions)

###### findOne(String columns, Object whereClause)

###### getValue(String column, Object whereClause)