generatetablefetch.sql.error	If the processor has incoming connections, and processing an incoming FlowFile causes a SQL Exception, the FlowFile is routed to failure and this attribute is set to the exception message.
generatetablefetch.tableName	The name of the database table to be queried.
generatetablefetch.columnNames	The comma-separated list of column names used in the query.
generatetablefetch.whereClause	Where clause used in the query to get the expected rows.
generatetablefetch.maxColumnNames	The comma-separated list of column names used to keep track of data that has been returned since the processor started running.
generatetablefetch.limit	The number of result rows to be fetched by the SQL statement.
generatetablefetch.offset	Offset to be used to retrieve the corresponding partition.
fragment.identifier	All FlowFiles generated from the same query result set will have the same value for the fragment.identifier attribute. This can then be used to correlate the results.
fragment.count	This is the total number of FlowFiles produced by a single ResultSet. This can be used in conjunction with the fragment.identifier attribute in order to know how many FlowFiles belonged to the same incoming ResultSet.
fragment.index	This is the position of this FlowFile in the list of outgoing FlowFiles that were all generated from the same execution. This can be used in conjunction with the fragment.identifier attribute to know which FlowFiles originated from the same execution and in what order FlowFiles were produced