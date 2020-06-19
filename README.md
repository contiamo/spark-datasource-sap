# SAP datasource for Spark

For usage examples see `src/test/scala/SapSparkDatasourceIntegrationSpec.scala`. 

Snapshot releases are published automatically upon PR merge. 
In order to publish a full release you need to tag a commit on 
`master` then create a GH release.

### Links on useful SAP tables

- Function to read tables https://www.se80.co.uk/sapfms/r/rfc_/rfc_read_table.htm
    - filter pushdown https://stackoverflow.com/questions/27633332/rfc-read-table-passing-options-and-fields-parametrs-c
    - possible alternatives `TABLE_ENTRIES_GET_VIA_RFC` or `RFC_GET_TABLE_ENTRIES` or `BBP_RFC_READ_TABLE`
- tables with info about tables
	- https://www.se80.co.uk/saptables/d/dd02/dd02l.htm — list
	- https://www.se80.co.uk/saptables/d/dd02/dd02t.htm — descriptions
	- https://www.se80.co.uk/saptables/d/dd03/dd03l.htm — list of columns
	- https://www.se80.co.uk/saptables/d/dd03/dd03t.htm — column descriptions
	- https://www.erpgreat.com/abap/how-to-get-the-field-descriptions-of-a-table.htm	
- a table with info about funcitons https://www.se80.co.uk/saptables/t/tfdi/tfdir.htm 
	- Functinoa with `FMODE = 'R'` are "remote"
- queries
	- https://www.se80.co.uk/saptables/a/aqlq/aqlqcat.htm 
	- https://www.se80.co.uk/saptables/a/aqgq/aqgqcat.htm seem to be queries related
	- RSAQ_REMOTE_QUERY_CALL_CATALOG — used for retrieving Queries


### Links on Spark Datasource API:

- https://github.com/assafmendelson/DataSourceV2/blob/master/src/main/scala/com/example/sources/readers/trivial/README.md
- http://shzhangji.com/blog/2018/12/08/spark-datasource-api-v2/
