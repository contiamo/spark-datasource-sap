# A collection of useful materials

### Links on useful SAP tables

- Function to read tables https://www.se80.co.uk/sapfms/r/rfc_/rfc_read_table.htm
    - filter pushdown https://stackoverflow.com/questions/27633332/rfc-read-table-passing-options-and-fields-parametrs-c
	- alt read rfc setup is described here as the 3rd option https://rfcconnector.com/documentation/kb/0007/
    - possible alternatives `TABLE_ENTRIES_GET_VIA_RFC` or `RFC_GET_TABLE_ENTRIES` or `BBP_RFC_READ_TABLE`
    - https://success.jitterbit.com/display/DOC/Guide+to+Using+RFC_READ_TABLE+to+Query+SAP+Tables
- tables with info about tables
	- https://www.se80.co.uk/saptables/d/dd02/dd02l.htm — list
	- https://www.se80.co.uk/saptables/d/dd02/dd02t.htm — descriptions
	- https://www.se80.co.uk/saptables/d/dd03/dd03l.htm — list of columns
	- https://www.se80.co.uk/saptables/d/dd03/dd03t.htm — column descriptions
	- https://www.erpgreat.com/abap/how-to-get-the-field-descriptions-of-a-table.htm	
	- https://www.system-overload.org/sap/tables.html
- a table with info about funcitons https://www.se80.co.uk/saptables/t/tfdi/tfdir.htm 
	- Functinoa with `FMODE = 'R'` are "remote"
- queries
	- https://www.se80.co.uk/saptables/a/aqlq/aqlqcat.htm 
	- https://www.se80.co.uk/saptables/a/aqgq/aqgqcat.htm seem to be queries related
	- RSAQ_REMOTE_QUERY_CALL_CATALOG — used for retrieving Queries
- other useful links
	- https://github.com/SAP/gorfc/tree/master/doc
	- http://www.datras.de/inside_dynabap_wp.htm
	
Javadoc for the SAP JCO library could be found at https://www.int13h.nl:8443/_content/SAPDocuments/JCo_API_3_0/index.html?com/sap/conn/jco/ext/package-summary.html


### Links on Spark Datasource API:

- https://github.com/assafmendelson/DataSourceV2/blob/master/src/main/scala/com/example/sources/readers/trivial/README.md
- http://shzhangji.com/blog/2018/12/08/spark-datasource-api-v2/
