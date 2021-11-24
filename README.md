# SAP data source for Spark

## Warning

This repository is not maintained at the moment and CI action is disabled.
Active maintenance and CI make sense only when there's access to an SAP 
system (there isn't right now).

## Supported features

This was a POC project, here is what it can do:

- Retrieve schemas for a list of tables
- Read a table using `RFC_READ_TABLE` (within its limitations)
  - Allows configuring an alternative read table function
  - Some filter expressions from `where` clause could be pused down
- Call a BAPI and return selected export parameters or tables

For usage examples see `src/test/scala/SapSparkDatasourceIntegrationSpec.scala`. 

## Known issues

- [Values, truncated by the read table function](https://stackoverflow.com/questions/58728382/reading-tcurr-table-with-rfc-read-table-truncates-the-rate-value), are not handled at the moment.

## Getting the SAP JCo driver

- Get the driver from your SAP vendor or download it from [SAP](https://support.sap.com/en/product/connectors/jco.html) 
- Put the driver's jar, so and jnilib into the `lib` folder
- Rename the files as explained [here](https://github.com/saro-lab/sap-jco-manager/blob/master/README.md#download-and-include-sapjco3)

## Running tests

- Get the SAP JCo JNI libraries (see above)
- Set SAP credentials via the environment variables:  
`export SAP_AHOST="/H/saprouter.example.com/S/0000/H/abcd/S/1111" SAP_USER=MYUSER SAP_PASSWORD=VERYSECRET`
- Run `sbt test`
- Some tests relied on the contents of the test SAP system we had access to.

