Kite Connector
==============

The Kite connector aloows querying data stored in Kite.  The schema and table created in Presto is in memory only and will be removed after system restart.
Data in kite will keep the same.

Compilation
==============

1. Compile kite-client-sdk and install to Maven

```
% git clone git@github.com:vderic/kite-client-sdk.git
cd kite-client-sdk/java
mvn clean install
```

2. Compile kite connector

```
% cd presto-kite
% ./mvnw clean package -DskipTests
```

3. Unzip kite connector zip file `presto-kite/target/kite-VERSION.zip` and copy all jar files to `$PRESTO_HOME/plugin/kite` directory

Configuration
==============

To configure the kite connector, create a catalog properties file etc/catalog/kite.properties with the following contents:

```
connector.name=kite
kite.splits-per-node=3
```

Multiple Kite Clusters
==============

You can have as many catalogs as you need, so if you have additional Kite clusters, simply add another propoerties file to etc/catalog with a different name (making sure it ends in .properties).
For example, if you name the property file dev.properties, Presto will create a catalog named dev using configured connector.

Configuration Properties
==============

The following configuration properties is available:

| Property Name | Description |
|---------------|--------------|
| kite.splits-per-node | The number of split per node |

Query Kite Tables
==============

## Table Options

| Property Name | Description |
|---------------|-------------|
| format        | csv or parquet |
| location      | kite URL with format kite://host:port/path |
| csv_header    | true or false |
| csv_quote     | csv quote character |
| csv_escape    | csv escape character |
| csv_separator | csv separator character |
| csv_nullstr   | csv NULL string |


```
presto> CREATE TABLE kite.default.lineitem ( L_ORDERKEY    BIGINT,
                             L_PARTKEY     BIGINT,
                             L_SUPPKEY     BIGINT,
                             L_LINENUMBER  BIGINT,
                             L_QUANTITY    DOUBLE,
                             L_EXTENDEDPRICE  DOUBLE,
                             L_DISCOUNT    DOUBLE,
                             L_TAX         DOUBLE,
                             L_RETURNFLAG  VARCHAR(1),
                             L_LINESTATUS  VARCHAR(1),
                             L_SHIPDATE    DATE,
                             L_COMMITDATE  DATE,
                             L_RECEIPTDATE DATE,
                             L_SHIPINSTRUCT VARCHAR(25),
                             L_SHIPMODE     VARCHAR(10),
                             L_COMMENT      VARCHAR(44))
WITH (format='csv', location='kite://localhost:7878/test_tpch/csv/lineitem*');

```
