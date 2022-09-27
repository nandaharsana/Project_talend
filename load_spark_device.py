
from pyspark.sql import SparkSession

appName = "pyspark_test"
master = "local"
# Create Spark session
spark = SparkSession.builder \
    .appName(appName) \
    .master(master) \
    .config("spark.driver.extraClassPath", "mariadb-java-client-3.0.7.jar:tigergraph-jdbc-driver-1.3.5.jar") \
    .getOrCreate()

# Create a data frame by reading data from mariadb via JDBC
def read_data(jdbc_url,sql,user,password,driver):
    return spark.read.format("jdbc") \
        .option("url", jdbc_url) \
        .option("query", sql) \
        .option("user", user) \
        .option("password", password) \
        .option("driver", jdbc_driver) \
        .load()

sql = "select id,deviceid,time from data_raya.device"
database = "data_raya"
user = "admin"
password = "password"
server = "172.16.11.224"
port = 3306
jdbc_url = f"jdbc:mysql://{server}:{port}/{database}?permitMysqlScheme"
jdbc_driver = "org.mariadb.jdbc.Driver"

df_trx = read_data(jdbc_url, sql, user, password, jdbc_driver)
df_trx.show(10)


#load df into tigergraph
def load_data_tg(dataframe,driver,url,user,password,graph,dbtable,filename,schema):
    return dataframe.write \
        .mode("overwrite") \
        .format("jdbc") \
        .option("driver", driver) \
        .option("url", url) \
        .option("user", user) \
        .option("password", password) \
        .option("graph", graph) \
        .option("dbtable", dbtable) \
        .option("filename", filename) \
        .option("sep", ",") \
        .option("eol", ";") \
        .option("schema", schema) \
        .option("batchsize", "100") \
        .option("debug", "1") \
        .save()

tg_driver = "com.tigergraph.jdbc.Driver"
tg_url = "jdbc:tg:http://172.16.11.250:14240"
tg_user = "tigergraph"
tg_password = "Password1234"
graph = "graph_pyspark"
dbtable = "job load_spark"
filename = "Device"
schema = "id,deviceid,time"

tg_load=load_data_tg(df_trx,tg_driver, tg_url, tg_user, tg_password, graph, dbtable, filename, schema)

