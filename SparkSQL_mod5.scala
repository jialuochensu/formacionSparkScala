//SparkSQL (JSON) modulo 5.1 -> Familiarizarse con el uso de SQL con Spark

import sqlContext.implicits._ //permite convertir RDD en DF

var ssc = new org.apache.spark.sql.SQLContext(sc)

//cargar el json 
var zips = ssc.load("file:/home/BIT/data/zips.json", "json") //contiene los datos de CP de EEUU
//visualizar el contenido 
zips.show()

//Obtén las filas cuyos códigos postales cuya población es superior a 10000 usando el api de DataFrames
zips.filter(zips("pop") > 10000).collect() //.show() para dejarlo en un formato mas facil de leer
zips.registerTempTable("zips") //guardar la tabla en un fichero temporal para poder ejercutar SQL

//usando SQL (equivalente a la linea 13)
ssc.sql("select * from zips where pop > 10000").collect()

//SQL, obtener la ciudad con mas de 100 CP
ssc.sql("select city from zips group by city having count(*)>100").show()

//SQL, obtén la población del estado de Wisconsin (WI)
ssc.sql("select SUM(pop) as num_pop from zips where state='WI'").show()

//SQL, obtén los 5 estados más poblados
ssc.sql("select state, SUM(pop) as num_pop from zips group by state order by num_pop DESC limit 5").show()


//SparkSQL (hive) modulo 5.2 -> Familiarizarse con el uso de SparkSQL para acceder a tablas en hive
val sqlContext = new org.apache.spark.sql.hive.HiveContext(sc)
//hive, creamos la base de datos
sqlContext.sql("CREATE DATABASE IF NOT EXISTS hivespark")
//hive, creamos tabla
sqlContext.sql("CREATE TABLE IF NOT EXISTS hivespark.empleados(id INT, name STRING, age INT) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'")
//hive, importamos un txt con SparkSQL
sqlContext.sql("LOAD DATA LOCAL INPATH '/home/cloudera/empleados.txt' INTO TABLE hivespark.empleados")

var query1 = sqlContext.sql("SELECT * FROM hivespark.empleados")
query1.show()

//SparkSQL (DataFrames) modulo 5.3 -> Familiarizarse con el uso de de la API DataFrames
var ssc = new org.apache.spark.sql.SQLContext(sc)
import sqlContext.implicits._
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StructType,StructField,StringType}

//Contenido del archivo
var ruta_datos="file:/home/cloudera/Desktop/DataSetPartidos.txt"
var datos =sc.textFile(ruta_datos)
//var que contenga la estructura de los datos
val schemaString = "idPartido::temporada::jornada::EquipoLocal::EquipoVisitante::golesLocal::golesVisitante::fecha::timestamp"
//Generamos el esquema 
val schema = StructType(schemaString.split("::").map(fieldName => StructField(fieldName, StringType, true)))
//convertimos las filas de RDD a Rows
val rowRDD = datos.map(_.split("::")).map(p => Row(p(0), p(1) , p(2) , p(3) , p(4) , p(5) , p(6) , p(7) , p(8).trim))
//aplicamos el schema al RDD
val partidosDF = sqlContext.createDataFrame(rowRDD, schema)
//creamos una tabla temporal
partidosDF.registerTempTable("partidos")

//Consultas

val res = sqlContext.sql("SELECT temporada, jornada FROM partidos")
res.show()
//las columnas en el Row son accesibles por índice o por nombre del campo
res.results.map(t => "Name: " + t(0)).collect().foreach(println)
//¿Cuál es el record de goles como visitante en una temporada del Oviedo?
Val recordOviedo = sqlContext.sql("SELECT SUM(golesVisitante) AS goles, temporada FROM partidos WHERE equipoVisitante='Real Oviedo' GROUP BY temporada ORDER BY goles DESC LIMIT 1")
recordOviedo.show()
//¿Quién ha estado más temporadas en 1 Division Sporting u Oviedo?
val temporadasOviedo = sqlContext.sql("SELECT COUNT(DISTINCT(temporada)) FROM partidos WHERE equipoLocal='Real Oviedo' OR equipoVisitante='Real Oviedo'")
val temporadasSporting = sqlContext.sql("SELECT COUNT(DISTINCT(temporada)) FROM partidos WHERE equipoLocal='Sporting de Gijon' OR equipoVisitante='Sporting de Gijon'")

