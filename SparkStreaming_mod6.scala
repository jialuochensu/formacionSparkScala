//Spark Streaming I - Modulo 6.1
//Iniciarnos en el uso de Spark Streaming

//imports
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.Seconds

//Crear un ssc con duracion 5 sec
val ssc = new StreamingContext(sc, Seconds(5))
//Crea un DStream para leer texto del puerto que pusiste en el comando “nc”, especificando el hostname de nuestra máquina
val mystream = ssc.socketTextStream("quickstart.cloudera",4444)
//mapReduce, para contar las palabrasque aparece en cada stream
val words = mystream.flatMap(line => line.split("\\W"))
val wordCounts = words.map(x => (x, 1)).reduceByKey((x,y) => x+y)
wordCounts.print()
//Arrancar el ssc y llamar awaitTermination para esperar a que la tarea termine
ssc.start()
ssc.awaitTermination()

//Spark Streaming II - Modulo 6.2
//imports
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.Seconds

//Crear un ssc con duracion 5 sec
var ssc=new StreamingContext(sc, Seconds(5))
//Crea un DStream, especificando el hostname(quickstart.cloudera) de nuestra máquina
var dstream=ssc.socketTextStream("quickstart.cloudera",4444)
//Filtra las líneas del Stream que contengan la cadena de caracteres “KBDOC”
val lineas =dstream.filter(x=>x.contains("KBDOC"))

//Para cada RDD, imprime el número de líneas que contienen la cadena de caracteres indicada. Para ello, puedes usar la función “foreachRDD()”.
lineas.foreachRDD{ rdd =>
  val contar = rdd.count()
  println(contar)
}
//Guarda el resultado del filtrado en un fichero de texto en sistema de archivos local
numero=ssc.saveAsTextFiles(“/home/Cloudera/…”)

// Contar el número de líneas que contienen la palabra "KBDOC" en ventanas de 10 segundos cada 2 segundos
val countWindowed = lineas.countByWindow(Seconds(10), Seconds(2))
countWindowed.print()

ssc.start()
b. ssc.awaitTermination()
