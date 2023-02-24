//Trabajando con PairRDDs módulo 4

//ej1
val log = "file:/home/BIT/data/weblogs/*" 
val logs = sc.textFile(log)

//map que contenga (claveID[2], 1).
//reduce para sumar los valores correspondientes para cada claveID
val userRegistro = logs.map(line => line.split(' ')).map(words => (words(2), 1)).reduceByKey((v1, v2)=>v1+v2)

userRegistro.take(5)

//map para intercambiar la clave por el valor.
val swapped = userRegistro.map(campos => campos.swap)

//sortByKey ordenamos por clave (cuando los accesos son los mayores(obtenido del reduce) como CLAVE) -> Orden descendiente
//realizamos el cambio nuevamente, para que obtener clave: userID y valor: num. accesos -> obtener los top 10
swapped.sortByKey(false).map(campos => campos.swap).take(10).foreach(println)

//map que contenga (claveID[2], valorID[0]), y agrupar por claves, para obtner una lista de valores dado clave (groupByKey)
val userIPS = logs.map(line => line.split(' ')).map(words => (words(2), words(0))).groupByKey()
userIPS.take(10)

//ej2
val account = "file:/home/BIT/data/accounts.csv"
//Clave el userID, y el valor todos los elementos
val accounts = sc.textFile(log_2).map(line => line.split(',')).map(account => (account(0), account))
//join account con userRegistro
val acc_userRegistro = accounts.join(userRegistro)

//un RDD que contenga userID, num visit, name, lastName de las 5 primeras lineas
for(acc <-acc_userRegistro.take(5)){
    println((acc._1, acc._2._2, acc._2._1(3), acc._2._1(4)))
}

//ej3
//usando keyBy con codigo postal como clave(8)
val accByPCode = sc.textFile(account).map(line => line.split(',')).keyBy(_(8)) //es lo mismo que hacer keyBy(a => a(8))
//Devuelve clave Pcode y valor todo el array

//Clave: PCode y valor: ls de nombres(4) y apellidos(3) con mapValues()
val namesByPCode = accByPCode.mapValues(valor => valor(4) + ',' + valor(3)).groupByKey()

//Ordenador los datos por PCode, posteriormente los primeros 5 PCode que esten asociados a ese PCode
//Forma1
namesByPCode.sortByKey().take(5).foreach{
    x => println("---" + x._1)
    x._2.foreach(println)
}
//Forma2 - case
namesByPCode.sortByKey().take(5).foreach{
    case(x,y) => println("---" + x)
    y.foreach(println)
}

//ej Opcionales 4.1

//ej1
val shakespeare = sc.textFile("file:/home/BIT/data/shakespeare/*").flatMap(line => line.split(' '))
//(palabra, 1) y con reduceByKey sumamos los valores por palabra
val shakespeare1 = shakespeare.map(word => (word, 1)).reduceByKey(_+_)
//cambiamos la posicion para que sea (numVeces, palabra) para ordenar de mayor a menor y posteriormente volver a cambiar (palabra, numVeces)
val shakespeare2 = shakespeare1.map(word => word.swap).sortByKey(false).map(value => value.swap)

//ej2
//si no cumple la exp. regular -> lo cambiamos a un espacio en blanco
val shakespeare_mod = sc.textFile("file:/home/BIT/data/shakespeare/*").map(line => line.replaceAll("[^a-zA-Z]+"," "))
val shakespeare_mod1 = shakespeare_mod.flatMap(line => line.split(" ")).map(word => word.toLowerCase)
val stopwords = sc.textFile("file:/home/BIT/data/stop-word-list.csv").flatMap(line => line.split(",")).map(word => word.replace(" ", ","))
//creamos una secuencia con un espacio en blanco, y despues eliminamos los espacios en blando del shakespeare_mod1
val res = shakespeare_mod1.substract(sc.parallelize(Seq(" ")))
//eliminamos las palabras con el "diccionario stopwords" del shakespeare_mod1
val res1 = res.substract(stopwords)
//volvemos a aplicar nuevamente, el ej1
val res2 = res1.map(word => (word, 1)).reduceByKey(_+_)
val res3 = res2.map(word => word.swap).sortByKey(false).map(value => value.swap)
//Queremos ocurrencias de más de una letra, por lo tanto usamos el filter para quitar los de tamaño 1
val resFinal = res3.filter(word => word._1.size !=1 ).take(20).foreach(println)


