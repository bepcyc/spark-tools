## Spark tools
Helper classes for those who spend lots of time with Spark.

Currently supports both Spark 2.0 / Scala 2.11 as well as Spark 1.6 / Scala 2.10.

### Getting the library
Add this to your `build.sbt`:

```scala
resolvers += "jitpack" at "https://jitpack.io"

libraryDependencies += "com.github.bepcyc" %% "spark-tools" % "0.1.2"
```

### Using

```scala
  def addTable(df: DataFrame) = {
    import com.github.bepcyc.SparkTools._
    df.createTable("mydb.df_table", partitions = Seq("month", "day"))
  }
```

Which will create a table `mydb.df_table` in Hive with the same columns as in a DataFrame and (optionally) partitions given.
