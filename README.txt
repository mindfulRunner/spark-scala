README


Developing Spark Applications Using Scala & Cloudera
https://app.pluralsight.com/library/courses/spark-scala-cloudera/table-of-contents

Spark 2 Requirements
	- http://tiny.bigdatainc.org/sparkreq
	- https://docs.cloudera.com/documentation/spark2/latest/topics/spark2_requirements.html
	
	
	CDH (Cloudera Distributed Hadoop)
	CDS (Cloudera Distributed Spark)
	


NOTE:
    - test data is coming from PluralSight tutorial 
        - D:\aaa\workout\techs\bigdata\a_courses\pluralsight\2_develop_spark_app_with_scala_&_cloudera\demo
	- use the docker build
		- D:\aaa\workout\techs\bigdata\a_docker\20220308\
    - exercise notes can be found in PluralSight tutorial 
        - D:\aaa\workout\techs\bigdata\a_courses\pluralsight\2_develop_spark_app_with_scala_&_cloudera\demo
		
	- reference D:\aaa\workout\techs\bigdata\a_docker\20220308\hadoop_docker_cluster\README.txt for details
		
	- copy course materials to docker
		- D:\aaa\workout\techs\bigdata\a_courses\pluralsight\2_develop_spark_app_with_scala_&_cloudera>docker cp spark-scala-cloudera.zip nodemaster:/tmp
		
		- D:\aaa\workout\techs\bigdata\a_courses\pluralsight\2_develop_spark_app_with_scala_&_cloudera>docker cp vi.stackexchange.com.7z nodemaster:/tmp
		
	- extract course materials
		- hadoop@nodemaster:/tmp$ dtrx spark-scala-cloudera.zip
		- hadoop@nodemaster:/tmp$ dtrx vi.stackexchange.com.7z
		
	- put stackexchange files to HDFS
		hadoop@nodemaster:/tmp/vi.stackexchange.com$ hdfs dfs -mkdir -p /user/cloudera/stackexchange
		hadoop@nodemaster:/tmp/vi.stackexchange.com$ hdfs dfs -ls
		ls: `.': No such file or directory
		hadoop@nodemaster:/tmp/vi.stackexchange.com$ hdfs dfs -put *.xml /user/cloudera/stackexchange
		hadoop@nodemaster:/tmp/vi.stackexchange.com$ hdfs dfs -ls /user/cloudera/stackexchange
		Found 8 items
		-rw-r--r--   1 hadoop supergroup    6602759 2022-10-06 05:22 /user/cloudera/stackexchange/Badges.xml
		-rw-r--r--   1 hadoop supergroup   15360893 2022-10-06 05:22 /user/cloudera/stackexchange/Comments.xml
		-rw-r--r--   1 hadoop supergroup   80356100 2022-10-06 05:22 /user/cloudera/stackexchange/PostHistory.xml
		-rw-r--r--   1 hadoop supergroup     418926 2022-10-06 05:22 /user/cloudera/stackexchange/PostLinks.xml
		-rw-r--r--   1 hadoop supergroup   45023223 2022-10-06 05:22 /user/cloudera/stackexchange/Posts.xml
		-rw-r--r--   1 hadoop supergroup      30338 2022-10-06 05:22 /user/cloudera/stackexchange/Tags.xml
		-rw-r--r--   1 hadoop supergroup   15291027 2022-10-06 05:22 /user/cloudera/stackexchange/Users.xml
		-rw-r--r--   1 hadoop supergroup   12854948 2022-10-06 05:22 /user/cloudera/stackexchange/Votes.xml
		hadoop@nodemaster:/tmp/vi.stackexchange.com$
		
		
	- how to create csv from xml through stackexchange data
		build / run scala project
			- go to hadoop@nodemaster with hadoop user
			- cp /tmp/spark-scala-cloudera/demos/2 - Getting an Environment and Data - CDH + StackOverflow/posts /tmp/posts
				- Spark, HDFS don't work well with long path with spaces in the folder name
			- build scala project
				- sbt package
			- run spark-submit command as specified in prepare_data_posts_all_csv.scala
				- spark-submit --class "PreparePostsCSVApp" target/scala-2.11/posts-project_2.11-1.0.jar
					- hadoop@nodemaster:/tmp/posts$ spark-submit --class PreparePostsCSVApp target/scala-2.11/posts-project_2.11-1.0.jar
			- there should be a directory called posts_all_csv created in HDFS
		
		- similarly, for user_csv
		
			hadoop@nodemaster:/tmp/demo/users$ ls
			build.sbt  src
			hadoop@nodemaster:/tmp/demo/users$ sbt package
			hadoop@nodemaster:/tmp/demo/users$ ls
			build.sbt  project  src  target
			hadoop@nodemaster:/tmp/demo/users$ spark-submit --class PrepareUsersApp target/scala-2.11/users-project_2.11-1.0.jar
			
			users_csv will be created in HDFS!
			
			hadoop@nodemaster:/$ hdfs dfs -ls /user/cloudera/stackexchange/
			Found 19 items
			-rw-r--r--   1 hadoop supergroup    6602759 2022-07-08 01:23 /user/cloudera/stackexchange/Badges.xml
			-rw-r--r--   1 hadoop supergroup   15360893 2022-07-08 01:23 /user/cloudera/stackexchange/Comments.xml
			-rw-r--r--   1 hadoop supergroup   80356100 2022-07-08 01:23 /user/cloudera/stackexchange/PostHistory.xml
			-rw-r--r--   1 hadoop supergroup     418926 2022-07-08 01:23 /user/cloudera/stackexchange/PostLinks.xml
			-rw-r--r--   1 hadoop supergroup   45023223 2022-07-08 01:24 /user/cloudera/stackexchange/Posts.xml
			-rw-r--r--   1 hadoop supergroup      30338 2022-07-08 01:24 /user/cloudera/stackexchange/Tags.xml
			-rw-r--r--   1 hadoop supergroup   15291027 2022-07-08 01:24 /user/cloudera/stackexchange/Users.xml
			-rw-r--r--   1 hadoop supergroup   12854948 2022-07-08 01:24 /user/cloudera/stackexchange/Votes.xml
			drwxr-xr-x   - hadoop supergroup          0 2022-07-08 01:43 /user/cloudera/stackexchange/badges_csv
			drwxr-xr-x   - hadoop supergroup          0 2022-07-09 01:59 /user/cloudera/stackexchange/badges_newapihadoop
			drwxr-xr-x   - hadoop supergroup          0 2022-07-25 06:11 /user/cloudera/stackexchange/badges_no_partitioner
			drwxr-xr-x   - hadoop supergroup          0 2022-07-09 01:39 /user/cloudera/stackexchange/badges_object
			drwxr-xr-x   - hadoop supergroup          0 2022-07-09 01:35 /user/cloudera/stackexchange/badges_txt
			drwxr-xr-x   - hadoop supergroup          0 2022-07-09 01:32 /user/cloudera/stackexchange/badges_txt_array
			drwxr-xr-x   - hadoop supergroup          0 2022-07-25 06:11 /user/cloudera/stackexchange/badges_yes_partitioner
			drwxr-xr-x   - hadoop supergroup          0 2022-07-08 01:30 /user/cloudera/stackexchange/posts_all_csv
			drwxr-xr-x   - hadoop supergroup          0 2022-07-16 01:09 /user/cloudera/stackexchange/simple_titles_txt
			drwxr-xr-x   - hadoop supergroup          0 2022-07-09 01:46 /user/cloudera/stackexchange/tuple_sequence
			drwxr-xr-x   - hadoop supergroup          0 2022-08-05 01:38 /user/cloudera/stackexchange/users_csv
			hadoop@nodemaster:/$




Course Highlights


0 - Course Overview
1 - Why Spark with Scala and Cloudera?
2 - Getting an Environment and Data: CDH + StackOverflow
3 - Refreshing Your Knowledge: Scala Fundamentals for This Course
4 - Understanding Spark: An Overview
	3 - A Few Words on Fine Grained Transformations and Scalability
		- Find grained transformation - update 1 cell in 1 row in 1 table in 1 database
			- update posts set tags='[apache-spark,sql]' where postid=1
	5 - How Word Count Works, Featuring Coarse Grained Transformations
		- Coarse Grained Transformation - operations to be applied to the whole dataset, like map, filter, group, reduce
			- steps:
				Step 1 - Split
				Step 2 - Output (of split) -> input (of next step)
				Step 3 - flatMap
				Step 4 - map
				Step 5 - reduceByKey
				
				val lines = sc.textFile("file:///se/simple_titles.txt")
				val words = lines.flatMap(line => line.split(" ")) // Step 1, 2, 3
				val word_for_count = words.map(x => (x, 1)) // Step 4
				word_for_count.reduceByKey(_ + _).collect() // Step 5
	6 - Parallelism by Partitoning Data
		- parallel process is faster than sequential one
	7 - Pipelining: One of Secrets of Spark's Performance
		- pipeline allows process to continue executing without waiting, so it's even faster than parallel one
	8 - Narrow and Wide Transformations
		- transformation with partition with unrelated data
		- gather similar data (with the same key) together cross partitions to form a single one
			- shuffle
	9 - Lazy Execution, Lineage, Directed Acyclic Graph (DAG), and Fault Tolerance
5 - Getting Technical with Spark
		
		D:\aaa\workout\techs\bigdata\a_courses\pluralsight\2_develop_spark_app_with_scala_&_cloudera\spark-scala-cloudera\demos\5 - Getting Techincal with Spark\getting-technical-spark.scala
		
		
	2 - Storage in Spark and Supported Data Formats
		- HDFS is default storage
			scala> sc.textFile("/user/cloudera/spark-committers.tsv").take(10)
			res3: Array[String] = Array(Name        Organization, Sameer Agarwal    Databricks, Michael Armbrust    Databricks, Joseph Bradley      Databricks, Felix Cheung        Microsoft, Mosharaf Chowdhury   University of Michigan, Ann Arbor, Jason Dai    Intel, Tathagata Das    Databricks, Ankur Dave  UC Berkeley, Aaron Davidson     Databricks)
		
		- local file system
			scala> sc.textFile("file:////tmp/spark-committers.tsv").take(10)
			res4: Array[String] = Array(Name        Organization, Sameer Agarwal    Databricks, Michael Armbrust    Databricks, Joseph Bradley      Databricks, Felix Cheung        Microsoft, Mosharaf Chowdhury   University of Michigan, Ann Arbor, Jason Dai    Intel, Tathagata Das    Databricks, Ankur Dave  UC Berkeley, Aaron Davidson     Databricks)
		
		- AWS S3
			scala> sc.textFile("s3a://pluralsight-spark-cloudera-scala/spark-committers-no-header.tsv").take(10)
				- need set up AWS S3 credential
	5 - SparkContext and SparkSession: Entry Points to Spark Apps
		- SparkContext
			- sc
				scala> sc
				res0: org.apache.spark.SparkContext = org.apache.spark.SparkContext@70201785
				
				scala> sc.getClass.getName
				res3: String = org.apache.spark.SparkContext
			- sc.version
			- sc.getClass
			- sc.applicationId
			- sc.sparkUser
			
		- SparkSession
			- spark
				scala> spark
				res6: org.apache.spark.sql.SparkSession = org.apache.spark.sql.SparkSession@44c6e366
				
				scala> spark.getClass.getName
				res7: String = org.apache.spark.sql.SparkSession
				
			- can get SparkContext
				scala> spark.sparkContext
				res9: org.apache.spark.SparkContext = org.apache.spark.SparkContext@6d8948b2
			
		- Spark SQL
			- spark.sql("select * from committers").show(10)
			- cmDF.select("Name").show(10)
				- not working now
				
		- Spark catalog API
			- spark.catalog
				scala> spark.catalog.listDatabases().show(truncate=false)
				+-------+----------------+---------------------+
				|name   |description     |locationUri          |
				+-------+----------------+---------------------+
				|default|default database|file:/spark-warehouse|
				+-------+----------------+---------------------+
				
	6 - Spark Configuration + Client and Cluster Deployment Modes
		- spark-defaults.conf
			- flags passed to spark-submit / spark-shell
			
			scala> spark.conf.get("spark.eventLog.dir")
			res12: String = file:/home/hadoop/spark/logs
		
			scala> import org.apache.spark.sql.SparkSession
			import org.apache.spark.sql.SparkSession
		
			scala> val spark_other_session = SparkSession.builder().
				 | master("yarn").
				 | appName("Test conf").
				 | config("spark.eventLog.dir", "/stackexchange/logs").
				 | getOrCreate()
			2022-06-24 00:45:20,850 WARN sql.SparkSession$Builder: Using an existing SparkSession; some spark core configurations may not take effect.
			spark_other_session: org.apache.spark.sql.SparkSession = org.apache.spark.sql.SparkSession@44c6e366
		
			scala> spark_other_session.conf.get("spark.eventLog.dir")
			res13: String = /stackexchange/logs
			
		- in application
			val conf = new SparkConf()
				.setMaster("yarn")
				.setAppName("Stackoverflow test")
				.set("spark.executor.memory", "1g")
			val sc = new SparkContext(conf)
			
		- dynamically set properties on execution
			spark-submit
				-- executor-memory 4g
				-- conf spark.eventLog.enabled=false
				-- class "PrepareBadgesCSVApp"
				target/scala-2.11/badges-project_2.11-1.0.jar
				
		- check all confs
			scala> sc.getConf.getAll
			res16: Array[(String, String)] = Array((spark.repl.class.uri,spark://nodemaster:39995/classes), (spark.driver.host,nodemaster), (spark.eventLog.dir,file:/home/hadoop/spark/logs), (spark.sql.catalogImplementation,in-memory), (spark.app.id,local-1656028694996), (spark.driver.port,39995), (spark.home,/home/hadoop/spark), (spark.executor.id,driver), (spark.app.name,Spark shell), (spark.repl.class.outputDir,/tmp/spark-3346703e-f6b6-4b7e-b8c0-b87618188ef0/repl-1ef78a65-f612-4a2c-b45f-27db471f4ed9), (spark.jars,""), (spark.master,local[*]), (spark.submit.pyFiles,""), (spark.submit.deployMode,client), (spark.history.fs.logDirectory,file:/home/hadoop/spark/logs), (spark.ui.showConsoleProgress,true))
			
		- Deployment mode (client / cluster)
			val spark = SparkSession.
				builder().
				appName("Stackoverflow test").
				config("spark.submit.deployMode", "client").	// standalone mode
				config("spark.submit.deployMode", "cluster").	// cluster mode
				getOrCreate()
				
	9 - Visualizing Your Spark App: Web UI & History Server
		- Spark Web UI
			- localhost:4040
		
		- Spark History Server
			- localhost:18089
6. Learning the Core of Spark: RDDs
	- D:\aaa\workout\techs\bigdata\a_courses\pluralsight\2_develop_spark_app_with_scala_&_cloudera\spark-scala-cloudera\demos\6 - Learning the Core of Spark - RDDs\learning_core_spark_rdd.scala
		
		
		
	2 - SparkContext: The Entry Point to a Spark Application
		- SparkContext (sc) is for
			- Spark Application
			- Configuration & Environment
			- Services
			- Create RDDs
		- create a new SparkContext
			- SparkContext.getOrCreate()
	3 - RDD and PairRDD - Resilient Distributed Datasets
		- 3 ways to create a RDD
			- parellize
			- external data
			- from another RDD
	4 - Creating RDD with Parallelize
		- scala> val list_one_to_five = sc.parallelize(List(1,2,3,4,5))
		  list_one_to_five: org.apache.spark.rdd.RDD[Int] = ParallelCollectionRDD[1] at parallelize at <console>:26
		
		- scala> list_one_to_five.collect()
		  res8: Array[Int] = Array(1, 2, 3, 4, 5)
		
		- scala> list_one_to_five.getNumPartitions
		  res9: Int = 8
		  
		- create empty RDD
			- val empty_rdd = sc.parallelize(Array[String]())
			- val other_empty = sc.emptyRDD
	5 - Returning Data to the Driver, ie. collect(), take(), first()...
		- get external data
			- scala> def log_search(url: String) = {
				   |	val page = scala.io.Source.fromURL("http://tiny.bigdatainc.org/" + url)
				   |	print("Url Accessed" + "http://tiny.bigdatainc.org/" + url)
				   | }
			- scala> val queries = sc.parallelize(Seq("ts1", "ts2"))
			- scala> queries.foreach(log_search)
	6 - Partions, Repartitions, Coalesce, Save as Text, HUE
		- saveAsTextFile (save to hadoop HDFS), repartition, coalesce
			scala> val bigger_rdd = sc.parallelize(1 to 1000)
			bigger_rdd: org.apache.spark.rdd.RDD[Int] = ParallelCollectionRDD[0] at parallelize at <console>:24

			scala> bigger_rdd.getNumPartitions
			res1: Int = 8

			scala> val play_part = bigger_rdd.repartition(10)
			play_part: org.apache.spark.rdd.RDD[Int] = MapPartitionsRDD[4] at repartition at <console>:25

			scala> play_part.saveAsTextFile("/user/cloudera/tests/play_part/ten")

			scala> play_part.repartition(4).saveAsTextFile("/user/cloudera/tests/play_part/four")

			scala> play_part.coalesce(1).saveAsTextFile("/user/cloudera/tests/play_part/one")
	7 - Creating RDDs from External Datasets
		- textFile() - read from HDFS
			scala> sc.textFile("/user/cloudera/tests/play_part/ten").count
			res5: Long = 1000

			scala> sc.textFile("/user/cloudera/tests/play_part/four").count
			res6: Long = 1000

			scala> sc.textFile("/user/cloudera/tests/play_part/four/part-00001").count()
			res7: Long = 250
		- textFile() - read from local file system
			hadoop@nodemaster:/tmp$ hdfs dfs -ls /user/cloudera/tests/play_part/one
			Found 2 items
			-rw-r--r--   1 hadoop supergroup          0 2022-07-07 18:57 /user/cloudera/tests/play_part/one/_SUCCESS
			-rw-r--r--   1 hadoop supergroup       3893 2022-07-07 18:57 /user/cloudera/tests/play_part/one/part-00000
			
			hadoop@nodemaster:/tmp$ hdfs dfs -get /user/cloudera/tests/play_part/one/part-00000 .
			
			scala> sc.textFile("file:///tmp/part-00000").count()
			res8: Long = 1000
		- convert XML (Posts.xml) to CSV
			For Posts,
				- follow D:\aaa\workout\techs\bigdata\a_docker\20220308\hadoop_docker_cluster\README.txt
				  to create stackexchange posts CSV if haven't done so
				
				scala> val posts_all = sc.textFile("/user/cloudera/stackexchange/posts_all_csv")
				posts_all: org.apache.spark.rdd.RDD[String] = /user/cloudera/stackexchange/posts_all_csv MapPartitionsRDD[24] at textFile at <console>:24

				scala> posts_all.count()
				res10: Long = 28750
			
			For Badges,
				hadoop@nodemaster:/tmp/demo/badges$ spark-submit --class PrepareBadgesCSVApp target/scala-2.11/badges-project_2.11-1.0.jar
				
				scala> val badges_rdd_csv = sc.textFile("/user/cloudera/stackexchange/badges_csv")
				badges_rdd_csv: org.apache.spark.rdd.RDD[String] = /user/cloudera/stackexchange/badges_csv MapPartitionsRDD[26] at textFile at <console>:24

				scala> badges_rdd_csv.take(1)
				res11: Array[String] = Array(1,5,Autobiographer,2015-02-03T16:41:10.170,3,False)

				Create an RDD from another one

				scala> def split_the_line(x: String): Array[String] = x.split(",")
				split_the_line: (x: String)Array[String]

				scala> val badges_columns_rdd = badges_rdd_csv.map(split_the_line)
				badges_columns_rdd: org.apache.spark.rdd.RDD[Array[String]] = MapPartitionsRDD[27] at map at <console>:27

				scala> badges_columns_rdd.take(1)
				res12: Array[Array[String]] = Array(Array(1, 5, Autobiographer, 2015-02-03T16:41:10.170, 3, False))

				scala>
		- can load JSON file
			- val tags_json = sc.textFile("/user/cloudera/stackexchange/tags_json")
			  tags_json.first()
		- can upload Amazon S3 
			- val tags_json_s3 = sc.textFile("s3a://pluralsight-spark-cloudera-scala/part-00000")
			  tags_json_s3.take(10)
	8 - Saving Data as ObjectFile, NewAPIHadoopFile, SequenceFile, ....
		- D:\aaa\workout\techs\bigdata\a_courses\pluralsight\2_develop_spark_app_with_scala_&_cloudera\spark-scala-cloudera\demos\6 - Learning the Core of Spark - RDDs\learning_core_spark_rdd.scala
			
		- save / write functions
			saveAsTextFile()
			saveAsObjectFile()
			saveAsSequenceFile()
			saveAsNewAPIHadoopFile()
		- load / read functions
			textFile()
			objectFile()
			sequenceFile()
			newAPIHadoopFile()
	9 - Creating RDDs with Transformations
		scala> def split_the_line(x: String): Array[String] = x.split(",")
		split_the_line: (x: String)Array[String]

		scala> val badges_rdd_csv = sc.textFile("/user/cloudera/stackexchange/badges_csv")
		badges_rdd_csv: org.apache.spark.rdd.RDD[String] = /user/cloudera/stackexchange/badges_csv MapPartitionsRDD[1] at textFile at <console>:24

		scala> val badges_columns_rdd = badges_rdd_csv.map(split_the_line)
		badges_columns_rdd: org.apache.spark.rdd.RDD[Array[String]] = MapPartitionsRDD[2] at map at <console>:27

		scala> val badges_entry = badges_columns_rdd.map(x => x(2))
		badges_entry: org.apache.spark.rdd.RDD[String] = MapPartitionsRDD[3] at map at <console>:25

		scala> badges_entry.take(1)
		res0: Array[String] = Array(Autobiographer)

		scala> val badges_name = badges_entry.map(x => (x, 1))
		badges_name: org.apache.spark.rdd.RDD[(String, Int)] = MapPartitionsRDD[4] at map at <console>:25

		scala> badges_name.take(1)
		res1: Array[(String, Int)] = Array((Autobiographer,1))

		scala> val badges_reduced = badges_name.reduceByKey(_ + _)
		badges_reduced: org.apache.spark.rdd.RDD[(String, Int)] = ShuffledRDD[5] at reduceByKey at <console>:25

		scala> badges_reduced.take(1)
		res2: Array[(String, Int)] = Array((Critic,288))

		scala> val badges_count_badge = badges_reduced.map({ case(x,y) => (y,x) })
		badges_count_badge: org.apache.spark.rdd.RDD[(Int, String)] = MapPartitionsRDD[6] at map at <console>:25

		scala> badges_count_badge.take(1)
		res3: Array[(Int, String)] = Array((288,Critic))

		scala> val badges_sorted = badges_count_badge.sortByKey(false).map({ case(x,y) => (y,x) })
		badges_sorted: org.apache.spark.rdd.RDD[(String, Int)] = MapPartitionsRDD[10] at map at <console>:25

		scala> badges_sorted.take(10)
		res4: Array[(String, Int)] = Array((Autobiographer,15401), (Supporter,11804), (Student,4232), (Scholar,3083), (Editor,2981), (Popular Question,2726), (Yearling,2534), (Teacher,2437), (Informed,1909), (Nice Answer,1384))

		scala> badges_sorted.take(1)
		res5: Array[(String, Int)] = Array((Autobiographer,15401))
	10 - A Little Bit More on Lineage
		- to see RDD lineage
			- toDebugString
			
			scala> badges_sorted.toDebugString
			res8: String =
			(2) MapPartitionsRDD[10] at map at <console>:25 []
			 |  ShuffledRDD[9] at sortByKey at <console>:25 []
			 +-(2) MapPartitionsRDD[6] at map at <console>:25 []
				|  ShuffledRDD[5] at reduceByKey at <console>:25 []
				+-(2) MapPartitionsRDD[4] at map at <console>:25 []
				   |  MapPartitionsRDD[3] at map at <console>:25 []
				   |  MapPartitionsRDD[2] at map at <console>:27 []
				   |  /user/cloudera/stackexchange/badges_csv MapPartitionsRDD[1] at textFile at <console>:24 []
				   |  /user/cloudera/stackexchange/badges_csv HadoopRDD[0] at textFile at <console>:24 []

			scala>
		- to explore dependencies
			- dependencies
			
			scala> badges_sorted.dependencies
			res9: Seq[org.apache.spark.Dependency[_]] = List(org.apache.spark.OneToOneDependency@482741af)

			scala> badges_reduced.dependencies
			res10: Seq[org.apache.spark.Dependency[_]] = List(org.apache.spark.ShuffleDependency@23269d0b)
7. Going Deeper into Spark Core
		
	D:\aaa\workout\techs\bigdata\a_courses\pluralsight\2_develop_spark_app_with_scala_&_cloudera\spark-scala-cloudera\demos\7 - Going Deeper into Spark Core\going_deeper_into_spark_core.scala	
		
		
	3 - A Quick Look at Map, FlatMap, Filter, and Sort
		- create simple_titles_txt
	
		hadoop@nodemaster:/tmp/demo/posts_simple_titles$ sbt package
		hadoop@nodemaster:/tmp/demo/posts_simple_titles$ spark-submit --class PreparePostsSimpleTitlesApp target/scala-2.11/posts-simple-titles-project_2.11-1.0.jar
		
		hadoop@nodemaster:/tmp$ hdfs dfs -ls /user/cloudera/stackexchange
		Found 16 items
		-rw-r--r--   1 hadoop supergroup    6602759 2022-07-08 01:23 /user/cloudera/stackexchange/Badges.xml
		-rw-r--r--   1 hadoop supergroup   15360893 2022-07-08 01:23 /user/cloudera/stackexchange/Comments.xml
		-rw-r--r--   1 hadoop supergroup   80356100 2022-07-08 01:23 /user/cloudera/stackexchange/PostHistory.xml
		-rw-r--r--   1 hadoop supergroup     418926 2022-07-08 01:23 /user/cloudera/stackexchange/PostLinks.xml
		-rw-r--r--   1 hadoop supergroup   45023223 2022-07-08 01:24 /user/cloudera/stackexchange/Posts.xml
		-rw-r--r--   1 hadoop supergroup      30338 2022-07-08 01:24 /user/cloudera/stackexchange/Tags.xml
		-rw-r--r--   1 hadoop supergroup   15291027 2022-07-08 01:24 /user/cloudera/stackexchange/Users.xml
		-rw-r--r--   1 hadoop supergroup   12854948 2022-07-08 01:24 /user/cloudera/stackexchange/Votes.xml
		drwxr-xr-x   - hadoop supergroup          0 2022-07-08 01:43 /user/cloudera/stackexchange/badges_csv
		drwxr-xr-x   - hadoop supergroup          0 2022-07-09 01:59 /user/cloudera/stackexchange/badges_newapihadoop
		drwxr-xr-x   - hadoop supergroup          0 2022-07-09 01:39 /user/cloudera/stackexchange/badges_object
		drwxr-xr-x   - hadoop supergroup          0 2022-07-09 01:35 /user/cloudera/stackexchange/badges_txt
		drwxr-xr-x   - hadoop supergroup          0 2022-07-09 01:32 /user/cloudera/stackexchange/badges_txt_array
		drwxr-xr-x   - hadoop supergroup          0 2022-07-08 01:30 /user/cloudera/stackexchange/posts_all_csv
		drwxr-xr-x   - hadoop supergroup          0 2022-07-16 01:09 /user/cloudera/stackexchange/simple_titles_txt
		drwxr-xr-x   - hadoop supergroup          0 2022-07-09 01:46 /user/cloudera/stackexchange/tuple_sequence
		hadoop@nodemaster:/tmp$
		
		- basic operation
		
		scala> val lines = sc.textFile("/user/cloudera/stackexchange/simple_titles_txt")
		lines: org.apache.spark.rdd.RDD[String] = /user/cloudera/stackexchange/simple_titles_txt MapPartitionsRDD[14] at textFile at <console>:24
		
		scala> val lines_10 = lines.take(10)
		lines_10: Array[String] = Array(How can I add line numbers to Vim, "", How can I show relative line numbers, How can I change the default indentation based on filetype, "", How can I use the undofile, "", "", Can I script Vim using Python, "")

		scala> for (l <- lines_10.collect()) println(l)
		<console>:26: error: not enough arguments for method collect: (pf: PartialFunction[String,B])(implicit bf: scala.collection.generic.CanBuildFrom[Array[String],B,That])That.
		Unspecified value parameter pf.
			   for (l <- lines_10.collect()) println(l)
										 ^

		scala> val lines_10 = lines.toDF().limit(10).rdd
		lines_10: org.apache.spark.rdd.RDD[org.apache.spark.sql.Row] = MapPartitionsRDD[22] at rdd at <console>:25

		scala> for (l <- lines_10.collect()) println(l)
		[How can I add line numbers to Vim]
		[]
		[How can I show relative line numbers]
		[How can I change the default indentation based on filetype]
		[]
		[How can I use the undofile]
		[]
		[]
		[Can I script Vim using Python]
		[]

		scala>
	
		- Map
			scala> val words_in_line = lines.map(x => x.split(" "))
			words_in_line: org.apache.spark.rdd.RDD[Array[String]] = MapPartitionsRDD[23] at map at <console>:25

			scala> words_in_line.collect()
			res15: Array[Array[String]] = Array(Array(How, can, I, add, line, numbers, to, Vim), Array(""), Array(How, can, I, show, relative, line, numbers), Array(How, can, I, change, the, default, indentation, based, on, filetype), Array(""), Array(How, can, I, use, the, undofile), Array(""), Array(""), Array(Can, I, script, Vim, using, Python), Array(""), Array(""), Array(How, can, I, generate, a, list, of, sequential, numbers,, one, per, line), Array(Does, any, solution, exist, to, use, vim, from, touch, screen), Array(""), Array(""), Array(Can, I, use, some, file-tree, selector, which, exists, on, graphical, IDEs), Array(How, can, I, search, for, a, string, between, certain, line, numbers), Array(""), Array(Why, can, ci&quot;, be, outside, of, quoted, area, and, ci(,...

			scala>
			
		- FlatMap
			- *** it solves the problem of List of List by flattening nested lists into a single list
			
			scala> val words = lines.flatMap(x => x.split(" "))
			words: org.apache.spark.rdd.RDD[String] = MapPartitionsRDD[24] at flatMap at <console>:25

			scala> words.collect()
			res16: Array[String] = Array(How, can, I, add, line, numbers, to, Vim, "", How, can, I, show, relative, line, numbers, How, can, I, change, the, default, indentation, based, on, filetype, "", How, can, I, use, the, undofile, "", "", Can, I, script, Vim, using, Python, "", "", How, can, I, generate, a, list, of, sequential, numbers,, one, per, line, Does, any, solution, exist, to, use, vim, from, touch, screen, "", "", Can, I, use, some, file-tree, selector, which, exists, on, graphical, IDEs, How, can, I, search, for, a, string, between, certain, line, numbers, "", Why, can, ci&quot;, be, outside, of, quoted, area, and, ci(, only, works, inside, parentheses, "", "", How, do, I, reload, my, vimrc, without, leaving, Vim, How, can, I, add, syntax, highlighting, to...

			scala>
			
		- Filter
			scala> def starts_h(word: (String, Int)) = word._1.toLowerCase.startsWith("h")
			starts_h: (word: (String, Int))Boolean

			scala> word_for_count.filter(starts_h).collect()
			res18: Array[(String, Int)] = Array((How,1), (How,1), (How,1), (How,1), (How,1), (How,1), (How,1), (How,1), (highlighting,1), (How,1), (How,1), (have,1), (How,1), (How,1), (How,1), (How,1), (How,1), (How,1), (How,1), (How,1), (hidden,1), (How,1), (How,1), (How,1), (How,1), (How,1), (How,1), (How,1), (have,1), (How,1), (happen,1), (How,1), (How,1), (How,1), (How,1), (How,1), (How,1), (How,1), (highlighting,1), (How,1), (How,1), (host,1), (How,1), (how,1), (How,1), (How,1), (How,1), (How,1), (How,1), (How,1), (How,1), (How,1), (history,1), (How,1), (highlighting,1), (How,1), (How,1), (How,1), (How,1), (have,1), (hidden,1), (How,1), (horizontal,1), (How,1), (How,1), (How,1), (help,1), (How,1), (history,1), (How,1), (How,1), (How,1), (HTML,1), (How,1), (header,1), ...

			scala>
			
		- SortBy and SortByKey
			scala> val word_count = word_for_count.reduceByKey(_ + _)
			word_count: org.apache.spark.rdd.RDD[(String, Int)] = ShuffledRDD[27] at reduceByKey at <console>:25

			scala> word_count.take(10)
			res19: Array[(String, Int)] = Array((apostrophes,2), (end/get,1), ((timeoutlen,1), (mjs,1), (&lt;Nop&gt;,2), (24-bit,1), (shellescape(),1), (vertically-block-select,1), (chapters,1), (key),5))

			scala> word_count.sortByKey().collect()
			res20: Array[(String, Int)] = Array(("",16452), (!,9), (!!foo,1), (!%,1), (!=,1), (!cd,1), (!echo,1), (!n,1), (!v,1), (!{cmd},1), (!{cmd}`,1), (#,17), (##,1), (#),1), (#-comments,1), (#45;),1), (#define,1), (#ifdef,1), (#include,1), (#{},1), ($,11), ($(),1), ($(which,1), ($*,1), ($F)i,,1), ($HOME,6), ($HOME/.vim,1), ($HOME/.viminfo!,1), ($HOME/bar`,1), ($HOME\_vimrc,1), ($LS_COLORS,1), ($MYVIMRC,6), ($PATH,4), ($PWD,1), ($TERM,1), ($VIM,2), ($VIMRUNTIME\mswin.vim,1), ($VIM\_vimrc,1), ($XDG_CONFIG_HOME,1), ($path,2), (%,15), (%&gt;%,1), (%,,1), (%-key,1), (%:p,1), (%:~&quot;,1), (%F,1), (%PATH%'),1), (%TeX,1), (%cpaste,1), (%f,1), (%match,1), (%s/regex/\=call-macro-as-function/gc,1), (&amp;,37), (&amp;&amp;,2), (&amp;columns,1), (&amp;cp,1), (&amp;filetype,1), (...

			scala> word_count.sortByKey(false).collect()
			res21: Array[(String, Int)] = Array((?,1), (?,1), (?,1), (?,1), (?,1), (?set,1), (?search,1), (?scanning,1), (?python?,1), (?python3?,1), (?on-the-fly?,1), (?gf?,1), (?ctrl+w+shift+f?,1), (?chomp?,1), (?balloonexpr?,1), (?E432:,1), (?-c?,1), (??,1), (?smart,1), (?n?,1), (?#?,1), (?,1), (??,1), (??,1), (?,1), (?,1), (?^,1), (?,1), (?,1), (?,1), (?y,1), (?,1), (?,1), (?,1), (~/vimfiles,2), (~/.vimrc,7), (~/.vim/vimrc,1), (~/.vim/snippets/,1), (~/.vim/colors,1), (~/.vim/bundle/python-mode/autoload/pymode/lint.vim,1), (~/.vim,5), (~/.config/nvim/init.vim,1), (~/.config/nvim/init.lua,1), (~/.config/nvim,1), (~.vim/after/,1), (~,,1), (~,7), (}`,1), (}',1), (},7), (||,1), (|&quot;,1), (|,9), ({},,1), ({},3), ({what},1), ({vimrc}&quot;,1), ({the,1), ({sub},1), ({subjec...

			scala>
			
			- more useful sort
				- swap the key / value
				- sortByKey in descending order
				- swap the key / value back
			scala> word_count.map({ case (x, y) => (y, x) }).sortByKey(false).map(x => x.swap).collect()
			res22: Array[(String, Int)] = Array(("",16452), (to,5222), (How,3477), (a,3382), (in,3310), (the,2966), (of,1767), (I,1527), (with,1368), (and,1362), (vim,1269), (for,1093), (file,1041), (line,985), (Vim,960), (command,893), (on,881), (is,808), (when,791), (from,791), (not,783), (can,711), (do,699), (mode,655), (Is,468), (buffer,454), (Why,453), (files,453), (it,452), (using,437), (lines,425), (text,424), (does,408), (use,384), (cursor,370), (function,366), (after,358), (window,351), (or,350), (current,349), (an,330), (as,321), (without,311), (there,308), (make,307), (What,300), (open,293), (way,290), (syntax,290), (that,288), (insert,287), (get,284), (search,275), (how,273), (key,271), (terminal,269), (work,268), (my,265), (only,256), (between,253), (by,251), ...

			scala>
			
			- use sortBy instead of sortByKey to simplify it
				- -y to specify sort by y AND in reverse order!
			scala> word_count.sortBy({ case (x, y) => -y }).collect()
			res23: Array[(String, Int)] = Array(("",16452), (to,5222), (How,3477), (a,3382), (in,3310), (the,2966), (of,1767), (I,1527), (with,1368), (and,1362), (vim,1269), (for,1093), (file,1041), (line,985), (Vim,960), (command,893), (on,881), (is,808), (when,791), (from,791), (not,783), (can,711), (do,699), (mode,655), (Is,468), (buffer,454), (Why,453), (files,453), (it,452), (using,437), (lines,425), (text,424), (does,408), (use,384), (cursor,370), (function,366), (after,358), (window,351), (or,350), (current,349), (an,330), (as,321), (without,311), (there,308), (make,307), (What,300), (open,293), (way,290), (syntax,290), (that,288), (insert,287), (get,284), (search,275), (how,273), (key,271), (terminal,269), (work,268), (my,265), (only,256), (between,253), (by,251), ...

			scala>
	4 - How Can I Tell It Is a Transformation
		if the API says it returns a new RDD, then it is a transformation!
			
			- def map(...)
				Return a new RDD by applying ...
			- def mapPartitions(...)
				Return a new RDD ...
			- def mapPartitionsWithIndex(...)
				Return a new RDD ...
	5 - Why Do We Need Actions?
		- because spark is lazy
		- action to trigger evaluation
			- collect()
			- take()
			- saveAsTextFile()
			- foreach
			- count
			- top
			- min
			- fold
		- action for PairRDDs
			- countByKeyApprox
			- keys
			- values
			- countByKey
			- sampleByKeyExact
	6 - Partition Operations: MapPartitions and PartitionBy
		scala> val badges_rdd_csv = sc.textFile("/user/cloudera/stackexchange/badges_csv")
		badges_rdd_csv: org.apache.spark.rdd.RDD[String] = /user/cloudera/stackexchange/badges_csv MapPartitionsRDD[1] at textFile at <console>:24

		scala> val badges_columns_rdd = badges_rdd_csv.map(x => x.split(","))
		badges_columns_rdd: org.apache.spark.rdd.RDD[Array[String]] = MapPartitionsRDD[2] at map at <console>:25

		scala> badges_columns_rdd.take(1)
		res0: Array[Array[String]] = Array(Array(1, 5, Autobiographer, 2015-02-03T16:41:10.170, 3, False))

		scala> val badges_for_part = badges_columns_rdd.map(x => (x(2), x.mkString(","))).
			 | repartition(50)
		badges_for_part: org.apache.spark.rdd.RDD[(String, String)] = MapPartitionsRDD[7] at repartition at <console>:26

		scala> badges_for_part.partitioner
		res1: Option[org.apache.spark.Partitioner] = None

		scala> import org.apache.spark.HashPartitioner
		import org.apache.spark.HashPartitioner

		scala> val badges_by_badge = badges_for_part.partitionBy(new HashPartitioner(50))
		badges_by_badge: org.apache.spark.rdd.RDD[(String, String)] = ShuffledRDD[8] at partitionBy at <console>:26

		scala> badges_by_badge.partitioner
		res2: Option[org.apache.spark.Partitioner] = Some(org.apache.spark.HashPartitioner@32))

		scala> badges_for_part.saveAsTextFile("/user/cloudera/stackexchange/badges_no_partitioner")

		scala> badges_by_badge.saveAsTextFile("/user/cloudera/stackexchange/badges_yes_partitioner")

		scala>
			
		Here, 	
			- badges_yes_partitioner
				- most of rows have the same key in a same partition
					hadoop@nodemaster:/$ hdfs dfs -cat /user/cloudera/stackexchange/badges_yes_partitioner/part-00049
					(Guru,29364,248,Guru,2018-08-29T10:45:48.000,2,False)
					(Guru,48115,865,Guru,2020-09-04T15:47:40.377,2,False)
					(Guru,28602,500,Guru,2018-07-22T12:28:09.233,2,False)
					(Guru,62021,1821,Guru,2022-03-07T11:35:40.333,2,False)
					(Proofreader,31945,15,Proofreader,2018-12-14T04:05:04.707,3,False)
					(Guru,34742,970,Guru,2019-04-23T20:50:15.910,2,False)
					(Guru,36413,51,Guru,2019-07-09T15:25:15.907,2,False)
					(Guru,36968,130,Guru,2019-08-02T18:45:01.207,2,False)
					(Guru,45784,5395,Guru,2020-05-13T18:57:00.200,2,False)
			- badges_no_partitioner
				- most of rows have different keys in a same partition
					hadoop@nodemaster:/tmp$ hdfs dfs -cat /user/cloudera/stackexchange/badges_no_partitioner/part-00049
					(Autobiographer,39,43,Autobiographer,2015-02-03T17:01:30.973,3,False)
					(Supporter,90,45,Supporter,2015-02-03T17:16:32.137,3,False)
					(Precognitive,142,19,Precognitive,2015-02-03T17:43:42.817,3,False)
					(Autobiographer,193,89,Autobiographer,2015-02-03T17:53:42.800,3,False)
					(Autobiographer,244,111,Autobiographer,2015-02-03T18:28:57.267,3,False)
					(Supporter,296,88,Supporter,2015-02-03T19:03:58.410,3,False)
					(Teacher,352,132,Teacher,2015-02-03T19:49:02.333,3,False)
					(Precognitive,403,156,Precognitive,2015-02-03T20:43:43.410,3,False)
					(Supporter,454,178,Supporter,2015-02-03T21:40:37.250,3,False)
					(Precognitive,504,188,Precognitive,2015-02-03T22:43:43.543,3,False)
					(Editor,554,73,Editor,2015-02-04T01:17:29.303,3,False)
					
		- glom
			- There is an action to coalesce all rows in a partition into an array
			- useful for operations on all items within a partition
			
			scala> badges_by_badge.map({ case (x, y) => x }).collect()
			res5: Array[String] = Array(substitute, Benefactor, Notable Question, Notable Question, Notable Question, Notable Question, Notable Question, Notable Question, Notable Question, Notable Question, Notable Question, Notable Question, Notable Question, Notable Question, Notable Question, Notable Question, Notable Question, Notable Question, Notable Question, Notable Question, Notable Question, Notable Question, Notable Question, Notable Question, Notable Question, Notable Question, Notable Question, Notable Question, Notable Question, Notable Question, Notable Question, Benefactor, Notable Question, Notable Question, Notable Question, Notable Question, Notable Question, Notable Question, Notable Question, Notable Question, Notable Question, Notable Question, Notab...

			scala> badges_by_badge.map({ case (x, y) => x }).glom().take(2)
			res6: Array[Array[String]] = Array(Array(substitute), Array(Benefactor, Notable Question, Notable Question, Notable Question, Notable Question, Notable Question, Notable Question, Notable Question, Notable Question, Notable Question, Notable Question, Notable Question, Notable Question, Notable Question, Notable Question, Notable Question, Notable Question, Notable Question, Notable Question, Notable Question, Notable Question, Notable Question, Notable Question, Notable Question, Notable Question, Notable Question, Notable Question, Notable Question, Notable Question, Notable Question, Benefactor, Notable Question, Notable Question, Notable Question, Notable Question, Notable Question, Notable Question, Notable Question, Notable Question, Notable Question, Not...

		
		- mapPartitions
			- Apply a function to each partition
			- Done at a single pass
			- Return after entire partition is processed

			scala> badges_by_badge.mapPartitions(x => Array(x.size).iterator, true).collect()
			res7: Array[Int] = Array(1, 1246, 0, 1, 0, 8082, 15473, 29, 0, 0, 198, 1209, 2487, 336, 1911, 2549, 304, 2, 47, 421, 11804, 0, 18, 599, 209, 5, 314, 0, 0, 0, 71, 0, 797, 1489, 837, 10, 196, 54, 3411, 20, 47, 585, 54, 3070, 96, 140, 300, 65, 22, 108)

			scala> badges_for_part.mapPartitions(x => Array(x.size).iterator, true).collect()
			res8: Array[Int] = Array(1171, 1171, 1171, 1171, 1171, 1171, 1171, 1171, 1171, 1171, 1171, 1172, 1172, 1173, 1173, 1173, 1173, 1173, 1173, 1173, 1173, 1173, 1173, 1173, 1173, 1173, 1173, 1173, 1173, 1173, 1173, 1173, 1173, 1173, 1173, 1173, 1173, 1173, 1173, 1173, 1173, 1173, 1173, 1172, 1172, 1172, 1172, 1172, 1171, 1171)

			scala>
	7 - Sampling Your Data
			- Sampleing Data
				- Selecting a representative part of the population
				- Faster, but you may lose accuracy
				- Also useful if you are resource constrained or your dataset is very large
				
				scala> val posts_all = sc.textFile("/user/cloudera/stackexchange/posts_all_csv")
				posts_all: org.apache.spark.rdd.RDD[String] = /user/cloudera/stackexchange/posts_all_csv MapPartitionsRDD[19] at textFile at <console>:25

				scala> posts_all.count
				res11: Long = 28750

				scala> val sample_posts = posts_all.sample(false, 0.1, 50)
				sample_posts: org.apache.spark.rdd.RDD[String] = PartitionwiseSampledRDD[20] at sample at <console>:26

				scala> sample_posts.count
				res12: Long = 2879

				scala>
			- Approximate Counts
				scala> posts_all.count
				res13: Long = 28750

				scala> posts_all.countApprox(100, 0.95)
				res14: org.apache.spark.partial.PartialResult[org.apache.spark.partial.BoundedDouble] = (final: [28750.000, 28750.000])

				scala>
			- Take a Sample of Exact Size
				scala> posts_all.takeSample(false, 15, 50)
				res15: Array[String] = Array(13040,2,"",2017-07-25T21:36:11.850,1,"",6590,"","","",2017-07-25T21:36:11.850,"","",1,"", 25071,1,"",2020-05-04T04:05:18.047,0,32,2021,"","",Unable to change foldmethod,2020-05-04T04:05:18.047,(folding),0,4,1, 14627,2,"",2017-12-22T00:35:03.583,5,"",2055,"","","",2017-12-22T00:35:03.583,"","",1,"", 20292,2,"",2019-06-11T17:21:59.783,0,"",18375,"","","",2019-06-11T17:21:59.783,"","",0,"", 5705,2,"",2015-12-03T16:03:50.613,1,"",5513,5864,2021-02-07T12:25:29.290,"",2021-02-07T12:25:29.290,"","",2,"", 9053,2,"",2016-08-02T20:16:12.073,3,"",71,2313,2016-08-03T05:50:03.230,"",2016-08-03T05:50:03.230,"","",2,"", 37173,2,"",2022-04-06T03:09:22.120,2,"",41342,"","","",2022-04-06T03:09:22.120,"","",0,"", 14098,2,"",2017-11-02T13:06:50.300,5,"...

				scala> posts_all.takeSample(false, 15, 50).size
				res16: Int = 15

				scala>
	8 - Set Operations: Join, Union, Full Right, Left Outer, and Cartesian
		scala> val questions = sc.parallelize(Array(("xavier", 1), ("troy", 2), ("xavier", 5)))
		questions: org.apache.spark.rdd.RDD[(String, Int)] = ParallelCollectionRDD[0] at parallelize at <console>:24

		scala> val answers = sc.parallelize(Array(("xavier", 3), ("beth", 4)))
		answers: org.apache.spark.rdd.RDD[(String, Int)] = ParallelCollectionRDD[1] at parallelize at <console>:24

		scala> questions.collect()
		res0: Array[(String, Int)] = Array((xavier,1), (troy,2), (xavier,5))

		scala> answers.collect()
		res1: Array[(String, Int)] = Array((xavier,3), (beth,4))

		scala> questions.union(answers).collect()
		res2: Array[(String, Int)] = Array((xavier,1), (troy,2), (xavier,5), (xavier,3), (beth,4))

		scala> questions.join(answers).collect()
		res3: Array[(String, (Int, Int))] = Array((xavier,(1,3)), (xavier,(5,3)))

		scala> questions.fullOuterJoin(answers).collect()
		res4: Array[(String, (Option[Int], Option[Int]))] = Array((troy,(Some(2),None)), (xavier,(Some(1),Some(3))), (xavier,(Some(5),Some(3))), (beth,(None,Some(4))))

		scala> questions.leftOuterJoin(answers).collect()
		res5: Array[(String, (Int, Option[Int]))] = Array((troy,(2,None)), (xavier,(1,Some(3))), (xavier,(5,Some(3))))

		scala> questions.rightOuterJoin(answers).collect()
		res6: Array[(String, (Option[Int], Int))] = Array((xavier,(Some(1),3)), (xavier,(Some(5),3)), (beth,(None,4)))

		scala> questions.cartesian(answers).collect()
		res7: Array[((String, Int), (String, Int))] = Array(((xavier,1),(xavier,3)), ((xavier,1),(beth,4)), ((troy,2),(xavier,3)), ((troy,2),(beth,4)), ((xavier,5),(xavier,3)), ((xavier,5),(beth,4)))

		scala>
	9 - Combining, Aggregating, Reducing, and Grouping on PairRDDs
		- Prepare Some Data
			- Extract user from each post
			- PairRDD
				- Key is user
				- Value is 1
				
			scala> val posts_all = sc.textFile("/user/cloudera/stackexchange/posts_all_csv")
			posts_all: org.apache.spark.rdd.RDD[String] = /user/cloudera/stackexchange/posts_all_csv MapPartitionsRDD[17] at textFile at <console>:24

			scala> posts_all.take(1)
			res9: Array[String] = Array(1,1,5,2015-02-03T16:40:26.487,45,10084,2,2,2015-02-03T17:51:07.583,How can I add line numbers to Vim?,2018-12-12T18:05:13.460,(line-numbers),2,0,8)

			scala> val each_post_owner = posts_all.map(x => x.split(",")(6))
			each_post_owner: org.apache.spark.rdd.RDD[String] = MapPartitionsRDD[18] at map at <console>:25

			scala> each_post_owner.collect()
			res10: Array[String] = Array(2, 5, 11, 12, 19, 2, 27, 19, 28, 27, 31, 24, 28, 2, 37, 28, 5, 31, 18, 37, 31, 53, 17, 16, 54, 37, 54, 59, 53, 15, 64, 51, 54, 65, 58, 15, 69, 53, 59, 29, 54, 2, "", 54, 15, 44, 78, 74, 53, 31, 74, 59, 51, 51, 2, 74, 82, 82, 81, 24, 53, 82, 74, 14, 88, 15, 31, 18, "", 74, 53, 44, 14, 51, 15, 53, 24, 92, 74, 53, 91, "", 91, 51, 93, 24, 74, 24, 53, 24, 24, 51, 93, 74, 88, 64, 88, 88, 88, 53, 53, "", 79, 88, 50, 51, 114, 124, 24, 24, 24, 13, 51, 131, 67, 51, 134, 135, 126, 132, 51, 31, 133, 114, 129, 126, 67, 135, 134, 142, 51, 51, 24, 20, 24, 142, 137, 41, 135, 142, 138, 150, 77, 18, 151, 93, 135, 153, 93, 142, 132, -1, 24, 141, 142, 25, 162, 161, 135, 24, 162, 54, 130, 132, 142, 171, 161, 142, 54, 24, 15, 132, 155, 179, 134, 69, 88, ...

			scala> val posts_owner_pair_rdd = each_post_owner.map(x => (x, 1))
			posts_owner_pair_rdd: org.apache.spark.rdd.RDD[(String, Int)] = MapPartitionsRDD[19] at map at <console>:25

			scala> posts_owner_pair_rdd.take(1)
			res11: Array[(String, Int)] = Array((2,1))

			scala>
		- Aggregate
			- GroupByKey (gbk)
			
				scala> val top_posters_gbk = posts_owner_pair_rdd.groupByKey()
				top_posters_gbk: org.apache.spark.rdd.RDD[(String, Iterable[Int])] = ShuffledRDD[20] at groupByKey at <console>:25

				scala> top_posters_gbk.take(10)
				res12: Array[(String, Iterable[Int])] = Array((20062,CompactBuffer(1)), (29386,CompactBuffer(1)), (20299,CompactBuffer(1)), (25717,CompactBuffer(1)), (19211,CompactBuffer(1, 1, 1, 1, 1)), (27513,CompactBuffer(1)), (37163,CompactBuffer(1)), (33541,CompactBuffer(1)), (37309,CompactBuffer(1)), (9760,CompactBuffer(1)))

				- Tuple of user id and list of 1's
				
					scala> top_posters_gbk.map({ case (x, y) => (x, y.toList) }).collect()
					res13: Array[(String, List[Int])] = Array((20062,List(1)), (29386,List(1)), (20299,List(1)), (25717,List(1)), (19211,List(1, 1, 1, 1, 1)), (27513,List(1)), (37163,List(1)), (33541,List(1)), (37309,List(1)), (9760,List(1)), (20864,List(1)), (40651,List(1)), (2277,List(1, 1, 1, 1, 1)), (9205,List(1)), (11372,List(1)), (11635,List(1, 1, 1, 1, 1, 1, 1, 1)), (7676,List(1)), (19497,List(1)), (9311,List(1)), (466,List(1, 1)), (10054,List(1)), (7559,List(1)), (16331,List(1)), (19550,List(1, 1)), (18498,List(1)), (7476,List(1, 1)), (12386,List(1)), (26637,List(1, 1)), (41458,List(1)), (30391,List(1)), (15158,List(1)), (178,List(1, 1, 1, 1, 1)), (1472,List(1, 1, 1, 1, 1, 1, 1, 1)), (7267,List(1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1...

				- Posts per user? -> User id and number of posts
				
					scala> top_posters_gbk.map({ case (x, y) => (x, y.size) }).collect()
					res14: Array[(String, Int)] = Array((20062,1), (29386,1), (20299,1), (25717,1), (19211,5), (27513,1), (37163,1), (33541,1), (37309,1), (9760,1), (20864,1), (40651,1), (2277,5), (9205,1), (11372,1), (11635,8), (7676,1), (19497,1), (9311,1), (466,2), (10054,1), (7559,1), (16331,1), (19550,2), (18498,1), (7476,2), (12386,1), (26637,2), (41458,1), (30391,1), (15158,1), (178,5), (1472,8), (7267,34), (8271,1), (2475,5), (22839,1), (3164,6), (31789,1), (35220,3), (34768,1), (5944,7), (19651,1), (5636,1), (4668,1), (8293,1), (23025,1), (26778,2), (32326,1), (18403,1), (31750,1), (25223,1), (7575,2), (21892,1), (5919,20), (35268,2), (1027,12), (7184,41), (10816,2), (22347,1), (8536,1), (31437,4), (38799,1), (5199,1), (17897,1), (41535,1), (23339,1), (25492,1), (38913,1)...

				- Use sortBy for top poster

					scala> top_posters_gbk.map({ case (x, y) => (x, y.size) }).
						 | sortBy({ case (x, y) => -y }).take(1)
					res16: Array[(String, Int)] = Array((18609,784))

					scala>
			
			- ReduceByKey (rbk)
			
				scala> val top_posters_rbk = posts_owner_pair_rdd.reduceByKey(_ + _)
				top_posters_rbk: org.apache.spark.rdd.RDD[(String, Int)] = ShuffledRDD[35] at reduceByKey at <console>:25

				scala> top_posters_rbk.lookup("51")
				res17: Seq[Int] = ArrayBuffer(570)

				// verify this gets the same result of sortBy from groupByKey above
				scala> top_posters_rbk.lookup("18609")
				res18: Seq[Int] = ArrayBuffer(784)

				scala>
			
			- AggregateByKey
			
				- Only questions, include score and user id
				
					// this is to create a pair RDD (using split() and map())
					scala> val posts_all_entries = posts_all.map(x => x.split(","))
					posts_all_entries: org.apache.spark.rdd.RDD[Array[String]] = MapPartitionsRDD[36] at map at <console>:25

					scala> val questions = posts_all_entries.filter(x => x(1) == "1")
					questions: org.apache.spark.rdd.RDD[Array[String]] = MapPartitionsRDD[37] at filter at <console>:25

					scala> val user_question_score = questions.map(x => (x(6), x(4).toInt))
					user_question_score: org.apache.spark.rdd.RDD[(String, Int)] = MapPartitionsRDD[38] at map at <console>:25

					scala> user_question_score.take(5).foreach(println)
					(2,45)
					(11,91)
					(12,42)
					(2,51)
					(28,22)

					scala>
				
				- Define initial value, merging function and combining function
				
					scala> var for_keeping_count = (0, 0)
					for_keeping_count: (Int, Int) = (0,0)

					scala> def combining (tuple_sum_count: (Int, Int), next_score: Int) =
						 | (tuple_sum_count._1 + next_score, tuple_sum_count._2 + 1)
					combining: (tuple_sum_count: (Int, Int), next_score: Int)(Int, Int)

					scala> def merging(tuple_sum_count: (Int, Int), tuple_next_partition_sum_count: (Int, Int)) =
						 | (tuple_sum_count._1 + tuple_next_partition_sum_count._1,
						 | tuple_sum_count._2 + tuple_next_partition_sum_count._2)
					merging: (tuple_sum_count: (Int, Int), tuple_next_partition_sum_count: (Int, Int))(Int, Int)

					scala> val aggregated_user_question =
						 | user_question_score.aggregateByKey(for_keeping_count)(combining, merging)
					aggregated_user_question: org.apache.spark.rdd.RDD[(String, (Int, Int))] = ShuffledRDD[39] at aggregateByKey at <console>:32

					scala> aggregated_user_question.take(1)
					res21: Array[(String, (Int, Int))] = Array((10942,(2,1)))

					scala> aggregated_user_question.lookup("51")
					res22: Seq[(Int, Int)] = ArrayBuffer((957,49))

					scala>
					
				- CombineByKey
				
					scala> def to_list(postId: Int): List[Int] = List(postId)
					to_list: (postId: Int)List[Int]

					scala> def merge_posts(postA: List[Int], postB: Int) = postB :: postA
					merge_posts: (postA: List[Int], postB: Int)List[Int]

					scala> def combine_posts(postA: List[Int], postB: List[Int]): List[Int] = postA ++ postB
					combine_posts: (postA: List[Int], postB: List[Int])List[Int]

					scala> val user_post = questions.map(x => (x(6), x(0).toInt))
					user_post: org.apache.spark.rdd.RDD[(String, Int)] = MapPartitionsRDD[40] at map at <console>:25

					scala> val combined = user_post.combineByKey(to_list, merge_posts, combine_posts)
					combined: org.apache.spark.rdd.RDD[(String, List[Int])] = ShuffledRDD[41] at combineByKey at <console>:31

					scala> combined.filter({ case (x, y) => x == "51"}).collect()
					res24: Array[(String, List[Int])] = Array((51,List(15857, 12770, 12325, 11476, 10817, 7116, 7014, 4148, 3710, 3697, 3355, 3180, 2521, 2428, 2155, 2119, 2115, 2108, 2044, 2003, 744, 627, 598, 588, 565, 564, 554, 538, 506, 434, 432, 373, 366, 364, 363, 338, 137, 126, 118, 88, 77, 56, 34, 29025, 24750, 20844, 20842, 19680, 19477)))

					scala>
					
				- CountByKey
				
					scala> user_post.countByKey()("51")
					res25: Long = 49

					scala>
					
	10 - ReduceByKey vs GroupByKey, which one is better?
		
		- groupByKey and reduceByKey get the similar result
		
		- but groupByKey needs to move / shuffle more data 
		
		- so reduceByKey is better
		
		- aggregateByKey, combineByKey are even better with their flexibility to take custom functions
		
	11 - Grouping Data into Buckets with Histogram
		
		scala> val badges_rdd_csv = sc.textFile("/user/cloudera/stackexchange/badges_csv")
		badges_rdd_csv: org.apache.spark.rdd.RDD[String] = /user/cloudera/stackexchange/badges_csv MapPartitionsRDD[46] at textFile at <console>:24

		scala> val badges_column_rdd = badges_rdd_csv.map(x => x.split(","))
		badges_column_rdd: org.apache.spark.rdd.RDD[Array[String]] = MapPartitionsRDD[47] at map at <console>:25

		scala> val badges_entry = badges_column_rdd.map(x => x(2))
		badges_entry: org.apache.spark.rdd.RDD[String] = MapPartitionsRDD[48] at map at <console>:25

		scala> badges_entry.take(1)
		res26: Array[String] = Array(Autobiographer)

		scala> val badges_name = badges_entry.map(x => (x, 1))
		badges_name: org.apache.spark.rdd.RDD[(String, Int)] = MapPartitionsRDD[49] at map at <console>:25

		scala> val badges_reduced = badges_name.reduceByKey(_ + _)
		badges_reduced: org.apache.spark.rdd.RDD[(String, Int)] = ShuffledRDD[50] at reduceByKey at <console>:25

		scala> badges_reduced.take(10)
		res27: Array[(String, Int)] = Array((Critic,288), (Tumbleweed,142), (Teacher,2437), (Electorate,11), (Citizen Patrol,194), (Scholar,3083), (Autobiographer,15401), (Booster,28), (Taxonomist,47), (Custodian,771))

		scala> badges_reduced.map({ case(x, y) => y }).histogram(7)
		res28: (Array[Double], Array[Long]) = (Array(1.0, 2201.0, 4401.0, 6601.0, 8801.0, 11001.0, 13201.0, 15401.0),Array(83, 6, 0, 0, 0, 1, 1))

		scala>

		scala> val intervals: Array[Double] = Array(0, 1000, 2000, 3000, 4000, 5000, 6000, 7000)
		intervals: Array[Double] = Array(0.0, 1000.0, 2000.0, 3000.0, 4000.0, 5000.0, 6000.0, 7000.0)

		scala> badges_reduced.map({ case(x, y) => y }).histogram(intervals)
		res29: Array[Long] = Array(80, 3, 4, 1, 1, 0, 0)

		scala> badges_reduced.sortBy(x => -x._2).take(10)
		res30: Array[(String, Int)] = Array((Autobiographer,15401), (Supporter,11804), (Student,4232), (Scholar,3083), (Editor,2981), (Popular Question,2726), (Yearling,2534), (Teacher,2437), (Informed,1909), (Nice Answer,1384))

		scala> badges_reduced.filter(x => x._2 < 1000).count()
		res31: Long = 80

		scala>
		
	12 - Cache & Data Persistence
		
		- call explicitly cache() and persist() when beneficial
		- cache() is equivalent to persist(MEMORY_ONLY)
		- StorageLevel
			MEMORY_ONLY
			MEMORY_AND_DISK
			MEMORY_ONLY_SER
			...
			
	13 - Shared Variable: Accumulators and Broadcast
			
		scala> val badges_rdd_csv = sc.textFile("/user/cloudera/stackexchange/badges_csv")
		badges_rdd_csv: org.apache.spark.rdd.RDD[String] = /user/cloudera/stackexchange/badges_csv MapPartitionsRDD[1] at textFile at <console>:24

		scala> val badges_columns_rdd = badges_rdd_csv.map(x => x.split(","))
		badges_columns_rdd: org.apache.spark.rdd.RDD[Array[String]] = MapPartitionsRDD[2] at map at <console>:25

		scala> val badges_for_part = badges_columns_rdd.map(x => (x(2), x.mkString(","))).
			 | repartition(50)
		badges_for_part: org.apache.spark.rdd.RDD[(String, String)] = MapPartitionsRDD[7] at repartition at <console>:26

		scala> badges_for_part.partitioner
		res0: Option[org.apache.spark.Partitioner] = None

		scala> import org.apache.spark.HashPartitioner
		import org.apache.spark.HashPartitioner

		scala> val badges_by_badge = badges_for_part.partitionBy(new HashPartitioner(50))
		badges_by_badge: org.apache.spark.rdd.RDD[(String, String)] = ShuffledRDD[8] at partitionBy at <console>:26

		scala> badges_by_badge.partitioner
		res1: Option[org.apache.spark.Partitioner] = Some(org.apache.spark.HashPartitioner@32)

		scala> val counted_badges = badges_by_badge.mapPartitions(x => Array(x.size).iterator, true)
		counted_badges: org.apache.spark.rdd.RDD[Int] = MapPartitionsRDD[9] at mapPartitions at <console>:26

		scala> counted_badges.collect()
		res2: Array[Int] = Array(1, 1246, 0, 1, 0, 8082, 15473, 29, 0, 0, 198, 1209, 2487, 336, 1911, 2549, 304, 2, 47, 421, 11804, 0, 18, 599, 209, 5, 314, 0, 0, 0, 71, 0, 797, 1489, 837, 10, 196, 54, 3411, 20, 47, 585, 54, 3070, 96, 140, 300, 65, 22, 108)

		scala> counted_badges.sum()
		res3: Double = 58617.0

		scala>
		
		scala> counted_badges.toDebugString
		res4: String =
		(50) MapPartitionsRDD[9] at mapPartitions at <console>:26 []
		 |   ShuffledRDD[8] at partitionBy at <console>:26 []
		 +-(50) MapPartitionsRDD[7] at repartition at <console>:26 []
			|   CoalescedRDD[6] at repartition at <console>:26 []
			|   ShuffledRDD[5] at repartition at <console>:26 []
			+-(2) MapPartitionsRDD[4] at repartition at <console>:26 []
			   |  MapPartitionsRDD[3] at map at <console>:25 []
			   |  MapPartitionsRDD[2] at map at <console>:25 []
			   |  /user/cloudera/stackexchange/badges_csv MapPartitionsRDD[1] at textFile at <console>:24 []
			   |  /user/cloudera/stackexchange/badges_csv HadoopRDD[0] at textFile at <console>:24 []

		scala>
		
		Accumulator
		
		scala> val accumulator_badge = sc.longAccumulator("Badge Accumulator")
		accumulator_badge: org.apache.spark.util.LongAccumulator = LongAccumulator(id: 100, name: Some(Badge Accumulator), value: 0)

		scala> def add_badge(item: (String, String)) = accumulator_badge.add(1)
		add_badge: (item: (String, String))Unit

		scala> badges_by_badge.foreach(add_badge)

		scala> accumulator_badge.value
		res6: Long = 58617

		scala>
		
		
		Broadcast Variable
		
		- changes from driver will be delivered to worker nodes, work nodes will also propagate the changes
		  to other work nodes
		
		scala> val users_all = sc.textFile("/user/cloudera/stackexchange/users_csv")
		users_all: org.apache.spark.rdd.RDD[String] = /user/cloudera/stackexchange/users_csv MapPartitionsRDD[12] at textFile at <console>:25

		scala> users_all.take(10)
		res8: Array[String] = Array(-1,1,2015-02-03T00:40:07.397,Community,2015-02-03T00:40:07.397,22,61,204,"",-1, 1,101,2015-02-03T16:33:52.997,Adam Lear,2021-06-30T23:29:34.707,106,0,0,"",37099, 2,2433,2015-02-03T16:37:39.043,Undo,2022-03-10T14:59:23.663,31,47,4,"",1703573, 3,101,2015-02-03T16:39:18.897,Laura,2015-02-03T16:39:18.897,12,0,0,"",463263, 4,101,2015-02-03T16:39:20.920,Taryn,2020-08-18T17:45:32.313,11,0,0,"",188123, 5,494,2015-02-03T16:40:06.483,Seth,2020-12-23T00:15:53.543,10,123,1,"",1208323, 6,101,2015-02-03T16:44:55.240,sharshi,2018-03-26T02:01:50.860,7,3,0,"",2455453, 7,101,2015-02-03T16:45:10.393,casey,2022-05-17T18:39:12.527,7,2,0,"",3672166, 8,1868,2015-02-03T16:47:15.203,Gilles 'SO- stop being evil',2022-03-14T09:14:08.053,44,443,10,"",164368, 9,...

		scala> val users_columns = users_all.map(x => x.split(","))
		users_columns: org.apache.spark.rdd.RDD[Array[String]] = MapPartitionsRDD[13] at map at <console>:26

		scala> users_columns.take(3)
		res9: Array[Array[String]] = Array(Array(-1, 1, 2015-02-03T00:40:07.397, Community, 2015-02-03T00:40:07.397, 22, 61, 204, "", -1), Array(1, 101, 2015-02-03T16:33:52.997, Adam Lear, 2021-06-30T23:29:34.707, 106, 0, 0, "", 37099), Array(2, 2433, 2015-02-03T16:37:39.043, Undo, 2022-03-10T14:59:23.663, 31, 47, 4, "", 1703573))

		scala>
		
		scala> val posts_all = sc.textFile("/user/cloudera/stackexchange/posts_all_csv")
		posts_all: org.apache.spark.rdd.RDD[String] = /user/cloudera/stackexchange/posts_all_csv MapPartitionsRDD[15] at textFile at <console>:25

		scala> val each_post_owner = posts_all.map(x => x.split(",")(6))
		each_post_owner: org.apache.spark.rdd.RDD[String] = MapPartitionsRDD[16] at map at <console>:26

		scala> val posts_owner_pair_rdd = each_post_owner.map(x => (x, 1))
		posts_owner_pair_rdd: org.apache.spark.rdd.RDD[(String, Int)] = MapPartitionsRDD[17] at map at <console>:26

		scala> val posters_gbk = posts_owner_pair_rdd.groupByKey()
		posters_gbk: org.apache.spark.rdd.RDD[(String, Iterable[Int])] = ShuffledRDD[18] at groupByKey at <console>:26

		scala> val top_posters_gbk = posts_owner_pair_rdd.groupByKey()
		top_posters_gbk: org.apache.spark.rdd.RDD[(String, Iterable[Int])] = ShuffledRDD[19] at groupByKey at <console>:26

		scala> val top_posters_rbk = posts_owner_pair_rdd.reduceByKey(_ + _)
		top_posters_rbk: org.apache.spark.rdd.RDD[(String, Int)] = ShuffledRDD[20] at reduceByKey at <console>:26

		scala> top_posters_rbk.take(10)
		res10: Array[(String, Int)] = Array((33815,1), (29386,1), (20299,1), (25717,1), (19211,5), (27513,1), (37163,1), (33541,1), (37309,1), (9760,1))

		scala> top_posters_rbk.lookup("51")
		res11: Seq[Int] = ArrayBuffer(570)

		scala> val tp = top_posters_rbk.collectAsMap()
		tp: scala.collection.Map[String,Int] = Map(29936 -> 1, 20542 -> 2, 16136 -> 1, 30921 -> 1, 590 -> 4, 10667 -> 2, 11231 -> 1, 39046 -> 4, 30120 -> 8, 12085 -> 5, 8655 -> 1, 20911 -> 1, 13797 -> 1, 17501 -> 1, 15080 -> 11, 11896 -> 2, 12460 -> 4, 24926 -> 1, 23261 -> 2, 32426 -> 1, 3114 -> 2, 27100 -> 1, 26536 -> 4, 5525 -> 1, 33094 -> 1, 20806 -> 1, 35370 -> 1, 29480 -> 5, 16340 -> 1, 2874 -> 1, 28806 -> 1, 36915 -> 1, 22416 -> 1, 37640 -> 1, 37088 -> 1, 38504 -> 1, 5890 -> 13, 6754 -> 2, 6640 -> 1, 30510 -> 2, 9723 -> 1, 9915 -> 1, 8364 -> 1, 29369 -> 2, 10580 -> 1, 12964 -> 1, 5899 -> 1, 683 -> 3, 11303 -> 1, 8868 -> 2, 20464 -> 1, 12163 -> 3, 188 -> 18, 14574 -> 2, 869 -> 1, 22734 -> 1, 10589 -> 2, 15318 -> 2, 1057 -> 1, 21369 -> 2, 10448 -> 4, 11204 -> 1, 25...

		scala> val broadcast_tp = sc.broadcast(tp)
		broadcast_tp: org.apache.spark.broadcast.Broadcast[scala.collection.Map[String,Int]] = Broadcast(14)

		scala>
		
		scala> def get_name(user_column: Array[String]) = {
			 | val user_id = user_column(0)
			 | val user_name = user_column(3)
			 | var user_post_count = "0"
			 | if (broadcast_tp.value.keySet.exists(_ == user_id))
			 |    user_post_count = broadcast_tp.value(user_id).toString
			 | (user_id, user_name, user_post_count)
			 | }
		get_name: (user_column: Array[String])(String, String, String)

		scala> val user_info = users_columns.map(get_name)
		user_info: org.apache.spark.rdd.RDD[(String, String, String)] = MapPartitionsRDD[21] at map at <console>:28

		scala> user_info.take(10)
		res12: Array[(String, String, String)] = Array((-1,Community,64), (1,Adam Lear,0), (2,Undo,9), (3,Laura,0), (4,Taryn,0), (5,Seth,2), (6,sharshi,0), (7,casey,0), (8,Gilles 'SO- stop being evil',20), (9,Kevin,3))

		scala>
		
8 - Increasing Proficiency with Spark: DataFrames and Spark SQL
		
	D:\aaa\workout\techs\bigdata\a_courses\pluralsight\2_develop_spark_app_with_scala_&_cloudera\spark-scala-cloudera\demos\8 - Increasing Proficiency with Spark - DataFrames and Spark SQL\increasing_proficiency_dataframes_spark_sql.scala
	
	
	1 - Increasing Proficiency with Spark: DataFrames and Spark SQL
		- RDD (Resilient Distributed Dataset) is lower level API		
		- DataFrame is higher level API
		
	2 - "Everyone" Uses SQL and How It All Begun
		- Spark SQL
		- SchemaRDD
			- DataFrame API
			
	3 - Hello DataFrames & Spark SQL
		- Spark SQL
			- works with Hive
		- DataFrame
			- Distributed collection of Row objects
				- Dataset[Row]
			- Equivalent to a database table
				- Rows and columns
				- Known schema
			- Structured or unstructured data
				- Conversion to and from RDD possible
			- Queries
				- Domain Specific Language (DSL)
				- Relational
				- Allows for optimizations
				
	4 - SparkSession: The Entry Point to Spark SQL / DataFrame API
		- Entry point to Spark SQL
		- Merges SQLContext, HiveContext
		- SparkContext
		
	5 - Creating DataFrame
		scala> val qa_listDF = spark.createDataFrame(Array((1, "Xavier"), (2, "Irene"), (3, "Xavier")))
		qa_listDF: org.apache.spark.sql.DataFrame = [_1: int, _2: string]

		scala> qa_listDF.rdd.collect()
		res0: Array[org.apache.spark.sql.Row] = Array([1,Xavier], [2,Irene], [3,Xavier])
		
		scala> qa_listDF.collect()
		res10: Array[org.apache.spark.sql.Row] = Array([1,Xavier], [2,Irene], [3,Xavier])

		scala> qa_listDF.take(3)
		res1: Array[org.apache.spark.sql.Row] = Array([1,Xavier], [2,Irene], [3,Xavier])

		scala> qa_listDF.show()
		+---+------+
		| _1|    _2|
		+---+------+
		|  1|Xavier|
		|  2| Irene|
		|  3|Xavier|
		+---+------+

		scala> qa_listDF.limit(1)
		res3: org.apache.spark.sql.Dataset[org.apache.spark.sql.Row] = [_1: int, _2: string]

		scala> qa_listDF.limit(1).show(1)
		+---+------+
		| _1|    _2|
		+---+------+
		|  1|Xavier|
		+---+------+


		scala> qa_listDF.head()
		res5: org.apache.spark.sql.Row = [1,Xavier]

		scala> qa_listDF.first()
		res7: org.apache.spark.sql.Row = [1,Xavier]

		scala> qa_listDF.take(1)
		res8: Array[org.apache.spark.sql.Row] = Array([1,Xavier])

		scala> qa_listDF.sample(false, .3, 42).collect()
		res9: Array[org.apache.spark.sql.Row] = Array()
		
		scala> qa_listDF.sample(false, .5, 42).collect()
		res17: Array[org.apache.spark.sql.Row] = Array()

		scala> qa_listDF.sample(false, .6, 42).collect()
		res18: Array[org.apache.spark.sql.Row] = Array()

		scala> qa_listDF.sample(false, .7, 42).collect()
		res19: Array[org.apache.spark.sql.Row] = Array([1,Xavier])

		scala> qa_listDF.sample(false, .8, 42).collect()
		res20: Array[org.apache.spark.sql.Row] = Array([1,Xavier], [3,Xavier])

		scala> qa_listDF.sample(false, .9, 42).collect()
		res21: Array[org.apache.spark.sql.Row] = Array([1,Xavier], [2,Irene], [3,Xavier])
		
		scala> qa_listDF.show()
		+---+------+
		| _1|    _2|
		+---+------+
		|  1|Xavier|
		|  2| Irene|
		|  3|Xavier|
		+---+------+

		scala>
		
		- give some meaningful label / name
			
		scala> val qaDF = qa_listDF.toDF("Id", "QA")
		qaDF: org.apache.spark.sql.DataFrame = [Id: int, QA: string]

		scala> qaDF.show()
		+---+------+
		| Id|    QA|
		+---+------+
		|  1|Xavier|
		|  2| Irene|
		|  3|Xavier|
		+---+------+

		scala>
	
	6 - DataFrames to RDDs and Vice Versa
		scala> val qa_rdd = sc.parallelize(Array((1, "Xavier"), (2, "Irene"), (3, "Xavier")))
		qa_rdd: org.apache.spark.rdd.RDD[(Int, String)] = ParallelCollectionRDD[41] at parallelize at <console>:24

		scala> val qa_with_ToDF = qa_rdd.toDF()
		qa_with_ToDF: org.apache.spark.sql.DataFrame = [_1: int, _2: string]

		scala> val qa_with_create = spark.createDataFrame(qa_rdd)
		qa_with_create: org.apache.spark.sql.DataFrame = [_1: int, _2: string]

		scala> qa_rdd.collect()
		res24: Array[(Int, String)] = Array((1,Xavier), (2,Irene), (3,Xavier))

		scala> qa_with_create.collect()
		res25: Array[org.apache.spark.sql.Row] = Array([1,Xavier], [2,Irene], [3,Xavier])

		scala> qa_with_ToDF.collect()
		res26: Array[org.apache.spark.sql.Row] = Array([1,Xavier], [2,Irene], [3,Xavier])

		scala> qa_with_ToDF.show()
		+---+------+
		| _1|    _2|
		+---+------+
		|  1|Xavier|
		|  2| Irene|
		|  3|Xavier|
		+---+------+


		scala> qa_with_create.show()
		+---+------+
		| _1|    _2|
		+---+------+
		|  1|Xavier|
		|  2| Irene|
		|  3|Xavier|
		+---+------+


		scala> qa_with_ToDF.rdd.collect()
		res30: Array[org.apache.spark.sql.Row] = Array([1,Xavier], [2,Irene], [3,Xavier])

		scala> qa_with_ToDF.rdd.map(x => (x(0), x(1))).collect()
		res31: Array[(Any, Any)] = Array((1,Xavier), (2,Irene), (3,Xavier))

		scala>
		
		scala> val badges_rdd_csv = sc.textFile("/user/cloudera/stackexchange/badges_csv")
		badges_rdd_csv: org.apache.spark.rdd.RDD[String] = /user/cloudera/stackexchange/badges_csv MapPartitionsRDD[61] at textFile at <console>:24

		scala> val badges_columns_rdd = badges_rdd_csv.map(x => x.split(","))
		badges_columns_rdd: org.apache.spark.rdd.RDD[Array[String]] = MapPartitionsRDD[62] at map at <console>:25

		scala> badges_columns_rdd.take(3)
		res32: Array[Array[String]] = Array(Array(1, 5, Autobiographer, 2015-02-03T16:41:10.170, 3, False), Array(2, 4, Autobiographer, 2015-02-03T16:41:10.170, 3, False), Array(3, 3, Autobiographer, 2015-02-03T16:41:10.170, 3, False))

		scala> val badges_from_rddDF = badges_columns_rdd.toDF()
		badges_from_rddDF: org.apache.spark.sql.DataFrame = [value: array<string>]

		scala> badges_from_rddDF.show(3)
		+--------------------+
		|               value|
		+--------------------+
		|[1, 5, Autobiogra...|
		|[2, 4, Autobiogra...|
		|[3, 3, Autobiogra...|
		+--------------------+
		only showing top 3 rows


		scala> val badges_tuple = badges_columns_rdd.map(x => (x(0), x(1), x(2), x(3), x(4), x(5))).toDF()
		badges_tuple: org.apache.spark.sql.DataFrame = [_1: string, _2: string ... 4 more fields]

		scala> badges_tuple.show(3)
		+---+---+--------------+--------------------+---+-----+
		| _1| _2|            _3|                  _4| _5|   _6|
		+---+---+--------------+--------------------+---+-----+
		|  1|  5|Autobiographer|2015-02-03T16:41:...|  3|False|
		|  2|  4|Autobiographer|2015-02-03T16:41:...|  3|False|
		|  3|  3|Autobiographer|2015-02-03T16:41:...|  3|False|
		+---+---+--------------+--------------------+---+-----+
		only showing top 3 rows


		scala> badges_tuple.printSchema
		root
		 |-- _1: string (nullable = true)
		 |-- _2: string (nullable = true)
		 |-- _3: string (nullable = true)
		 |-- _4: string (nullable = true)
		 |-- _5: string (nullable = true)
		 |-- _6: string (nullable = true)


		scala>
		
		Here, all columns are string types.
		
	7 - Loading DataFrames: Text and CSV
	
		scala> val posts_no_schemaTxtDF = spark.read.format("text").
			 | load("/user/cloudera/stackexchange/posts_all_csv")
		posts_no_schemaTxtDF: org.apache.spark.sql.DataFrame = [value: string]

		scala> posts_no_schemaTxtDF.printSchema
		root
		 |-- value: string (nullable = true)


		scala> posts_no_schemaTxtDF.show(3)
		+--------------------+
		|               value|
		+--------------------+
		|1,1,5,2015-02-03T...|
		|2,2,"",2015-02-03...|
		|3,1,8,2015-02-03T...|
		+--------------------+
		only showing top 3 rows


		scala> posts_no_schemaTxtDF.show(3, truncate=false)
		+--------------------------------------------------------------------------------------------------------------------------------------------------------+
		|value                                                                                                                                                   |
		+--------------------------------------------------------------------------------------------------------------------------------------------------------+
		|1,1,5,2015-02-03T16:40:26.487,45,10084,2,2,2015-02-03T17:51:07.583,How can I add line numbers to Vim?,2018-12-12T18:05:13.460,(line-numbers),2,0,8      |
		|2,2,"",2015-02-03T16:43:11.760,24,"",5,1109,2018-12-12T18:05:13.460,"",2018-12-12T18:05:13.460,"","",1,""                                               |
		|3,1,8,2015-02-03T16:54:26.737,91,45278,11,28,2015-02-03T16:55:58.233,How can I show relative line numbers?,2017-11-20T16:51:43.517,(line-numbers),4,0,16|
		+--------------------------------------------------------------------------------------------------------------------------------------------------------+
		only showing top 3 rows


		scala>
		
		scala> val posts_no_schemaTxtDF = spark.read.text("/user/cloudera/stackexchange/posts_all_csv")
		posts_no_schemaTxtDF: org.apache.spark.sql.DataFrame = [value: string]

		scala> posts_no_schemaTxtDF.printSchema
		root
		 |-- value: string (nullable = true)


		scala> posts_no_schemaTxtDF.show(1)
		+--------------------+
		|               value|
		+--------------------+
		|1,1,5,2015-02-03T...|
		+--------------------+
		only showing top 1 row


		scala
		
		scala> val posts_no_schemaCSV = spark.read.csv("/user/cloudera/stackexchange/posts_all_csv")
		posts_no_schemaCSV: org.apache.spark.sql.DataFrame = [_c0: string, _c1: string ... 13 more fields]

		scala> posts_no_schemaCSV.show(3)
		+---+---+----+--------------------+---+-----+---+----+--------------------+--------------------+--------------------+--------------+----+----+----+
		|_c0|_c1| _c2|                 _c3|_c4|  _c5|_c6| _c7|                 _c8|                 _c9|                _c10|          _c11|_c12|_c13|_c14|
		+---+---+----+--------------------+---+-----+---+----+--------------------+--------------------+--------------------+--------------+----+----+----+
		|  1|  1|   5|2015-02-03T16:40:...| 45|10084|  2|   2|2015-02-03T17:51:...|How can I add lin...|2018-12-12T18:05:...|(line-numbers)|   2|   0|   8|
		|  2|  2|null|2015-02-03T16:43:...| 24| null|  5|1109|2018-12-12T18:05:...|                null|2018-12-12T18:05:...|          null|null|   1|null|
		|  3|  1|   8|2015-02-03T16:54:...| 91|45278| 11|  28|2015-02-03T16:55:...|How can I show re...|2017-11-20T16:51:...|(line-numbers)|   4|   0|  16|
		+---+---+----+--------------------+---+-----+---+----+--------------------+--------------------+--------------------+--------------+----+----+----+
		only showing top 3 rows


	8 - Schemas: Inferred and Programatically Specified + Option
		
		scala> val posts_inferred = spark.read.option("inferSchema", "true").
			 | csv("/user/cloudera/stackexchange/posts_all_csv")
		posts_inferred: org.apache.spark.sql.DataFrame = [_c0: int, _c1: int ... 13 more fields]

		scala> posts_inferred.printSchema
		root
		 |-- _c0: integer (nullable = true)
		 |-- _c1: integer (nullable = true)
		 |-- _c2: integer (nullable = true)
		 |-- _c3: timestamp (nullable = true)
		 |-- _c4: integer (nullable = true)
		 |-- _c5: integer (nullable = true)
		 |-- _c6: integer (nullable = true)
		 |-- _c7: integer (nullable = true)
		 |-- _c8: timestamp (nullable = true)
		 |-- _c9: string (nullable = true)
		 |-- _c10: timestamp (nullable = true)
		 |-- _c11: string (nullable = true)
		 |-- _c12: integer (nullable = true)
		 |-- _c13: integer (nullable = true)
		 |-- _c14: integer (nullable = true)


		scala>
		
		Here, with "inferSchema", each column has its own data type.  But the headers are still _c0, _c1, _c2, ...
		
		
		RDD is lazy
		BUT DataFrame read ahead
		
		scala> val add_rdd = sc.textFile("/thisdoesnotexist")
		add_rdd: org.apache.spark.rdd.RDD[String] = /thisdoesnotexist MapPartitionsRDD[116] at textFile at <console>:24

		scala> val a_df = spark.read.text("/thisdoesnotexist")
		org.apache.spark.sql.AnalysisException: Path does not exist: hdfs://localhost:9000/thisdoesnotexist;
		  at org.apache.spark.sql.execution.datasources.DataSource$.$anonfun$checkAndGlobPathIfNecessary$1(DataSource.scala:759)
		  at scala.collection.TraversableLike.$anonfun$flatMap$1(TraversableLike.scala:245)
		  at scala.collection.immutable.List.foreach(List.scala:392)
		  at scala.collection.TraversableLike.flatMap(TraversableLike.scala:245)
		  at scala.collection.TraversableLike.flatMap$(TraversableLike.scala:242)
		  at scala.collection.immutable.List.flatMap(List.scala:355)
		  at org.apache.spark.sql.execution.datasources.DataSource$.checkAndGlobPathIfNecessary(DataSource.scala:746)
		  at org.apache.spark.sql.execution.datasources.DataSource.checkAndGlobPathIfNecessary(DataSource.scala:575)
		  at org.apache.spark.sql.execution.datasources.DataSource.resolveRelation(DataSource.scala:398)
		  at org.apache.spark.sql.DataFrameReader.loadV1Source(DataFrameReader.scala:279)
		  at org.apache.spark.sql.DataFrameReader.$anonfun$load$2(DataFrameReader.scala:268)
		  at scala.Option.getOrElse(Option.scala:189)
		  at org.apache.spark.sql.DataFrameReader.load(DataFrameReader.scala:268)
		  at org.apache.spark.sql.DataFrameReader.text(DataFrameReader.scala:825)
		  at org.apache.spark.sql.DataFrameReader.text(DataFrameReader.scala:791)
		  ... 47 elided

		scala>
		
		chaining options
		
		scala> spark.read.option("inferSchema", "true").csv("/user/cloudera/stackexchange/posts_all_csv").printSchema
		root
		 |-- _c0: integer (nullable = true)
		 |-- _c1: integer (nullable = true)
		 |-- _c2: integer (nullable = true)
		 |-- _c3: timestamp (nullable = true)
		 |-- _c4: integer (nullable = true)
		 |-- _c5: integer (nullable = true)
		 |-- _c6: integer (nullable = true)
		 |-- _c7: integer (nullable = true)
		 |-- _c8: timestamp (nullable = true)
		 |-- _c9: string (nullable = true)
		 |-- _c10: timestamp (nullable = true)
		 |-- _c11: string (nullable = true)
		 |-- _c12: integer (nullable = true)
		 |-- _c13: integer (nullable = true)
		 |-- _c14: integer (nullable = true)


		scala> spark.read.option("inferSchema", "true").option("sep", "|").csv("/user/cloudera/stackexchange/posts_all_csv").pri
		ntSchema
		root
		 |-- _c0: string (nullable = true)


		scala> spark.read.options(Map("inferSchema" -> "true", "sep" -> "|")).csv("/user/cloudera/stackexchange/posts_all_csv").
		printSchema
		root
		 |-- _c0: string (nullable = true)


		scala>
		
		
		
		In another terminal,
		
		hadoop@nodemaster:/tmp/demo$ ls
		badges   comments            commiters_rdd.scala  posts_header         spark-committers-no-header.tsv  tags
		badgesc  commiters_df.scala  posts                posts_simple_titles  spark-committers.tsv            users
		hadoop@nodemaster:/tmp/demo$ cd posts_header
		hadoop@nodemaster:/tmp/demo/posts_header$ ls
		build.sbt  src
		hadoop@nodemaster:/tmp/demo/posts_header$ sbt package
		[info] welcome to sbt 1.6.2 (Private Build Java 1.8.0_312)
		[info] loading project definition from /tmp/demo/posts_header/project
		[info] loading settings for project posts_header from build.sbt ...
		[info] set current project to Posts Header Project (in build file:/tmp/demo/posts_header/)
		[info] compiling 1 Scala source to /tmp/demo/posts_header/target/scala-2.11/classes ...
		[success] Total time: 14 s, completed Sep 2, 2022 5:32:49 AM
		hadoop@nodemaster:/tmp/demo/posts_header$
		
		hadoop@nodemaster:/tmp/demo/posts_header$ spark-submit --class PreparePostsHeaderCSVApp target/scala-2.11/posts-header-p
roject_2.11-1.0.jar



		back to spark-shell
		
		scala> val posts_headersDF = spark.read.option("inferSchema", "true").option("header", "true").
			 | csv("/user/cloudera/stackexchange/posts_all_csv_with_header")
		posts_headersDF: org.apache.spark.sql.DataFrame = [Id: int, PostTypeId: int ... 13 more fields]

		scala> posts_headersDF.printSchema
		root
		 |-- Id: integer (nullable = true)
		 |-- PostTypeId: integer (nullable = true)
		 |-- AcceptedAnswerId: integer (nullable = true)
		 |-- CreationDate: timestamp (nullable = true)
		 |-- Score: integer (nullable = true)
		 |-- ViewCount: integer (nullable = true)
		 |-- OwnerUserId: integer (nullable = true)
		 |-- LastEditorUserId: integer (nullable = true)
		 |-- LastEditDate: timestamp (nullable = true)
		 |-- Title: string (nullable = true)
		 |-- LastActivityDate: timestamp (nullable = true)
		 |-- Tags: string (nullable = true)
		 |-- AnswerCount: integer (nullable = true)
		 |-- CommentCount: integer (nullable = true)
		 |-- FavoriteCount: integer (nullable = true)


		scala>
		
		Now, header is showing up.
		
		
		
		scala> import org.apache.spark.sql.types._
		import org.apache.spark.sql.types._

		scala> val postsSchema =
			 |   StructType(Array(
			 |               StructField("Id", IntegerType),
			 | StructField("PostTypeId", IntegerType),
			 | StructField("AcceptedAnswerId", IntegerType),
			 | StructField("CreationDate", TimestampType),
			 | StructField("Score", IntegerType),
			 | StructField("ViewCount", IntegerType),
			 | StructField("OwnerUserId", IntegerType),
			 | StructField("LastEditorUserId", IntegerType),
			 | StructField("LastEditDate", TimestampType),
			 | StructField("Title", StringType),
			 | StructField("LastActivityDate", TimestampType),
			 | StructField("Tags", StringType),
			 | StructField("AnswerCount", IntegerType),
			 | StructField("CommentCount", IntegerType),
			 | StructField("FavoriteCount", IntegerType)))
		postsSchema: org.apache.spark.sql.types.StructType = StructType(StructField(Id,IntegerType,true), StructField(PostTypeId,IntegerType,true), StructField(AcceptedAnswerId,IntegerType,true), StructField(CreationDate,TimestampType,true), StructField(Score,IntegerType,true), StructField(ViewCount,IntegerType,true), StructField(OwnerUserId,IntegerType,true), StructField(LastEditorUserId,IntegerType,true), StructField(LastEditDate,TimestampType,true), StructField(Title,StringType,true), StructField(LastActivityDate,TimestampType,true), StructField(Tags,StringType,true), StructField(AnswerCount,IntegerType,true), StructField(CommentCount,IntegerType,true), StructField(FavoriteCount,IntegerType,true))

		scala> val postsDF = spark.read.schema(postsSchema).
			 | csv("/user/cloudera/stackexchange/posts_all_csv")
		postsDF: org.apache.spark.sql.DataFrame = [Id: int, PostTypeId: int ... 13 more fields]

		scala> postsDF.printSchema
		root
		 |-- Id: integer (nullable = true)
		 |-- PostTypeId: integer (nullable = true)
		 |-- AcceptedAnswerId: integer (nullable = true)
		 |-- CreationDate: timestamp (nullable = true)
		 |-- Score: integer (nullable = true)
		 |-- ViewCount: integer (nullable = true)
		 |-- OwnerUserId: integer (nullable = true)
		 |-- LastEditorUserId: integer (nullable = true)
		 |-- LastEditDate: timestamp (nullable = true)
		 |-- Title: string (nullable = true)
		 |-- LastActivityDate: timestamp (nullable = true)
		 |-- Tags: string (nullable = true)
		 |-- AnswerCount: integer (nullable = true)
		 |-- CommentCount: integer (nullable = true)
		 |-- FavoriteCount: integer (nullable = true)


		scala> postsDF.schema
		res51: org.apache.spark.sql.types.StructType = StructType(StructField(Id,IntegerType,true), StructField(PostTypeId,IntegerType,true), StructField(AcceptedAnswerId,IntegerType,true), StructField(CreationDate,TimestampType,true), StructField(Score,IntegerType,true), StructField(ViewCount,IntegerType,true), StructField(OwnerUserId,IntegerType,true), StructField(LastEditorUserId,IntegerType,true), StructField(LastEditDate,TimestampType,true), StructField(Title,StringType,true), StructField(LastActivityDate,TimestampType,true), StructField(Tags,StringType,true), StructField(AnswerCount,IntegerType,true), StructField(CommentCount,IntegerType,true), StructField(FavoriteCount,IntegerType,true))

		scala> postsDF.dtypes
		res52: Array[(String, String)] = Array((Id,IntegerType), (PostTypeId,IntegerType), (AcceptedAnswerId,IntegerType), (CreationDate,TimestampType), (Score,IntegerType), (ViewCount,IntegerType), (OwnerUserId,IntegerType), (LastEditorUserId,IntegerType), (LastEditDate,TimestampType), (Title,StringType), (LastActivityDate,TimestampType), (Tags,StringType), (AnswerCount,IntegerType), (CommentCount,IntegerType), (FavoriteCount,IntegerType))

		scala> postsDF.columns
		res53: Array[String] = Array(Id, PostTypeId, AcceptedAnswerId, CreationDate, Score, ViewCount, OwnerUserId, LastEditorUserId, LastEditDate, Title, LastActivityDate, Tags, AnswerCount, CommentCount, FavoriteCount)

		scala>
		
	9 - More Data Loading: Parquet and JSON
		
		Parquet is the default file format for DataFrame.
		
		In another terminal,
		
		hadoop@nodemaster:/tmp/demo$ cd comments
		hadoop@nodemaster:/tmp/demo/comments$ ls
		build.sbt  src
		hadoop@nodemaster:/tmp/demo/comments$ sbt package
		[info] Updated file /tmp/demo/comments/project/build.properties: set sbt.version to 1.6.2
		[info] welcome to sbt 1.6.2 (Private Build Java 1.8.0_312)
		[info] loading project definition from /tmp/demo/comments/project
		[info] loading settings for project comments from build.sbt ...
		[info] set current project to Comments Project (in build file:/tmp/demo/comments/)
		[info] compiling 1 Scala source to /tmp/demo/comments/target/scala-2.11/classes ...
		[success] Total time: 13 s, completed Sep 2, 2022 6:02:48 AM
		hadoop@nodemaster:/tmp/demo/comments$ spark-submit --class PrepareCommentsParquetApp target/scala-2.11/comments-project_2.11-1.0.jar
		
		
		
		back to the original terminal,
		
		scala> val comments_parquetDF = spark.read.
			 | parquet("/user/cloudera/stackexchange/comments_parquet")
		comments_parquetDF: org.apache.spark.sql.DataFrame = [Id: int, PostId: int ... 3 more fields]

		scala> comments_parquetDF.printSchema
		root
		 |-- Id: integer (nullable = true)
		 |-- PostId: integer (nullable = true)
		 |-- Score: integer (nullable = true)
		 |-- CreationDate: string (nullable = true)
		 |-- UserId: integer (nullable = true)


		scala> comments_parquetDF.show(5)
		+---+------+-----+--------------------+------+
		| Id|PostId|Score|        CreationDate|UserId|
		+---+------+-----+--------------------+------+
		|  1|     7|    2|2015-02-03T16:59:...|    11|
		|  2|    10|    2|2015-02-03T17:01:...|     5|
		|  3|     7|    0|2015-02-03T17:05:...|    27|
		|  4|    19|    1|2015-02-03T17:11:...|    46|
		|  6|    13|    2|2015-02-03T17:18:...|    58|
		+---+------+-----+--------------------+------+
		only showing top 5 rows


		scala>
		
		
		Try with JSON
		
		go to another terminal,
		
		hadoop@nodemaster:/tmp/demo$ ls
		badges   comments            commiters_rdd.scala  posts_header         spark-committers-no-header.tsv  tags
		badgesc  commiters_df.scala  posts                posts_simple_titles  spark-committers.tsv            users
		hadoop@nodemaster:/tmp/demo$ cd tags
		hadoop@nodemaster:/tmp/demo/tags$ ls
		build.sbt  src
		hadoop@nodemaster:/tmp/demo/tags$ sbt package
		[info] Updated file /tmp/demo/tags/project/build.properties: set sbt.version to 1.6.2
		[info] welcome to sbt 1.6.2 (Private Build Java 1.8.0_312)
		[info] loading project definition from /tmp/demo/tags/project
		[info] loading settings for project tags from build.sbt ...
		[info] set current project to Tags Project (in build file:/tmp/demo/tags/)
		[info] compiling 1 Scala source to /tmp/demo/tags/target/scala-2.11/classes ...
		[success] Total time: 13 s, completed Sep 2, 2022 6:17:19 AM
		hadoop@nodemaster:/tmp/demo/tags$ spark-submit --class "PrepareTagsJSONApp" target/scala-2.11/tags-project_2.11-1.0.jar
		
		
		
		back to original terminal
		
		scala> val tags_json = spark.read.json("/user/cloudera/stackexchange/tags_json")
		tags_json: org.apache.spark.sql.DataFrame = [Count: string, ExcerptPostId: string ... 3 more fields]

		scala> tags_json.printSchema
		root
		 |-- Count: string (nullable = true)
		 |-- ExcerptPostId: string (nullable = true)
		 |-- Id: string (nullable = true)
		 |-- TagName: string (nullable = true)
		 |-- WikiPostId: string (nullable = true)


		scala> tags_json.show(5)
		+-----+-------------+---+---------------+----------+
		|Count|ExcerptPostId| Id|        TagName|WikiPostId|
		+-----+-------------+---+---------------+----------+
		|   81|         4450|  1|   line-numbers|      4449|
		|  338|         3239|  2|    indentation|      3238|
		|  164|          856|  6|          macro|       855|
		|   31|         6549|  7|text-generation|      6548|
		|  443|         4216| 12|         search|      4215|
		+-----+-------------+---+---------------+----------+
		only showing top 5 rows


		scala>
		
		JSON Lines
		
		each row of table is a JSON file
		
	10 - Rows, Columns, Expressions, and Operators
		
		scala> import org.apache.spark.sql.types._
		import org.apache.spark.sql.types._

		scala> val postsSchema =
			 |   StructType(Array(
			 |               StructField("Id", IntegerType),
			 | StructField("PostTypeId", IntegerType),
			 | StructField("AcceptedAnswerId", IntegerType),
			 | StructField("CreationDate", TimestampType),
			 | StructField("Score", IntegerType),
			 | StructField("ViewCount", IntegerType),
			 | StructField("OwnerUserId", IntegerType),
			 | StructField("LastEditorUserId", IntegerType),
			 | StructField("LastEditDate", TimestampType),
			 | StructField("Title", StringType),
			 | StructField("LastActivityDate", TimestampType),
			 | StructField("Tags", StringType),
			 | StructField("AnswerCount", IntegerType),
			 | StructField("CommentCount", IntegerType),
			 | StructField("FavoriteCount", IntegerType)))
		postsSchema: org.apache.spark.sql.types.StructType = StructType(StructField(Id,IntegerType,true), StructField(PostTypeId,IntegerType,true), StructField(AcceptedAnswerId,IntegerType,true), StructField(CreationDate,TimestampType,true), StructField(Score,IntegerType,true), StructField(ViewCount,IntegerType,true), StructField(OwnerUserId,IntegerType,true), StructField(LastEditorUserId,IntegerType,true), StructField(LastEditDate,TimestampType,true), StructField(Title,StringType,true), StructField(LastActivityDate,TimestampType,true), StructField(Tags,StringType,true), StructField(AnswerCount,IntegerType,true), StructField(CommentCount,IntegerType,true), StructField(FavoriteCount,IntegerType,true))

		scala> val postsDF = spark.read.schema(postsSchema).
			 | csv("/user/cloudera/stackexchange/posts_all_csv")
		postsDF: org.apache.spark.sql.DataFrame = [Id: int, PostTypeId: int ... 13 more fields]
		
		
		scala> postsDF.take(1)
		res58: Array[org.apache.spark.sql.Row] = Array([1,1,5,2015-02-03 16:40:26.487,45,10084,2,2,2015-02-03 17:51:07.583,How can I add line numbers to Vim?,2018-12-12 18:05:13.46,(line-numbers),2,0,8])

		scala> postsDF.show()
		+---+----------+----------------+--------------------+-----+---------+-----------+----------------+--------------------+--------------------+--------------------+--------------------+-----------+------------+-------------+
		| Id|PostTypeId|AcceptedAnswerId|        CreationDate|Score|ViewCount|OwnerUserId|LastEditorUserId|        LastEditDate|               Title|    LastActivityDate|                Tags|AnswerCount|CommentCount|FavoriteCount|
		+---+----------+----------------+--------------------+-----+---------+-----------+----------------+--------------------+--------------------+--------------------+--------------------+-----------+------------+-------------+
		|  1|         1|               5|2015-02-03 16:40:...|   45|    10084|          2|               2|2015-02-03 17:51:...|How can I add lin...|2018-12-12 18:05:...|      (line-numbers)|          2|           0|            8|
		|  2|         2|            null|2015-02-03 16:43:...|   24|     null|          5|            1109|2018-12-12 18:05:...|                null|2018-12-12 18:05:...|                null|       null|           1|         null|
		|  3|         1|               8|2015-02-03 16:54:...|   91|    45278|         11|              28|2015-02-03 16:55:...|How can I show re...|2017-11-20 16:51:...|      (line-numbers)|          4|           0|           16|
		|  4|         1|              43|2015-02-03 16:54:...|   42|    14206|         12|           21062|2019-08-06 22:20:...|How can I change ...|2019-08-06 22:20:...|(indentation,file...|          4|           1|            9|
		|  5|         2|            null|2015-02-03 16:54:...|   58|     null|         19|             135|2015-02-03 21:05:...|                null|2015-02-03 21:05:...|                null|       null|           2|         null|
		|  6|         1|            null|2015-02-03 16:55:...|   51|    15456|          2|              24|2015-02-05 08:44:...|How can I use the...|2020-11-25 17:31:...|(persistent-state...|          1|           1|           13|
		|  7|         2|            null|2015-02-03 16:56:...|    5|     null|         27|            null|                null|                null|2015-02-03 16:56:...|                null|       null|           7|         null|
		|  8|         2|            null|2015-02-03 16:58:...|  110|     null|         19|              -1|2017-04-13 12:51:...|                null|2015-02-03 21:58:...|                null|       null|           4|         null|
		|  9|         1|              23|2015-02-03 16:58:...|   22|     1360|         28|             343|2016-02-10 10:55:...|Can I script Vim ...|2016-02-10 10:55:...|  (vimscript-python)|          2|           1|            2|
		| 10|         2|            null|2015-02-03 16:59:...|   12|     null|         27|              27|2015-02-03 17:06:...|                null|2015-02-03 17:06:...|                null|       null|           1|         null|
		| 11|         2|            null|2015-02-03 17:00:...|   14|     null|         31|            null|                null|                null|2015-02-03 17:00:...|                null|       null|           0|         null|
		| 12|         1|              14|2015-02-03 17:00:...|   40|     5532|         24|            null|                null|How can I generat...|2016-12-07 20:47:...|(line-numbers,tex...|          5|           0|            7|
		| 13|         1|            2561|2015-02-03 17:01:...|   14|      932|         28|              28|2015-02-11 09:12:...|Does any solution...|2021-09-23 14:27:...|     (input-devices)|          2|           8|            2|
		| 14|         2|            null|2015-02-03 17:02:...|   56|     null|          2|              -1|2017-04-13 12:51:...|                null|2015-02-03 17:41:...|                null|       null|           1|         null|
		| 15|         2|            null|2015-02-03 17:03:...|    7|     null|         37|            null|                null|                null|2015-02-03 17:03:...|                null|       null|           0|         null|
		| 16|         1|              47|2015-02-03 17:03:...|    9|      150|         28|              33|2015-02-03 17:47:...|Can I use some fi...|2015-02-03 21:07:...|        (filesystem)|          4|           0|         null|
		| 17|         1|            null|2015-02-03 17:05:...|   15|     2956|          5|            null|                null|How can I search ...|2019-10-03 12:00:...|(line-numbers,sea...|          4|           0|            1|
		| 18|         2|            null|2015-02-03 17:05:...|   20|     null|         31|            null|                null|                null|2015-02-03 17:05:...|                null|       null|           1|         null|
		| 19|         1|              33|2015-02-03 17:05:...|   30|     1354|         18|            1292|2018-08-17 20:19:...|Why can ci&quot; ...|2020-10-05 02:33:...|(cursor-motions,c...|          4|           1|            5|
		| 20|         2|            null|2015-02-03 17:05:...|    4|     null|         37|            null|                null|                null|2015-02-03 17:05:...|                null|       null|           0|         null|
		+---+----------+----------------+--------------------+-----+---------+-----------+----------------+--------------------+--------------------+--------------------+--------------------+-----------+------------+-------------+
		only showing top 20 rows


		scala>
		
		
		Catalyst Expression
			- produces a value per row
			- based on a value, function or operation
			
	11 - Working with Columns
		
		scala> postsDF("Title")
		res60: org.apache.spark.sql.Column = Title

		scala> $"Title"
		res61: org.apache.spark.sql.ColumnName = Title


		scala> col("Title")
		res66: org.apache.spark.sql.Column = Title

		scala>
		
		scala> postsDF.select(postsDF("Title"))
		res62: org.apache.spark.sql.DataFrame = [Title: string]

		scala> postsDF.select(postsDF("Title")).show(1)
		+--------------------+
		|               Title|
		+--------------------+
		|How can I add lin...|
		+--------------------+
		only showing top 1 row


		scala> postsDF.select($"Title").show(1)
		+--------------------+
		|               Title|
		+--------------------+
		|How can I add lin...|
		+--------------------+
		only showing top 1 row


		scala> postsDF.select(col("Title")).show(1)
		+--------------------+
		|               Title|
		+--------------------+
		|How can I add lin...|
		+--------------------+
		only showing top 1 row

		scala>
		
		scala> postsDF.select($"Title", postsDF("Id"), col("CreationDate")).show(1)
		+--------------------+---+--------------------+
		|               Title| Id|        CreationDate|
		+--------------------+---+--------------------+
		|How can I add lin...|  1|2015-02-03 16:40:...|
		+--------------------+---+--------------------+
		only showing top 1 row


		scala> postsDF.select($"Score" * 1000).show(1)
		+--------------+
		|(Score * 1000)|
		+--------------+
		|         45000|
		+--------------+
		only showing top 1 row


		scala>
		
	12 - More Columns, Expressions, Cloning, Renaming, Casting, Dropping
		
		NOTE:
			- Every such operations will generate a new DataFrame, original DataFrame will remain unchanged!
		
		scala> postsDF.dtypes
		res70: Array[(String, String)] = Array((Id,IntegerType), (PostTypeId,IntegerType), (AcceptedAnswerId,IntegerType), (CreationDate,TimestampType), (Score,IntegerType), (ViewCount,IntegerType), (OwnerUserId,IntegerType), (LastEditorUserId,IntegerType), (LastEditDate,TimestampType), (Title,StringType), (LastActivityDate,TimestampType), (Tags,StringType), (AnswerCount,IntegerType), (CommentCount,IntegerType), (FavoriteCount,IntegerType))

		scala> postsDF.dtypes.find(x => x._1 == "ViewCount")
		res71: Option[(String, String)] = Some((ViewCount,IntegerType))

		scala>
		
		scala> postsDF.schema("ViewCount")
		res72: org.apache.spark.sql.types.StructField = StructField(ViewCount,IntegerType,true)

		scala> postsDF.select("ViewCount").printSchema
		root
		 |-- ViewCount: integer (nullable = true)

		scala>
		
		
		
		- Use withColumn() and cast
		
		scala> val posts_viewDF = postsDF.withColumn("ViewCount", postsDF("ViewCount").cast("String"))
		posts_viewDF: org.apache.spark.sql.DataFrame = [Id: int, PostTypeId: int ... 13 more fields]
		
		scala> posts_viewDF.printSchema
		root
		 |-- Id: integer (nullable = true)
		 |-- PostTypeId: integer (nullable = true)
		 |-- AcceptedAnswerId: integer (nullable = true)
		 |-- CreationDate: timestamp (nullable = true)
		 |-- Score: integer (nullable = true)
		 |-- ViewCount: string (nullable = true)
		 |-- OwnerUserId: integer (nullable = true)
		 |-- LastEditorUserId: integer (nullable = true)
		 |-- LastEditDate: timestamp (nullable = true)
		 |-- Title: string (nullable = true)
		 |-- LastActivityDate: timestamp (nullable = true)
		 |-- Tags: string (nullable = true)
		 |-- AnswerCount: integer (nullable = true)
		 |-- CommentCount: integer (nullable = true)
		 |-- FavoriteCount: integer (nullable = true)


		scala>
		
		
		- rename column
		
		scala> val postsVCDF = postsDF.withColumnRenamed("ViewCount", "ViewCountStr")
		postsVCDF: org.apache.spark.sql.DataFrame = [Id: int, PostTypeId: int ... 13 more fields]

		scala> postsVCDF.printSchema
		root
		 |-- Id: integer (nullable = true)
		 |-- PostTypeId: integer (nullable = true)
		 |-- AcceptedAnswerId: integer (nullable = true)
		 |-- CreationDate: timestamp (nullable = true)
		 |-- Score: integer (nullable = true)
		 |-- ViewCountStr: integer (nullable = true)
		 |-- OwnerUserId: integer (nullable = true)
		 |-- LastEditorUserId: integer (nullable = true)
		 |-- LastEditDate: timestamp (nullable = true)
		 |-- Title: string (nullable = true)
		 |-- LastActivityDate: timestamp (nullable = true)
		 |-- Tags: string (nullable = true)
		 |-- AnswerCount: integer (nullable = true)
		 |-- CommentCount: integer (nullable = true)
		 |-- FavoriteCount: integer (nullable = true)


		scala> val posts_twoDF = postsDF.withColumnRenamed("ViewCount", "ViewCountStr").
			 | withColumnRenamed("Score", "ScoreInt")
		posts_twoDF: org.apache.spark.sql.DataFrame = [Id: int, PostTypeId: int ... 13 more fields]

		scala> posts_twoDF.printSchema
		root
		 |-- Id: integer (nullable = true)
		 |-- PostTypeId: integer (nullable = true)
		 |-- AcceptedAnswerId: integer (nullable = true)
		 |-- CreationDate: timestamp (nullable = true)
		 |-- ScoreInt: integer (nullable = true)
		 |-- ViewCountStr: integer (nullable = true)
		 |-- OwnerUserId: integer (nullable = true)
		 |-- LastEditorUserId: integer (nullable = true)
		 |-- LastEditDate: timestamp (nullable = true)
		 |-- Title: string (nullable = true)
		 |-- LastActivityDate: timestamp (nullable = true)
		 |-- Tags: string (nullable = true)
		 |-- AnswerCount: integer (nullable = true)
		 |-- CommentCount: integer (nullable = true)
		 |-- FavoriteCount: integer (nullable = true)
		 
		 
		 

		- clone column

		scala> val posts_wcDF = postsDF.withColumn("TitleClone1", $"Title")
		posts_wcDF: org.apache.spark.sql.DataFrame = [Id: int, PostTypeId: int ... 14 more fields]

		scala> posts_wcDF.printSchema
		root
		 |-- Id: integer (nullable = true)
		 |-- PostTypeId: integer (nullable = true)
		 |-- AcceptedAnswerId: integer (nullable = true)
		 |-- CreationDate: timestamp (nullable = true)
		 |-- Score: integer (nullable = true)
		 |-- ViewCount: integer (nullable = true)
		 |-- OwnerUserId: integer (nullable = true)
		 |-- LastEditorUserId: integer (nullable = true)
		 |-- LastEditDate: timestamp (nullable = true)
		 |-- Title: string (nullable = true)
		 |-- LastActivityDate: timestamp (nullable = true)
		 |-- Tags: string (nullable = true)
		 |-- AnswerCount: integer (nullable = true)
		 |-- CommentCount: integer (nullable = true)
		 |-- FavoriteCount: integer (nullable = true)
		 |-- TitleClone1: string (nullable = true)


		scala> postsDF.withColumn("Title", concat(lit("Title: "), $"Title")).select($"Title").show(5)
		+--------------------+
		|               Title|
		+--------------------+
		|Title: How can I ...|
		|                null|
		|Title: How can I ...|
		|Title: How can I ...|
		|                null|
		+--------------------+
		only showing top 5 rows


		scala>
		
		scala> postsDF.withColumn("Title", lower($"Title")).select("Title").show(5)
		+--------------------+
		|               Title|
		+--------------------+
		|how can i add lin...|
		|                null|
		|how can i show re...|
		|how can i change ...|
		|                null|
		+--------------------+
		only showing top 5 rows


		scala>
		
		
		- drop column
		
		scala> posts_wcDF.columns
		res83: Array[String] = Array(Id, PostTypeId, AcceptedAnswerId, CreationDate, Score, ViewCount, OwnerUserId, LastEditorUserId, LastEditDate, Title, LastActivityDate, Tags, AnswerCount, CommentCount, FavoriteCount, TitleClone1)

		scala> posts_wcDF.columns.contains("TitleClone1")
		res84: Boolean = true

		scala> val posts_no_cloneDF = posts_wcDF.drop("TitleClone1")
		posts_no_cloneDF: org.apache.spark.sql.DataFrame = [Id: int, PostTypeId: int ... 13 more fields]

		scala> posts_no_cloneDF.columns.contains("TitleClone1")
		res85: Boolean = false

		scala> posts_no_cloneDF.printSchema
		root
		 |-- Id: integer (nullable = true)
		 |-- PostTypeId: integer (nullable = true)
		 |-- AcceptedAnswerId: integer (nullable = true)
		 |-- CreationDate: timestamp (nullable = true)
		 |-- Score: integer (nullable = true)
		 |-- ViewCount: integer (nullable = true)
		 |-- OwnerUserId: integer (nullable = true)
		 |-- LastEditorUserId: integer (nullable = true)
		 |-- LastEditDate: timestamp (nullable = true)
		 |-- Title: string (nullable = true)
		 |-- LastActivityDate: timestamp (nullable = true)
		 |-- Tags: string (nullable = true)
		 |-- AnswerCount: integer (nullable = true)
		 |-- CommentCount: integer (nullable = true)
		 |-- FavoriteCount: integer (nullable = true)


		scala>
		
	13 - User Defined Functions (UDFs) on Spark SQL
		
		scala> val questionsDF = postsDF.filter(col("PostTypeId") === 1)
		questionsDF: org.apache.spark.sql.Dataset[org.apache.spark.sql.Row] = [Id: int, PostTypeId: int ... 13 more fields]

		scala> questionsDF.select("Tags").show(3, truncate=false)
		+----------------------+
		|Tags                  |
		+----------------------+
		|(line-numbers)        |
		|(line-numbers)        |
		|(indentation,filetype)|
		+----------------------+
		only showing top 3 rows


		scala>
		
		scala> questionsDF.select("Tags").take(1)(0)(0).getClass
		res88: Class[_] = class java.lang.String
		
		

		- User Defined Functions (UDF)
			- create function and register the UDF
			
		scala> def give_me_list(str_lst: String): List[String] = {
			 | if (str_lst == "()" || Option(str_lst).getOrElse("").isEmpty || str_lst.length < 2) return List[String]()
			 | val elements = str_lst.slice(1, str_lst.length - 1)
			 | elements.split(",").toList
			 | }
		give_me_list: (str_lst: String)List[String]

		scala> val list_in_string = "(indentation,line_numbers)"
		list_in_string: String = (indentation,line_numbers)

		scala> give_me_list(list_in_string)
		res90: List[String] = List(indentation, line_numbers)

		scala> import org.apache.spark.sql.functions.udf
		import org.apache.spark.sql.functions.udf

		scala> val udf_give_me_list = udf(give_me_list _)
		udf_give_me_list: org.apache.spark.sql.expressions.UserDefinedFunction = SparkUserDefinedFunction($Lambda$4226/1566614930@10d1337b,ArrayType(StringType,true),List(Some(class[value[0]: string])),None,true,true)

		scala> val questions_id_tagsDF = questionsDF.withColumn("Tags",
			 | udf_give_me_list(questionsDF("Tags"))).select("Id", "Tags")
		questions_id_tagsDF: org.apache.spark.sql.DataFrame = [Id: int, Tags: array<string>]

		scala> questions_id_tagsDF.printSchema
		root
		 |-- Id: integer (nullable = true)
		 |-- Tags: array (nullable = true)
		 |    |-- element: string (containsNull = true)


		scala> questions_id_tagsDF.select("Tags").show(5, truncate=false)
		+-----------------------------+
		|Tags                         |
		+-----------------------------+
		|[line-numbers]               |
		|[line-numbers]               |
		|[indentation, filetype]      |
		|[persistent-state, undo-redo]|
		|[vimscript-python]           |
		+-----------------------------+
		only showing top 5 rows


		scala>
		
		- Distinct Tags
			- with explode(), you get one entry per item on the array
			
		scala> import org.apache.spark.sql.functions.explode
		import org.apache.spark.sql.functions.explode

		scala> questions_id_tagsDF.select(explode(questions_id_tagsDF("Tags"))).show(10)
		+----------------+
		|             col|
		+----------------+
		|    line-numbers|
		|    line-numbers|
		|     indentation|
		|        filetype|
		|persistent-state|
		|       undo-redo|
		|vimscript-python|
		|    line-numbers|
		| text-generation|
		|   input-devices|
		+----------------+
		only showing top 10 rows


		scala> questions_id_tagsDF.select(explode(questions_id_tagsDF("Tags"))).count()
		res95: Long = 24455

		scala> questions_id_tagsDF.select(explode(questions_id_tagsDF("Tags"))).distinct.count()
		res96: Long = 416

		scala>
		
		
		
9 - Continuing the Journey on DataFrames on Spark SQL
		
	D:\aaa\workout\techs\bigdata\a_courses\pluralsight\2_develop_spark_app_with_scala_&_cloudera\spark-scala-cloudera\demos\9 - Continuing the Journey on DataFrames and Spark SQL\continuing-journey-dataframes-spark-sql.scala
		
		
	1 - Querying, Sorting, and Filtering DataFrames: The DSL
		- DSL
			- Data Specific Language
		
		- Querying
			
			SQL						|		DataFrame
			
			select * from posts		|		postsDF.show()
									|		postsDF.select(*).show()
			
			select id from posts	|		postsDF.select("Id").show()
			
			select id from posts	|		postsDF.select("Id")
			where id = 1			|		.where(col("Id") === 1).show()
			
			
			
		scala> postsDF.select("Id").show(5)
		+---+
		| Id|
		+---+
		|  1|
		|  2|
		|  3|
		|  4|
		|  5|
		+---+
		only showing top 5 rows


		scala> postsDF.select("Id").limit(5).show()
		+---+
		| Id|
		+---+
		|  1|
		|  2|
		|  3|
		|  4|
		|  5|
		+---+


		scala> postsDF.select("Id", "Title").show(5)
		+---+--------------------+
		| Id|               Title|
		+---+--------------------+
		|  1|How can I add lin...|
		|  2|                null|
		|  3|How can I show re...|
		|  4|How can I change ...|
		|  5|                null|
		+---+--------------------+
		only showing top 5 rows


		scala> postsDF.select(col("Id"), postedDF("Title"), $"Score").show(1)
		<console>:31: error: not found: value postedDF
			   postsDF.select(col("Id"), postedDF("Title"), $"Score").show(1)
										 ^

		scala> postsDF.select(col("Id"), postsDF("Title"), $"Score").show(1)
		+---+--------------------+-----+
		| Id|               Title|Score|
		+---+--------------------+-----+
		|  1|How can I add lin...|   45|
		+---+--------------------+-----+
		only showing top 1 row


		scala> postsDF.select(col("Id"), postsDF("Title"), $"Score" * 1000).show(1)
		+---+--------------------+--------------+
		| Id|               Title|(Score * 1000)|
		+---+--------------------+--------------+
		|  1|How can I add lin...|         45000|
		+---+--------------------+--------------+
		only showing top 1 row


		scala> postsDF.filter(col("PostTypeId") === 1).count()
		res103: Long = 12307

		scala> postsDF.where(col("PostTypeId") === 1).count()
		res104: Long = 12307

		scala> postsDF.select(col("PostTypeId")).distinct().show()
		+----------+
		|PostTypeId|
		+----------+
		|         1|
		|         6|
		|         5|
		|         4|
		|         7|
		|         2|
		+----------+


		scala>
		
		scala> postsDF.select("Id", "PostTypeId").show(10)
		+---+----------+
		| Id|PostTypeId|
		+---+----------+
		|  1|         1|
		|  2|         2|
		|  3|         1|
		|  4|         1|
		|  5|         2|
		|  6|         1|
		|  7|         2|
		|  8|         2|
		|  9|         1|
		| 10|         2|
		+---+----------+
		only showing top 10 rows
		
		
		- nicer way
		
		scala> postsDF.select($"Id", when($"PostTypeId" === 1, "Question").
			 | otherwise("Other"), $"Title").show(5)
		+---+-------------------------------------------------------+--------------------+
		| Id|CASE WHEN (PostTypeId = 1) THEN Question ELSE Other END|               Title|
		+---+-------------------------------------------------------+--------------------+
		|  1|                                               Question|How can I add lin...|
		|  2|                                                  Other|                null|
		|  3|                                               Question|How can I show re...|
		|  4|                                               Question|How can I change ...|
		|  5|                                                  Other|                null|
		+---+-------------------------------------------------------+--------------------+
		only showing top 5 rows


		scala>
		
		scala> postsDF.select($"Id", when($"PostTypeId" === 1, "Question").
			 | when($"PostTypeId" === 2, "Answer").
			 | otherwise("Other").alias("PostType"), $"Title").show(100)
		+---+--------+--------------------+
		| Id|PostType|               Title|
		+---+--------+--------------------+
		|  1|Question|How can I add lin...|
		|  2|  Answer|                null|
		|  3|Question|How can I show re...|
		|  4|Question|How can I change ...|
		|  5|  Answer|                null|
		|  6|Question|How can I use the...|
		|  7|  Answer|                null|
		|  8|  Answer|                null|
		|  9|Question|Can I script Vim ...|
		| 10|  Answer|                null|
		+---+--------+--------------------+
		only showing top 100 rows


		scala>
		
		scala> postsDF.where(($"PostTypeID" === 5) || ($"PostTypeId" === 1)).count()
		res109: Long = 12547

		scala> postsDF.where(($"PostTypeID" === 5) && ($"PostTypeId" === 1)).count()
		res110: Long = 0

		scala> val qDF = questionsDF.select("Title", "AnswerCount", "Score")
		qDF: org.apache.spark.sql.DataFrame = [Title: string, AnswerCount: int ... 1 more field]

		scala> qDF.orderBy("AnswerCount").show(5)
		+--------------------+-----------+-----+
		|               Title|AnswerCount|Score|
		+--------------------+-----------+-----+
		|Resources for scr...|          0|    6|
		|How can I work ar...|          0|    3|
		|Return to previou...|          0|    4|
		|How do I make Vim...|          0|    1|
		|do a command with...|          0|    1|
		+--------------------+-----------+-----+
		only showing top 5 rows


		scala> qDF.orderBy($"AnswerCount".desc).show(5)
		+--------------------+-----------+-----+
		|               Title|AnswerCount|Score|
		+--------------------+-----------+-----+
		|How do I delete a...|         16|  113|
		|Other ways to exi...|         16|  126|
		|How to &quot;full...|         15|   50|
		|How can I inspire...|         14|   39|
		|How can I see the...|         13|  221|
		+--------------------+-----------+-----+
		only showing top 5 rows


		scala> qDF.sort($"AnswerCount".desc, $"Score".asc).show(5)
		+--------------------+-----------+-----+
		|               Title|AnswerCount|Score|
		+--------------------+-----------+-----+
		|How do I delete a...|         16|  113|
		|Other ways to exi...|         16|  126|
		|How to &quot;full...|         15|   50|
		|How can I inspire...|         14|   39|
		|How to insert a n...|         13|   61|
		+--------------------+-----------+-----+
		only showing top 5 rows


		scala>
		
	2 - What to Do with Missing or Corrupted Data
		
		- Removing
		
			- na.drop()
			
		
		scala> postsDF.select("Id", "Title").show(10)
		+---+--------------------+
		| Id|               Title|
		+---+--------------------+
		|  1|How can I add lin...|
		|  2|                null|
		|  3|How can I show re...|
		|  4|How can I change ...|
		|  5|                null|
		|  6|How can I use the...|
		|  7|                null|
		|  8|                null|
		|  9|Can I script Vim ...|
		| 10|                null|
		+---+--------------------+
		only showing top 10 rows


		scala> postsDF.select("Id", "Title").na.drop().show(10)
		+---+--------------------+
		| Id|               Title|
		+---+--------------------+
		|  1|How can I add lin...|
		|  3|How can I show re...|
		|  4|How can I change ...|
		|  6|How can I use the...|
		|  9|Can I script Vim ...|
		| 12|How can I generat...|
		| 13|Does any solution...|
		| 16|Can I use some fi...|
		| 17|How can I search ...|
		| 19|Why can ci&quot; ...|
		+---+--------------------+
		only showing top 10 rows


		scala> postsDF.select("Id", "Title").na.drop("any").show(10)
		+---+--------------------+
		| Id|               Title|
		+---+--------------------+
		|  1|How can I add lin...|
		|  3|How can I show re...|
		|  4|How can I change ...|
		|  6|How can I use the...|
		|  9|Can I script Vim ...|
		| 12|How can I generat...|
		| 13|Does any solution...|
		| 16|Can I use some fi...|
		| 17|How can I search ...|
		| 19|Why can ci&quot; ...|
		+---+--------------------+
		only showing top 10 rows


		scala> postsDF.select("Id", "Title").na.drop("all").show(10)
		+---+--------------------+
		| Id|               Title|
		+---+--------------------+
		|  1|How can I add lin...|
		|  2|                null|
		|  3|How can I show re...|
		|  4|How can I change ...|
		|  5|                null|
		|  6|How can I use the...|
		|  7|                null|
		|  8|                null|
		|  9|Can I script Vim ...|
		| 10|                null|
		+---+--------------------+
		only showing top 10 rows
		
		- "all" means ALL columns are null
		  So there is no effect because Id is not null


		scala> postsDF.select("Id", "Title").na.drop("any", Seq("Title")).show(10)
		+---+--------------------+
		| Id|               Title|
		+---+--------------------+
		|  1|How can I add lin...|
		|  3|How can I show re...|
		|  4|How can I change ...|
		|  6|How can I use the...|
		|  9|Can I script Vim ...|
		| 12|How can I generat...|
		| 13|Does any solution...|
		| 16|Can I use some fi...|
		| 17|How can I search ...|
		| 19|Why can ci&quot; ...|
		+---+--------------------+
		only showing top 10 rows


		scala>
		
		- Replacing & Filling
	
			- na.replace()
			- na.fill()
			
		scala> postsDF.select("Id", "Title").show(5)
		+---+--------------------+
		| Id|               Title|
		+---+--------------------+
		|  1|How can I add lin...|
		|  2|                null|
		|  3|How can I show re...|
		|  4|How can I change ...|
		|  5|                null|
		+---+--------------------+
		only showing top 5 rows


		scala> postsDF.select("Id", "Title").na.replace(Seq("Title"),
			 | Map("How can I add line numbers to Vim?" -> "[Redacted]")).show(5)
		+---+--------------------+
		| Id|               Title|
		+---+--------------------+
		|  1|          [Redacted]|
		|  2|                null|
		|  3|How can I show re...|
		|  4|How can I change ...|
		|  5|                null|
		+---+--------------------+
		only showing top 5 rows


		scala> postsDF.select("Id", "Title").na.fill("[NA]").show(5)
		+---+--------------------+
		| Id|               Title|
		+---+--------------------+
		|  1|How can I add lin...|
		|  2|                [NA]|
		|  3|How can I show re...|
		|  4|How can I change ...|
		|  5|                [NA]|
		+---+--------------------+
		only showing top 5 rows


		scala>
		
		- corrupted data fix
		
			- need to use Hue
		
	3 - Saving DataFrames
		
		- save - no options
		
		scala> postsDF.write.save("/user/cloudera/stackexchange/dataframes/just_save")

		scala> postsDF.rdd.getNumPartitions
		res126: Int = 1

		scala>
		
		scala> postsDF.repartition(2).write.save("/user/cloudera/stackexchange/dataframes/two_partitions")

		scala>
		
		scala> postsDF.select("Title").write.format("text").
			 | save("/user/cloudera/stackexchange/dataframes/just_text")

		scala>
		
		scala> postsDF.write.format("csv").save("/user/cloudera/stackexchange/dataframes/format_csv")

		scala> postsDF.write.csv("/user/cloudera/stackexchange/dataframes/just_csv")

		scala> postsDF.write.option("header", "true").
			 | csv("/user/cloudera/stackexchange/dataframes/csv_h")

		scala> postsDF.rdd.saveAsTextFile("/user/cloudera/stackexchange/dataframes/just_rdd")

		scala> postsDF.write.csv("/user/cloudera/stackexchange/dataframes/repeat_df")

		scala> postsDF.write.csv("/user/cloudera/stackexchange/dataframes/repeat_df")
		org.apache.spark.sql.AnalysisException: path hdfs://localhost:9000/user/cloudera/stackexchange/dataframes/repeat_df already exists.;
		  at org.apache.spark.sql.execution.datasources.InsertIntoHadoopFsRelationCommand.run(InsertIntoHadoopFsRelationCommand.scala:121)
		  at org.apache.spark.sql.execution.command.DataWritingCommandExec.sideEffectResult$lzycompute(commands.scala:108)
		  at org.apache.spark.sql.execution.command.DataWritingCommandExec.sideEffectResult(commands.scala:106)
		  at org.apache.spark.sql.execution.command.DataWritingCommandExec.doExecute(commands.scala:131)
		  at org.apache.spark.sql.execution.SparkPlan.$anonfun$execute$1(SparkPlan.scala:175)
		  at org.apache.spark.sql.execution.SparkPlan.$anonfun$executeQuery$1(SparkPlan.scala:213)
		  at org.apache.spark.rdd.RDDOperationScope$.withScope(RDDOperationScope.scala:151)
		  at org.apache.spark.sql.execution.SparkPlan.executeQuery(SparkPlan.scala:210)
		  at org.apache.spark.sql.execution.SparkPlan.execute(SparkPlan.scala:171)
		  at org.apache.spark.sql.execution.QueryExecution.toRdd$lzycompute(QueryExecution.scala:122)
		  at org.apache.spark.sql.execution.QueryExecution.toRdd(QueryExecution.scala:121)
		  at org.apache.spark.sql.DataFrameWriter.$anonfun$runCommand$1(DataFrameWriter.scala:944)
		  at org.apache.spark.sql.execution.SQLExecution$.$anonfun$withNewExecutionId$5(SQLExecution.scala:100)
		  at org.apache.spark.sql.execution.SQLExecution$.withSQLConfPropagated(SQLExecution.scala:160)
		  at org.apache.spark.sql.execution.SQLExecution$.$anonfun$withNewExecutionId$1(SQLExecution.scala:87)
		  at org.apache.spark.sql.SparkSession.withActive(SparkSession.scala:763)
		  at org.apache.spark.sql.execution.SQLExecution$.withNewExecutionId(SQLExecution.scala:64)
		  at org.apache.spark.sql.DataFrameWriter.runCommand(DataFrameWriter.scala:944)
		  at org.apache.spark.sql.DataFrameWriter.saveToV1Source(DataFrameWriter.scala:396)
		  at org.apache.spark.sql.DataFrameWriter.save(DataFrameWriter.scala:380)
		  at org.apache.spark.sql.DataFrameWriter.save(DataFrameWriter.scala:269)
		  at org.apache.spark.sql.DataFrameWriter.csv(DataFrameWriter.scala:934)
		  ... 49 elided

		scala> postsDF.write.mode("overwrite").csv("/user/cloudera/stackexchange/dataframes/repeat_df")

		scala> postsDF.write.mode("overwrite").csv("/user/cloudera/stackexchange/dataframes/repeat_df")

		scala> postsDF.write.mode("overwrite").csv("/user/cloudera/stackexchange/dataframes/repeat_df")

		scala>
		
	4 - Spark SQL: Querying Using Temporary Views
		
		Can use SQL query directly?
		
		scala> spark.sql("select * from postsDF").show()
		org.apache.spark.sql.AnalysisException: Table or view not found: postsDF; line 1 pos 14;
		'Project [*]
		+- 'UnresolvedRelation [postsDF]

		  at org.apache.spark.sql.catalyst.analysis.package$AnalysisErrorAt.failAnalysis(package.scala:42)
		  at org.apache.spark.sql.catalyst.analysis.CheckAnalysis.$anonfun$checkAnalysis$1(CheckAnalysis.scala:106)
		  at org.apache.spark.sql.catalyst.analysis.CheckAnalysis.$anonfun$checkAnalysis$1$adapted(CheckAnalysis.scala:92)
		  at org.apache.spark.sql.catalyst.trees.TreeNode.foreachUp(TreeNode.scala:177)
		  at org.apache.spark.sql.catalyst.trees.TreeNode.$anonfun$foreachUp$1(TreeNode.scala:176)
		  at org.apache.spark.sql.catalyst.trees.TreeNode.$anonfun$foreachUp$1$adapted(TreeNode.scala:176)
		  at scala.collection.immutable.List.foreach(List.scala:392)
		  at org.apache.spark.sql.catalyst.trees.TreeNode.foreachUp(TreeNode.scala:176)
		  at org.apache.spark.sql.catalyst.analysis.CheckAnalysis.checkAnalysis(CheckAnalysis.scala:92)
		  at org.apache.spark.sql.catalyst.analysis.CheckAnalysis.checkAnalysis$(CheckAnalysis.scala:89)
		  at org.apache.spark.sql.catalyst.analysis.Analyzer.checkAnalysis(Analyzer.scala:130)
		  at org.apache.spark.sql.catalyst.analysis.Analyzer.$anonfun$executeAndCheck$1(Analyzer.scala:156)
		  at org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper$.markInAnalyzer(AnalysisHelper.scala:201)
		  at org.apache.spark.sql.catalyst.analysis.Analyzer.executeAndCheck(Analyzer.scala:153)
		  at org.apache.spark.sql.execution.QueryExecution.$anonfun$analyzed$1(QueryExecution.scala:68)
		  at org.apache.spark.sql.catalyst.QueryPlanningTracker.measurePhase(QueryPlanningTracker.scala:111)
		  at org.apache.spark.sql.execution.QueryExecution.$anonfun$executePhase$1(QueryExecution.scala:133)
		  at org.apache.spark.sql.SparkSession.withActive(SparkSession.scala:763)
		  at org.apache.spark.sql.execution.QueryExecution.executePhase(QueryExecution.scala:133)
		  at org.apache.spark.sql.execution.QueryExecution.analyzed$lzycompute(QueryExecution.scala:68)
		  at org.apache.spark.sql.execution.QueryExecution.analyzed(QueryExecution.scala:66)
		  at org.apache.spark.sql.execution.QueryExecution.assertAnalyzed(QueryExecution.scala:58)
		  at org.apache.spark.sql.Dataset$.$anonfun$ofRows$2(Dataset.scala:99)
		  at org.apache.spark.sql.SparkSession.withActive(SparkSession.scala:763)
		  at org.apache.spark.sql.Dataset$.ofRows(Dataset.scala:97)
		  at org.apache.spark.sql.SparkSession.$anonfun$sql$1(SparkSession.scala:606)
		  at org.apache.spark.sql.SparkSession.withActive(SparkSession.scala:763)
		  at org.apache.spark.sql.SparkSession.sql(SparkSession.scala:601)
		  ... 49 elided

		
		- must register the DataFrame first as a temporary SQL view!
		
		
		scala> postsDF.createOrReplaceTempView("Posts")

		scala> spark.sql("select count(*) from Posts")
		res141: org.apache.spark.sql.DataFrame = [count(1): bigint]

		scala> spark.sql("select count(*) from Posts").show()
		+--------+
		|count(1)|
		+--------+
		|   28750|
		+--------+


		scala>
		
		scala> import org.apache.spark.sql.types._
		import org.apache.spark.sql.types._

		scala> val postsSchema =
			 |   StructType(Array(
			 |               StructField("Id", IntegerType),
			 | StructField("PostTypeId", IntegerType),
			 | StructField("AcceptedAnswerId", IntegerType),
			 | StructField("CreationDate", TimestampType),
			 | StructField("Score", IntegerType),
			 | StructField("ViewCount", IntegerType),
			 | StructField("OwnerUserId", IntegerType),
			 | StructField("LastEditorUserId", IntegerType),
			 | StructField("LastEditDate", TimestampType),
			 | StructField("Title", StringType),
			 | StructField("LastActivityDate", TimestampType),
			 | StructField("Tags", StringType),
			 | StructField("AnswerCount", IntegerType),
			 | StructField("CommentCount", IntegerType),
			 | StructField("FavoriteCount", IntegerType)))
		postsSchema: org.apache.spark.sql.types.StructType = StructType(StructField(Id,IntegerType,true), StructField(PostTypeId,IntegerType,true), StructField(AcceptedAnswerId,IntegerType,true), StructField(CreationDate,TimestampType,true), StructField(Score,IntegerType,true), StructField(ViewCount,IntegerType,true), StructField(OwnerUserId,IntegerType,true), StructField(LastEditorUserId,IntegerType,true), StructField(LastEditDate,TimestampType,true), StructField(Title,StringType,true), StructField(LastActivityDate,TimestampType,true), StructField(Tags,StringType,true), StructField(AnswerCount,IntegerType,true), StructField(CommentCount,IntegerType,true), StructField(FavoriteCount,IntegerType,true))

		scala> val postsDF = spark.read.schema(postsSchema).
			 | csv("/user/cloudera/stackexchange/posts_all_csv")
		postsDF: org.apache.spark.sql.DataFrame = [Id: int, PostTypeId: int ... 13 more fields]

		scala> postsDF.createOrReplaceTempView("Posts")

		scala> spark.sql("select count(*) from Posts")
		res2: org.apache.spark.sql.DataFrame = [count(1): bigint]

		scala> spark.sql("select count(*) as TotalPosts from Posts").show()
		+----------+
		|TotalPosts|
		+----------+
		|     28750|
		+----------+


		scala> spark.sql("select Id, Title, ViewCount, AnswerCount from Posts").show(5, truncate=false)
		+---+-----------------------------------------------------------+---------+-----------+
		|Id |Title                                                      |ViewCount|AnswerCount|
		+---+-----------------------------------------------------------+---------+-----------+
		|1  |How can I add line numbers to Vim?                         |10084    |2          |
		|2  |null                                                       |null     |null       |
		|3  |How can I show relative line numbers?                      |45278    |4          |
		|4  |How can I change the default indentation based on filetype?|14206    |4          |
		|5  |null                                                       |null     |null       |
		+---+-----------------------------------------------------------+---------+-----------+
		only showing top 5 rows


		scala>
		
		scala> import org.apache.spark.sql.functions.reverse
		import org.apache.spark.sql.functions.reverse

		scala> val top_viewsDF = spark.sql("select Id, reverse(Title) as eltiT, ViewCount, AnswerCount from Posts order by ViewCount desc")
		top_viewsDF: org.apache.spark.sql.DataFrame = [Id: int, eltiT: string ... 2 more fields]

		scala> top_viewsDF.show(5, truncate=false)
		+----+----------------------------------------------------------------+---------+-----------+
		|Id  |eltiT                                                           |ViewCount|AnswerCount|
		+----+----------------------------------------------------------------+---------+-----------+
		|84  |?miV morf draobpilc metsys eht ot txet ypoc I nac woH           |554639   |13         |
		|2232|?rotide xeh a sa miV esu I nac woH                              |276639   |2          |
		|625 |?loot ffid a sa miv esu I od woH                                |240742   |11         |
		|422 |sretcarahc sa sbat gniyalpsiD                                   |220981   |8          |
		|9028|?miVsV dna miv ni ;touq&llA tceleS;touq& rof dnammoc eht si tahw|204656   |7          |
		+----+----------------------------------------------------------------+---------+-----------+
		only showing top 5 rows


		scala>
		
	5 - Loading Files and Views into DataFrames Using Spark SQL
		
		- load from file into DataFrame
		
		scala> val comments_loadedDF = spark.
			 | sql("select * from parquet.`/user/cloudera/stackexchange/comments_parquet`")
		comments_loadedDF: org.apache.spark.sql.DataFrame = [Id: int, PostId: int ... 3 more fields]

		scala> comments_loadedDF.show(5)
		+---+------+-----+--------------------+------+
		| Id|PostId|Score|        CreationDate|UserId|
		+---+------+-----+--------------------+------+
		|  1|     7|    2|2015-02-03T16:59:...|    11|
		|  2|    10|    2|2015-02-03T17:01:...|     5|
		|  3|     7|    0|2015-02-03T17:05:...|    27|
		|  4|    19|    1|2015-02-03T17:11:...|    46|
		|  6|    13|    2|2015-02-03T17:18:...|    58|
		+---+------+-----+--------------------+------+
		only showing top 5 rows


		scala>
		
		scala> sql("select * from parquet.`/user/cloudera/stackexchange/comments_parquet` order by Score desc").show(5)
		+-----+------+-----+--------------------+------+
		|   Id|PostId|Score|        CreationDate|UserId|
		+-----+------+-----+--------------------+------+
		|  281|   309|   96|2015-02-04T15:05:...|   134|
		|  803|   626|   56|2015-02-10T11:15:...|   343|
		|  234|    51|   51|2015-02-04T09:23:...|   267|
		|12267|  7468|   31|2016-04-22T23:43:...|   205|
		|  405|   390|   29|2015-02-05T03:24:...|   246|
		+-----+------+-----+--------------------+------+
		only showing top 5 rows


		scala>
		
		
		- load view as DataFrame
		
		scala> comments_loadedDF.createOrReplaceTempView("comments")

		scala> val comments_reloadedDF = spark.table("comments")
		comments_reloadedDF: org.apache.spark.sql.DataFrame = [Id: int, PostId: int ... 3 more fields]

		scala> comments_reloadedDF.orderBy($"Score".desc).show(5)
		+-----+------+-----+--------------------+------+
		|   Id|PostId|Score|        CreationDate|UserId|
		+-----+------+-----+--------------------+------+
		|  281|   309|   96|2015-02-04T15:05:...|   134|
		|  803|   626|   56|2015-02-10T11:15:...|   343|
		|  234|    51|   51|2015-02-04T09:23:...|   267|
		|12267|  7468|   31|2016-04-22T23:43:...|   205|
		|  405|   390|   29|2015-02-05T03:24:...|   246|
		+-----+------+-----+--------------------+------+
		only showing top 5 rows


		scala>
		
	6 - Saving to Persistent Tables + Spark 2 Known Issue
			
		- persist using saveAsTable()
		
		!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
		- USE postsDF.write.option("path", ...).saveAsTable NOT postsDF.write.saveAsTable!!!
			- see D:\aaa\workout\techs\bigdata\spark\README.txt
		!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
		
		scala> import org.apache.spark.sql.types._
		import org.apache.spark.sql.types._

		scala> val postsSchema =
			 |   StructType(Array(
			 |               StructField("Id", IntegerType),
			 | StructField("PostTypeId", IntegerType),
			 | StructField("AcceptedAnswerId", IntegerType),
			 | StructField("CreationDate", TimestampType),
			 | StructField("Score", IntegerType),
			 | StructField("ViewCount", IntegerType),
			 | StructField("OwnerUserId", IntegerType),
			 | StructField("LastEditorUserId", IntegerType),
			 | StructField("LastEditDate", TimestampType),
			 | StructField("Title", StringType),
			 | StructField("LastActivityDate", TimestampType),
			 | StructField("Tags", StringType),
			 | StructField("AnswerCount", IntegerType),
			 | StructField("CommentCount", IntegerType),
			 | StructField("FavoriteCount", IntegerType)))
		postsSchema: org.apache.spark.sql.types.StructType = StructType(StructField(Id,IntegerType,true), StructField(PostTypeId,IntegerType,true), StructField(AcceptedAnswerId,IntegerType,true), StructField(CreationDate,TimestampType,true), StructField(Score,IntegerType,true), StructField(ViewCount,IntegerType,true), StructField(OwnerUserId,IntegerType,true), StructField(LastEditorUserId,IntegerType,true), StructField(LastEditDate,TimestampType,true), StructField(Title,StringType,true), StructField(LastActivityDate,TimestampType,true), StructField(Tags,StringType,true), StructField(AnswerCount,IntegerType,true), StructField(CommentCount,IntegerType,true), StructField(FavoriteCount,IntegerType,true))

		scala> val postsDF = spark.read.schema(postsSchema).
			 | csv("/user/cloudera/stackexchange/posts_all_csv")
		postsDF: org.apache.spark.sql.DataFrame = [Id: int, PostTypeId: int ... 13 more fields]

		scala> postsDF.write.option("path", "/user/cloudera/stackexchange/tables").
			 | saveAsTable("posts_savetable_option")

		!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
		NOT this
			scala> postsDF.write.saveAsTable("posts_savetable_nooption")
		unless login in with user "root" not user "hadoop"
		!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

		scala> spark.sql("select * from posts_savetable_option")
		res2: org.apache.spark.sql.DataFrame = [Id: int, PostTypeId: int ... 13 more fields]

		scala> spark.sql("select * from posts_savetable_option").show()
		+---+----------+----------------+--------------------+-----+---------+-----------+----------------+--------------------+--------------------+--------------------+--------------------+-----------+------------+-------------+
		| Id|PostTypeId|AcceptedAnswerId|        CreationDate|Score|ViewCount|OwnerUserId|LastEditorUserId|        LastEditDate|               Title|    LastActivityDate|                Tags|AnswerCount|CommentCount|FavoriteCount|
		+---+----------+----------------+--------------------+-----+---------+-----------+----------------+--------------------+--------------------+--------------------+--------------------+-----------+------------+-------------+
		|  1|         1|               5|2015-02-03 16:40:...|   45|    10084|          2|               2|2015-02-03 17:51:...|How can I add lin...|2018-12-12 18:05:...|      (line-numbers)|          2|           0|            8|
		|  2|         2|            null|2015-02-03 16:43:...|   24|     null|          5|            1109|2018-12-12 18:05:...|                null|2018-12-12 18:05:...|                null|       null|           1|         null|
		|  3|         1|               8|2015-02-03 16:54:...|   91|    45278|         11|              28|2015-02-03 16:55:...|How can I show re...|2017-11-20 16:51:...|      (line-numbers)|          4|           0|           16|
		|  4|         1|              43|2015-02-03 16:54:...|   42|    14206|         12|           21062|2019-08-06 22:20:...|How can I change ...|2019-08-06 22:20:...|(indentation,file...|          4|           1|            9|
		|  5|         2|            null|2015-02-03 16:54:...|   58|     null|         19|             135|2015-02-03 21:05:...|                null|2015-02-03 21:05:...|                null|       null|           2|         null|
		|  6|         1|            null|2015-02-03 16:55:...|   51|    15456|          2|              24|2015-02-05 08:44:...|How can I use the...|2020-11-25 17:31:...|(persistent-state...|          1|           1|           13|
		|  7|         2|            null|2015-02-03 16:56:...|    5|     null|         27|            null|                null|                null|2015-02-03 16:56:...|                null|       null|           7|         null|
		|  8|         2|            null|2015-02-03 16:58:...|  110|     null|         19|              -1|2017-04-13 12:51:...|                null|2015-02-03 21:58:...|                null|       null|           4|         null|
		|  9|         1|              23|2015-02-03 16:58:...|   22|     1360|         28|             343|2016-02-10 10:55:...|Can I script Vim ...|2016-02-10 10:55:...|  (vimscript-python)|          2|           1|            2|
		| 10|         2|            null|2015-02-03 16:59:...|   12|     null|         27|              27|2015-02-03 17:06:...|                null|2015-02-03 17:06:...|                null|       null|           1|         null|
		| 11|         2|            null|2015-02-03 17:00:...|   14|     null|         31|            null|                null|                null|2015-02-03 17:00:...|                null|       null|           0|         null|
		| 12|         1|              14|2015-02-03 17:00:...|   40|     5532|         24|            null|                null|How can I generat...|2016-12-07 20:47:...|(line-numbers,tex...|          5|           0|            7|
		| 13|         1|            2561|2015-02-03 17:01:...|   14|      932|         28|              28|2015-02-11 09:12:...|Does any solution...|2021-09-23 14:27:...|     (input-devices)|          2|           8|            2|
		| 14|         2|            null|2015-02-03 17:02:...|   56|     null|          2|              -1|2017-04-13 12:51:...|                null|2015-02-03 17:41:...|                null|       null|           1|         null|
		| 15|         2|            null|2015-02-03 17:03:...|    7|     null|         37|            null|                null|                null|2015-02-03 17:03:...|                null|       null|           0|         null|
		| 16|         1|              47|2015-02-03 17:03:...|    9|      150|         28|              33|2015-02-03 17:47:...|Can I use some fi...|2015-02-03 21:07:...|        (filesystem)|          4|           0|         null|
		| 17|         1|            null|2015-02-03 17:05:...|   15|     2956|          5|            null|                null|How can I search ...|2019-10-03 12:00:...|(line-numbers,sea...|          4|           0|            1|
		| 18|         2|            null|2015-02-03 17:05:...|   20|     null|         31|            null|                null|                null|2015-02-03 17:05:...|                null|       null|           1|         null|
		| 19|         1|              33|2015-02-03 17:05:...|   30|     1354|         18|            1292|2018-08-17 20:19:...|Why can ci&quot; ...|2020-10-05 02:33:...|(cursor-motions,c...|          4|           1|            5|
		| 20|         2|            null|2015-02-03 17:05:...|    4|     null|         37|            null|                null|                null|2015-02-03 17:05:...|                null|       null|           0|         null|
		+---+----------+----------------+--------------------+-----+---------+-----------+----------------+--------------------+--------------------+--------------------+--------------------+-----------+------------+-------------+
		only showing top 20 rows


		scala>
		
		
	7 - Hive Support and External Databases
		
		- create a database, a table in mysql
			
			mysql> create database ohana;
			Query OK, 1 row affected (0.04 sec)

			mysql> use ohana
			Database changed
			mysql> create table home(id int(1), name varchar(20), address varchar(20));
			Query OK, 0 rows affected, 1 warning (0.13 sec)

			mysql> insert into home (id, name, address) values (1, 'home 1', '6888 Rumble St');
			Query OK, 1 row affected (0.02 sec)

			mysql> select * from home;
			+------+--------+----------------+
			| id   | name   | address        |
			+------+--------+----------------+
			|    1 | home 1 | 6888 Rumble St |
			+------+--------+----------------+
			1 row in set (0.00 sec)

			mysql>
			
		- give permissions!
			
			mysql> GRANT ALL PRIVILEGES ON *.* TO 'root'@'localhost' WITH GRANT OPTION;
			Query OK, 0 rows affected (0.02 sec)

			mysql> FLUSH PRIVILEGES;
			Query OK, 0 rows affected (0.03 sec)

			mysql>
			
			mysql> ALTER USER 'root'@'localhost' IDENTIFIED WITH mysql_native_password BY 'root';
			Query OK, 0 rows affected (0.02 sec)

			mysql> FLUSH PRIVILEGES;
			Query OK, 0 rows affected (0.01 sec)

			mysql> select user, authentication_string, plugin, host from mysql.user;
			+------------------+------------------------------------------------------------------------+-----------------------+-----------+
			| user             | authentication_string                                                  | plugin                | host      |
			+------------------+------------------------------------------------------------------------+-----------------------+-----------+
			| hiveuser         | $A$005$    r#%<;n?t;{K?}!>J?[xVCAw8TQgf4LXRegM2AAbeWKHbUqxpF51WBSvpC1Ocb/ | caching_sha2_password | %         |
			| debian-sys-maint | $A$005$mt0{7%x5KYi?Zx9??z0OcswzwNOkyY8hhfvnbXECVP9xJaoqt9eNGl8K4n5 | caching_sha2_password | localhost |
			| mysql.infoschema | $A$005$THISISACOMBINATIONOFINVALIDSALTANDPASSWORDTHATMUSTNEVERBRBEUSED | caching_sha2_password | localhost |
			| mysql.session    | $A$005$THISISACOMBINATIONOFINVALIDSALTANDPASSWORDTHATMUSTNEVERBRBEUSED | caching_sha2_password | localhost |
			| mysql.sys        | $A$005$THISISACOMBINATIONOFINVALIDSALTANDPASSWORDTHATMUSTNEVERBRBEUSED | caching_sha2_password | localhost |
			| root             | *81F5E21E35407D884A6CD4A731AEBFB6AF209E1B                              | mysql_native_password | localhost |
			+------------------+------------------------------------------------------------------------+-----------------------+-----------+
			6 rows in set (0.00 sec)

			mysql>
			
		- test with java JDBC program directly
			D:\aaa\workout\techs\sql\mysql\MysqlJDBCTester.java
			
			hadoop@nodemaster:~/workout/jdbc$ view MysqlJDBCTester.java
			hadoop@nodemaster:~/workout/jdbc$ pwd
			/home/hadoop/workout/jdbc
			hadoop@nodemaster:~/workout/jdbc$ java -classpath .:/home/hadoop/hive/lib/mysql-connector-java-8.0.30.jar MysqlJDBCTester
			id: 1, name: home 1, address: 6888 Rumble St
		
		- start spark-shell cli with mysql-connector-java
			
			hadoop@nodemaster:/$ pwd
			/
			hadoop@nodemaster:/$ spark-shell --jars /home/hadoop/hive/lib/mysql-connector-java-8.0.30.jar
			
		- retrieve data from MySQL with JDBC
			
			scala> val external_databaseDF = spark.read.format("jdbc").
				 | option("url", "jdbc:mysql://localhost:3306/ohana").
				 | option("driver", "com.mysql.cj.jdbc.Driver").
				 | option("dbtable", "(select * from home) as table1").
				 | option("user", "root").
				 | option("password", "root").
				 | load()
			external_databaseDF: org.apache.spark.sql.DataFrame = [id: int, name: string ... 1 more field]

			scala>
			
			scala> external_databaseDF.count()
			res0: Long = 1

			scala> external_databaseDF.columns
			res1: Array[String] = Array(id, name, address)

			scala> external_databaseDF.show()
			+---+------+--------------+
			| id|  name|       address|
			+---+------+--------------+
			|  1|home 1|6888 Rumble St|
			+---+------+--------------+


			scala>
			
			add required jar () to argument of spark-shell
				
				- hadoop@nodemaster:/$ spark-shell --jars /home/hadoop/hive/lib/mysql-connector-java-8.0.30.jar,/home/hadoop/spark/jars/spark-hive_2.12-3.0.0.jar
			
			scala> import org.apache.spark.sql.SparkSession
			import org.apache.spark.sql.SparkSession

			scala> val spark_hive = SparkSession.builder.
				 | appName("Hive Demo with Spark Scala").
				 | config("spark.sql.warehouse.dir", "/user/hive/warehouses/").
				 | enableHiveSupport().
				 | getOrCreate()
			java.lang.IllegalArgumentException: Unable to instantiate SparkSession with Hive support because Hive classes are not found.
			  at org.apache.spark.sql.SparkSession$Builder.enableHiveSupport(SparkSession.scala:871)
			  ... 51 elided

			scala>
			
	8 - Aggregating, Grouping, and Joining
			
		- prepare data if not done yet
			hadoop@nodemaster:/tmp/demos$ cd comments
			hadoop@nodemaster:/tmp/demos/comments$ ls
			build.sbt  src
			hadoop@nodemaster:/tmp/demos/comments$ sbt package
			[info] Updated file /tmp/demos/comments/project/build.properties: set sbt.version to 1.6.2
			[info] welcome to sbt 1.6.2 (Private Build Java 1.8.0_342)
			[info] loading project definition from /tmp/demos/comments/project
			[info] loading settings for project comments from build.sbt ...
			[info] set current project to Comments Project (in build file:/tmp/demos/comments/)
			[info] compiling 1 Scala source to /tmp/demos/comments/target/scala-2.11/classes ...
			[success] Total time: 14 s, completed Oct 14, 2022 7:46:53 PM
			hadoop@nodemaster:/tmp/demos/comments$ ls
			build.sbt  project  src  target
			hadoop@nodemaster:/tmp/demos/comments$ spark-submit --class PrepareCommentsParquetApp target/scala-2.11/comments-project_2.11-1.0.jar

		scala> val commentsDF = spark.read.parquet("/user/cloudera/stackexchange/comments_parquet")
		commentsDF: org.apache.spark.sql.DataFrame = [Id: int, PostId: int ... 3 more fields]

		scala> commentsDF.show(5)
		+---+------+-----+--------------------+------+
		| Id|PostId|Score|        CreationDate|UserId|
		+---+------+-----+--------------------+------+
		|  1|     7|    2|2015-02-03T16:59:...|    11|
		|  2|    10|    2|2015-02-03T17:01:...|     5|
		|  3|     7|    0|2015-02-03T17:05:...|    27|
		|  4|    19|    1|2015-02-03T17:11:...|    46|
		|  6|    13|    2|2015-02-03T17:18:...|    58|
		+---+------+-----+--------------------+------+
		only showing top 5 rows


		scala>
		
		scala> commentsDF.groupBy("UserId")
		res1: org.apache.spark.sql.RelationalGroupedDataset = RelationalGroupedDataset: [grouping expressions: [UserId: int], value: [Id: int, PostId: int ... 3 more fields], type: GroupBy]

		scala> commentsDF.groupBy("UserId").getClass
		res2: Class[_ <: org.apache.spark.sql.RelationalGroupedDataset] = class org.apache.spark.sql.RelationalGroupedDataset

		scala> commentsDF.groupBy("UserId").
			 | ;
		<console>:2: error: identifier expected but ';' found.
			   ;
			   ^

		- need use TAB after .!!!
		
		scala> commentsDF.groupBy("UserId").
		agg   as   avg   count   max   mean   min   pivot   sum   toString

		scala> commentsDF.groupBy("UserId").sum("Score").show(5)
		+------+----------+
		|UserId|sum(Score)|
		+------+----------+
		|   496|         7|
		|   833|         0|
		|  2659|         1|
		|  6357|         9|
		|  1238|        10|
		+------+----------+
		only showing top 5 rows


		scala> commentsDF.groupBy("UserId").sum("Score").sort($"sum(Score)".desc).show(5)
		+------+----------+
		|UserId|sum(Score)|
		+------+----------+
		| 10604|      2795|
		|    71|      1893|
		|  1841|      1469|
		|    51|      1264|
		| 18609|      1058|
		+------+----------+
		only showing top 5 rows


		scala> commentsDF.groupBy("UserId").avg("Score").sort($"avg(Score)".desc).show(5)
		+------+------------------+
		|UserId|        avg(Score)|
		+------+------------------+
		|  8872|              18.0|
		|   134|14.714285714285714|
		|  8935|              14.5|
		|  2241|              12.0|
		| 15251|              12.0|
		+------+------------------+
		only showing top 5 rows


		scala> commentsDF.groupBy("UserId").agg(avg("Score"), count("Score").alias("Total")).show(5)
		+------+------------------+-----+
		|UserId|        avg(Score)|Total|
		+------+------------------+-----+
		|   496|0.3684210526315789|   19|
		|   833|               0.0|    3|
		|  2659|               1.0|    1|
		|  6357|0.2903225806451613|   31|
		|  1238|0.5263157894736842|   19|
		+------+------------------+-----+
		only showing top 5 rows


		scala> commentsDF.groupBy("UserId").agg(avg("Score"), count("Score").alias("Total")).sort($"avg(Score)".desc).show(5)
		+------+------------------+-----+
		|UserId|        avg(Score)|Total|
		+------+------------------+-----+
		|  8872|              18.0|    1|
		|   134|14.714285714285714|    7|
		|  8935|              14.5|    2|
		|  2241|              12.0|    1|
		| 15251|              12.0|    1|
		+------+------------------+-----+
		only showing top 5 rows


		scala> commentsDF.groupBy("UserId").agg(sum("Score"), count("Score").alias("Total")).sort($"sum(Score)".desc).show(5)
		+------+----------+-----+
		|UserId|sum(Score)|Total|
		+------+----------+-----+
		| 10604|      2795| 4375|
		|    71|      1893| 2064|
		|  1841|      1469| 1873|
		|    51|      1264| 1234|
		| 18609|      1058| 2174|
		+------+----------+-----+
		only showing top 5 rows


		scala>
		
		scala> commentsDF.describe().show(5)
		+-------+------------------+------------------+------------------+--------------------+------------------+
		|summary|                Id|            PostId|             Score|        CreationDate|            UserId|
		+-------+------------------+------------------+------------------+--------------------+------------------+
		|  count|             49465|             49465|             49465|               49465|             49081|
		|   mean|  32718.7555847569|17703.607560901648|0.5373294248458506|                null|10217.500071310691|
		| stddev|18890.528611375223| 10272.54490140659|1.2355694713397791|                null| 10021.93084096969|
		|    min|                 1|                 2|                 0|2015-02-03T16:59:...|                -1|
		|    max|             68553|             37620|                96|2022-06-05T01:42:...|             42282|
		+-------+------------------+------------------+------------------+--------------------+------------------+


		scala> commentsDF.describe("Score").show(5)
		+-------+------------------+
		|summary|             Score|
		+-------+------------------+
		|  count|             49465|
		|   mean|0.5373294248458506|
		| stddev|1.2355694713397791|
		|    min|                 0|
		|    max|                96|
		+-------+------------------+


		scala>
		
		scala> postsDF
		<console>:25: error: not found: value postsDF
			   postsDF
			   ^

		scala> import org.apache.spark.sql.types._
		import org.apache.spark.sql.types._

		scala> val postsSchema =
			 |                    |   StructType(Array(
			 |                    |               StructField("Id", IntegerType),
			 |                    | StructField("PostTypeId", IntegerType),
			 |                    | StructField("AcceptedAnswerId", IntegerType),
			 |                    | StructField("CreationDate", TimestampType),
			 |                    | StructField("Score", IntegerType),
			 |                    | StructField("ViewCount", IntegerType),
			 |                    | StructField("OwnerUserId", IntegerType),
			 |                    | StructField("LastEditorUserId", IntegerType),
			 |                    | StructField("LastEditDate", TimestampType),
			 |                    | StructField("Title", StringType),
			 |                    | StructField("LastActivityDate", TimestampType),
			 |                    | StructField("Tags", StringType),
			 |                    | StructField("AnswerCount", IntegerType),
			 |                    | StructField("CommentCount", IntegerType),
			 |                    | StructField("FavoriteCount", IntegerType)))
		postsSchema: org.apache.spark.sql.types.StructType = StructType(StructField(Id,IntegerType,true), StructField(PostTypeId,IntegerType,true), StructField(AcceptedAnswerId,IntegerType,true), StructField(CreationDate,TimestampType,true), StructField(Score,IntegerType,true), StructField(ViewCount,IntegerType,true), StructField(OwnerUserId,IntegerType,true), StructField(LastEditorUserId,IntegerType,true), StructField(LastEditDate,TimestampType,true), StructField(Title,StringType,true), StructField(LastActivityDate,TimestampType,true), StructField(Tags,StringType,true), StructField(AnswerCount,IntegerType,true), StructField(CommentCount,IntegerType,true), StructField(FavoriteCount,IntegerType,true))

		scala> val postsDF = spark.read.schema(postsSchema).
			 | csv("/user/cloudera/stackexchange/posts_all_csv")
		postsDF: org.apache.spark.sql.DataFrame = [Id: int, PostTypeId: int ... 13 more fields]

		scala> postsDF
		res13: org.apache.spark.sql.DataFrame = [Id: int, PostTypeId: int ... 13 more fields]

		scala> val ask_questionsDF = postsDF.where("PostTypeId = 1").select("OwnerUserId").distinct()
		ask_questionsDF: org.apache.spark.sql.Dataset[org.apache.spark.sql.Row] = [OwnerUserId: int]

		scala> val answer_questionsDF = postsDF.where("PostTypeId = 2").select("OwnerUserId").distinct()
		answer_questionsDF: org.apache.spark.sql.Dataset[org.apache.spark.sql.Row] = [OwnerUserId: int]

		scala> postsDF.select("OwnerUserId").distinct().count()
		res14: Long = 6481

		scala> ask_questionsDF.count()
		res15: Long = 5001

		scala> answer_questionsDF.count()
		res16: Long = 2997

		scala> ask_questionsDF.join(answer_questionsDF, ask_questionsDF("OwnerUserId") === answer_questionsDF("OwnerUserId")).count()
		res17: Long = 1520

		scala> commentsDF.createOrReplaceTempView("comments")

		scala> spark.sql("Select UserId, count(Score) as TotalCount from comments group by UserId order by TotalCount desc").show(5)
		+------+----------+
		|UserId|TotalCount|
		+------+----------+
		| 10604|      4375|
		| 18609|      2174|
		|    71|      2064|
		| 11054|      2041|
		|  1841|      1873|
		+------+----------+
		only showing top 5 rows


		scala>
		
	9 - The Catalog API
		
		scala> spark.catalog
		res20: org.apache.spark.sql.catalog.Catalog = org.apache.spark.sql.internal.CatalogImpl@6e35926

		scala> spark.catalog.
		cacheTable            currentDatabase      functionExists   isCached        listTables          setCurrentDatabase
		clearCache            databaseExists       getDatabase      listColumns     recoverPartitions   tableExists
		createExternalTable   dropGlobalTempView   getFunction      listDatabases   refreshByPath       uncacheTable
		createTable           dropTempView         getTable         listFunctions   refreshTable

		scala> spark.catalog.listDatabases()
		res22: org.apache.spark.sql.Dataset[org.apache.spark.sql.catalog.Database] = [name: string, description: string ... 1 more field]

		scala> spark.catalog.listDatabases().show(truncate=false)
		+-------+----------------+---------------------+
		|name   |description     |locationUri          |
		+-------+----------------+---------------------+
		|default|default database|file:/spark-warehouse|
		+-------+----------------+---------------------+


		scala> spark.catalog.listTables()
		res24: org.apache.spark.sql.Dataset[org.apache.spark.sql.catalog.Table] = [name: string, database: string ... 3 more fields]

		scala> spark.catalog.listTables("default").show()
		+--------+--------+-----------+---------+-----------+
		|    name|database|description|tableType|isTemporary|
		+--------+--------+-----------+---------+-----------+
		|comments|    null|       null|TEMPORARY|       true|
		+--------+--------+-----------+---------+-----------+


		scala>
		
		scala> spark.catalog.listTables("default").count()
		res26: Long = 1

		scala> spark.catalog.dropTempView("comments")
		res27: Boolean = true

		scala> spark.catalog.listTables("default").count()
		res28: Long = 0

		scala> spark.catalog.listTables("default").show()
		+----+--------+-----------+---------+-----------+
		|name|database|description|tableType|isTemporary|
		+----+--------+-----------+---------+-----------+
		+----+--------+-----------+---------+-----------+


		scala>
		
10 - Working with a Typed API: Datasets
		
	1 - Understanding a Typed API: Datasets
		
		- RDD (Resilient Distributed Dataset)
			- at the bottom level
		- DataFrame
			- untyped API
		- Dataset
			- typed API
			
	2 - Motivation behind Dataset		
		
		hadoop@nodemaster:/tmp/demos/posts_simple_titles$ hdfs dfs -ls /user/cloudera/stackexchange
		Found 12 items
		-rwxr-xr-x   1 hadoop hadoop    6602759 2022-10-06 05:22 /user/cloudera/stackexchange/Badges.xml
		-rwxr-xr-x   1 hadoop hadoop   15360893 2022-10-06 05:22 /user/cloudera/stackexchange/Comments.xml
		-rwxr-xr-x   1 hadoop hadoop   80356100 2022-10-06 05:22 /user/cloudera/stackexchange/PostHistory.xml
		-rwxr-xr-x   1 hadoop hadoop     418926 2022-10-06 05:22 /user/cloudera/stackexchange/PostLinks.xml
		-rwxr-xr-x   1 hadoop hadoop   45023223 2022-10-06 05:22 /user/cloudera/stackexchange/Posts.xml
		-rwxr-xr-x   1 hadoop hadoop      30338 2022-10-06 05:22 /user/cloudera/stackexchange/Tags.xml
		-rwxr-xr-x   1 hadoop hadoop   15291027 2022-10-06 05:22 /user/cloudera/stackexchange/Users.xml
		-rwxr-xr-x   1 hadoop hadoop   12854948 2022-10-06 05:22 /user/cloudera/stackexchange/Votes.xml
		drwxr-xr-x   - hadoop hadoop          0 2022-10-14 19:50 /user/cloudera/stackexchange/comments_parquet
		drwxr-xr-x   - hadoop hadoop          0 2022-10-06 05:58 /user/cloudera/stackexchange/dataframes
		drwxr-xr-x   - hadoop hadoop          0 2022-10-06 05:36 /user/cloudera/stackexchange/posts_all_csv
		drwxr-xr-x   - hadoop hadoop          0 2022-10-11 05:29 /user/cloudera/stackexchange/tables
	
		hadoop@nodemaster:/tmp/demos$ ls
		badges   comments            commiters_rdd.scala  posts_header         spark-committers-no-header.tsv  tags
		badgesc  commiters_df.scala  posts                posts_simple_titles  spark-committers.tsv            users
		hadoop@nodemaster:/tmp/demos$ cd posts_simple_titles/
		hadoop@nodemaster:/tmp/demos/posts_simple_titles$ ls
		build.sbt  src
		hadoop@nodemaster:/tmp/demos/posts_simple_titles$ sbt package
		[info] Updated file /tmp/demos/posts_simple_titles/project/build.properties: set sbt.version to 1.6.2
		[info] welcome to sbt 1.6.2 (Private Build Java 1.8.0_342)
		[info] loading project definition from /tmp/demos/posts_simple_titles/project
		[info] loading settings for project posts_simple_titles from build.sbt ...
		[info] set current project to Posts Simple Titles Project (in build file:/tmp/demos/posts_simple_titles/)
		[info] compiling 1 Scala source to /tmp/demos/posts_simple_titles/target/scala-2.11/classes ...
		[success] Total time: 15 s, completed Oct 17, 2022 5:18:59 AM
		hadoop@nodemaster:/tmp/demos/posts_simple_titles$ ls
		build.sbt  project  src  target
		hadoop@nodemaster:/tmp/demos/posts_simple_titles$ spark-submit --class PreparePostsSimpleTitlesApp target/scala-2.11/posts-simple-titles-project_2.11-1.0.jar
		
		hadoop@nodemaster:/tmp/demos/posts_simple_titles$ hdfs dfs -ls /user/cloudera/stackexchange
		Found 13 items
		-rwxr-xr-x   1 hadoop hadoop    6602759 2022-10-06 05:22 /user/cloudera/stackexchange/Badges.xml
		-rwxr-xr-x   1 hadoop hadoop   15360893 2022-10-06 05:22 /user/cloudera/stackexchange/Comments.xml
		-rwxr-xr-x   1 hadoop hadoop   80356100 2022-10-06 05:22 /user/cloudera/stackexchange/PostHistory.xml
		-rwxr-xr-x   1 hadoop hadoop     418926 2022-10-06 05:22 /user/cloudera/stackexchange/PostLinks.xml
		-rwxr-xr-x   1 hadoop hadoop   45023223 2022-10-06 05:22 /user/cloudera/stackexchange/Posts.xml
		-rwxr-xr-x   1 hadoop hadoop      30338 2022-10-06 05:22 /user/cloudera/stackexchange/Tags.xml
		-rwxr-xr-x   1 hadoop hadoop   15291027 2022-10-06 05:22 /user/cloudera/stackexchange/Users.xml
		-rwxr-xr-x   1 hadoop hadoop   12854948 2022-10-06 05:22 /user/cloudera/stackexchange/Votes.xml
		drwxr-xr-x   - hadoop hadoop          0 2022-10-14 19:50 /user/cloudera/stackexchange/comments_parquet
		drwxr-xr-x   - hadoop hadoop          0 2022-10-06 05:58 /user/cloudera/stackexchange/dataframes
		drwxr-xr-x   - hadoop hadoop          0 2022-10-06 05:36 /user/cloudera/stackexchange/posts_all_csv
		drwxr-xr-x   - hadoop hadoop          0 2022-10-17 05:28 /user/cloudera/stackexchange/simple_titles_txt
		drwxr-xr-x   - hadoop hadoop          0 2022-10-11 05:29 /user/cloudera/stackexchange/tables
		hadoop@nodemaster:/tmp/demos/posts_simple_titles$
		
		
		Now simple_titles_txt has been created.
		
		
		- RDD vs Dataset
		
		scala> val postsRDD = sc.textFile("/user/cloudera/stackexchange/simple_titles_txt")
		postsRDD: org.apache.spark.rdd.RDD[String] = /user/cloudera/stackexchange/simple_titles_txt MapPartitionsRDD[135] at textFile at <console>:28

		scala> val postsDS = spark.read.text("/user/cloudera/stackexchange/simple_titles_txt").
			 | as[String]
		postsDS: org.apache.spark.sql.Dataset[String] = [value: string]

		scala> postsRDD.setName("PostsRDD")
		res30: postsRDD.type = PostsRDD MapPartitionsRDD[135] at textFile at <console>:28

		scala> postsRDD.cache
		res31: postsRDD.type = PostsRDD MapPartitionsRDD[135] at textFile at <console>:28

		scala> postsDS.cache
		res32: postsDS.type = [value: string]

		scala> postsRDD.count
		res33: Long = 28750

		scala> postsDS.count
		res34: Long = 28750

		scala>
		
		- DataFrame vs Dataset
		
		scala> val postsDF = spark.read.text("/user/cloudera/stackexchange/simple_titles_txt")
		postsDF: org.apache.spark.sql.DataFrame = [value: string]

		scala> postsDF.cache
		res35: postsDF.type = [value: string]

		scala> postsDF.count
		res36: Long = 28750

		scala>
		
		scala> import org.apache.spark.sql.types._
		import org.apache.spark.sql.types._

		scala> val postsSchema =
			 |   StructType(Array(
			 |               StructField("Id", IntegerType),
			 | StructField("PostTypeId", IntegerType),
			 | StructField("AcceptedAnswerId", IntegerType),
			 | StructField("CreationDate", TimestampType),
			 | StructField("Score", IntegerType),
			 | StructField("ViewCount", IntegerType),
			 | StructField("OwnerUserId", IntegerType),
			 | StructField("LastEditorUserId", IntegerType),
			 | StructField("LastEditDate", TimestampType),
			 | StructField("Title", StringType),
			 | StructField("LastActivityDate", TimestampType),
			 | StructField("Tags", StringType),
			 | StructField("AnswerCount", IntegerType),
			 | StructField("CommentCount", IntegerType),
			 | StructField("FavoriteCount", IntegerType)))
		postsSchema: org.apache.spark.sql.types.StructType = StructType(StructField(Id,IntegerType,true), StructField(PostTypeId,IntegerType,true), StructField(AcceptedAnswerId,IntegerType,true), StructField(CreationDate,TimestampType,true), StructField(Score,IntegerType,true), StructField(ViewCount,IntegerType,true), StructField(OwnerUserId,IntegerType,true), StructField(LastEditorUserId,IntegerType,true), StructField(LastEditDate,TimestampType,true), StructField(Title,StringType,true), StructField(LastActivityDate,TimestampType,true), StructField(Tags,StringType,true), StructField(AnswerCount,IntegerType,true), StructField(CommentCount,IntegerType,true), StructField(FavoriteCount,IntegerType,true))

		scala> case class Post(Id: Int, PostTypeId: Int, Score: Integer, ViewCount: Integer, AnswerCount: Integer, OwnerUserId: Integer)
		defined class Post

		scala> val posts_all = spark.read.schema(postsSchema).csv("/user/cloudera/stackexchange/posts_all_csv")
		posts_all: org.apache.spark.sql.DataFrame = [Id: int, PostTypeId: int ... 13 more fields]

		scala> val postsDF = posts_all.select().na.drop("any", Seq("ViewCount"))
		postsDF: org.apache.spark.sql.DataFrame = [Id: int, PostTypeId: int ... 4 more fields]

		scala> val postsDS = posts_all.select($"Id", $"PostTypeId", $"Score", $"ViewCount", $"AnswerCount", $"OwnerUserId").as[Post]
		postsDS: org.apache.spark.sql.Dataset[Post] = [Id: int, PostTypeId: int ... 4 more fields]

		scala> postsDF.first.
		anyNull   fieldIndex   getByte      getFloat     getList        getSeq      getTimestamp   json         schema
		apply     get          getDate      getInstant   getLocalDate   getShort    getValuesMap   length       size
		copy      getAs        getDecimal   getInt       getLong        getString   hashCode       mkString     toSeq
		equals    getBoolean   getDouble    getJavaMap   getMap         getStruct   isNullAt       prettyJson   toString

		scala> postsDS.first.
		AnswerCount   OwnerUserId   Score       canEqual   equals     productArity     productIterator   toString
		Id            PostTypeId    ViewCount   copy       hashCode   productElement   productPrefix

		scala>
		
		scala> spark.sql("select Score from PostsSE").show()
		+-----+
		|Score|
		+-----+
		|   45|
		|   91|
		|   42|
		|   51|
		|   22|
		|   40|
		|   14|
		|    9|
		|   15|
		|   30|
		|   58|
		|   10|
		|   17|
		|  185|
		|   10|
		|   69|
		|   38|
		|   16|
		|   52|
		|   13|
		+-----+
		only showing top 20 rows


		---------------------------------------------------------------------------------------------------------
		for Spark SQL
			- SQL syntax error is not caught at compile time
			- SQL analysis error is also not caught at run time
		---------------------------------------------------------------------------------------------------------

		scala> spark.sql("selct Score from PostsSE").show()
		org.apache.spark.sql.catalyst.parser.ParseException:
		mismatched input 'selct' expecting {'(', 'ADD', 'ALTER', 'ANALYZE', 'CACHE', 'CLEAR', 'COMMENT', 'COMMIT', 'CREATE', 'DELETE', 'DESC', 'DESCRIBE', 'DFS', 'DROP', 'EXPLAIN', 'EXPORT', 'FROM', 'GRANT', 'IMPORT', 'INSERT', 'LIST', 'LOAD', 'LOCK', 'MAP', 'MERGE', 'MSCK', 'REDUCE', 'REFRESH', 'REPLACE', 'RESET', 'REVOKE', 'ROLLBACK', 'SELECT', 'SET', 'SHOW', 'START', 'TABLE', 'TRUNCATE', 'UNCACHE', 'UNLOCK', 'UPDATE', 'USE', 'VALUES', 'WITH'}(line 1, pos 0)

		== SQL ==
		selct Score from PostsSE
		^^^

		  at org.apache.spark.sql.catalyst.parser.ParseException.withCommand(ParseDriver.scala:266)
		  at org.apache.spark.sql.catalyst.parser.AbstractSqlParser.parse(ParseDriver.scala:133)
		  at org.apache.spark.sql.execution.SparkSqlParser.parse(SparkSqlParser.scala:48)
		  at org.apache.spark.sql.catalyst.parser.AbstractSqlParser.parsePlan(ParseDriver.scala:81)
		  at org.apache.spark.sql.SparkSession.$anonfun$sql$2(SparkSession.scala:604)
		  at org.apache.spark.sql.catalyst.QueryPlanningTracker.measurePhase(QueryPlanningTracker.scala:111)
		  at org.apache.spark.sql.SparkSession.$anonfun$sql$1(SparkSession.scala:604)
		  at org.apache.spark.sql.SparkSession.withActive(SparkSession.scala:763)
		  at org.apache.spark.sql.SparkSession.sql(SparkSession.scala:601)
		  ... 51 elided

		scala> spark.sql("select Scre from PostsSE").show()
		org.apache.spark.sql.AnalysisException: cannot resolve '`Scre`' given input columns: [postsse.AnswerCount, postsse.Id, postsse.OwnerUserId, postsse.PostTypeId, postsse.Score, postsse.ViewCount]; line 1 pos 7;
		'Project ['Scre]
		+- SubqueryAlias postsse
		   +- Filter AtLeastNNulls(n, ViewCount#909)
			  +- Project [Id#904, PostTypeId#905, Score#908, ViewCount#909, AnswerCount#916, OwnerUserId#910]
				 +- Relation[Id#904,PostTypeId#905,AcceptedAnswerId#906,CreationDate#907,Score#908,ViewCount#909,OwnerUserId#910,LastEditorUserId#911,LastEditDate#912,Title#913,LastActivityDate#914,Tags#915,AnswerCount#916,CommentCount#917,FavoriteCount#918] csv

		  at org.apache.spark.sql.catalyst.analysis.package$AnalysisErrorAt.failAnalysis(package.scala:42)
		  at org.apache.spark.sql.catalyst.analysis.CheckAnalysis$$anonfun$$nestedInanonfun$checkAnalysis$1$2.applyOrElse(CheckAnalysis.scala:143)
		  at org.apache.spark.sql.catalyst.analysis.CheckAnalysis$$anonfun$$nestedInanonfun$checkAnalysis$1$2.applyOrElse(CheckAnalysis.scala:140)
		  at org.apache.spark.sql.catalyst.trees.TreeNode.$anonfun$transformUp$2(TreeNode.scala:333)
		  at org.apache.spark.sql.catalyst.trees.CurrentOrigin$.withOrigin(TreeNode.scala:72)
		  at org.apache.spark.sql.catalyst.trees.TreeNode.transformUp(TreeNode.scala:333)
		  at org.apache.spark.sql.catalyst.plans.QueryPlan.$anonfun$transformExpressionsUp$1(QueryPlan.scala:106)
		  at org.apache.spark.sql.catalyst.plans.QueryPlan.$anonfun$mapExpressions$1(QueryPlan.scala:118)
		  at org.apache.spark.sql.catalyst.trees.CurrentOrigin$.withOrigin(TreeNode.scala:72)
		  at org.apache.spark.sql.catalyst.plans.QueryPlan.transformExpression$1(QueryPlan.scala:118)
		  at org.apache.spark.sql.catalyst.plans.QueryPlan.recursiveTransform$1(QueryPlan.scala:129)
		  at org.apache.spark.sql.catalyst.plans.QueryPlan.$anonfun$mapExpressions$3(QueryPlan.scala:134)
		  at scala.collection.TraversableLike.$anonfun$map$1(TraversableLike.scala:238)
		  at scala.collection.immutable.List.foreach(List.scala:392)
		  at scala.collection.TraversableLike.map(TraversableLike.scala:238)
		  at scala.collection.TraversableLike.map$(TraversableLike.scala:231)
		  at scala.collection.immutable.List.map(List.scala:298)
		  at org.apache.spark.sql.catalyst.plans.QueryPlan.recursiveTransform$1(QueryPlan.scala:134)
		  at org.apache.spark.sql.catalyst.plans.QueryPlan.$anonfun$mapExpressions$4(QueryPlan.scala:139)
		  at org.apache.spark.sql.catalyst.trees.TreeNode.mapProductIterator(TreeNode.scala:237)
		  at org.apache.spark.sql.catalyst.plans.QueryPlan.mapExpressions(QueryPlan.scala:139)
		  at org.apache.spark.sql.catalyst.plans.QueryPlan.transformExpressionsUp(QueryPlan.scala:106)
		  at org.apache.spark.sql.catalyst.analysis.CheckAnalysis.$anonfun$checkAnalysis$1(CheckAnalysis.scala:140)
		  at org.apache.spark.sql.catalyst.analysis.CheckAnalysis.$anonfun$checkAnalysis$1$adapted(CheckAnalysis.scala:92)
		  at org.apache.spark.sql.catalyst.trees.TreeNode.foreachUp(TreeNode.scala:177)
		  at org.apache.spark.sql.catalyst.analysis.CheckAnalysis.checkAnalysis(CheckAnalysis.scala:92)
		  at org.apache.spark.sql.catalyst.analysis.CheckAnalysis.checkAnalysis$(CheckAnalysis.scala:89)
		  at org.apache.spark.sql.catalyst.analysis.Analyzer.checkAnalysis(Analyzer.scala:130)
		  at org.apache.spark.sql.catalyst.analysis.Analyzer.$anonfun$executeAndCheck$1(Analyzer.scala:156)
		  at org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper$.markInAnalyzer(AnalysisHelper.scala:201)
		  at org.apache.spark.sql.catalyst.analysis.Analyzer.executeAndCheck(Analyzer.scala:153)
		  at org.apache.spark.sql.execution.QueryExecution.$anonfun$analyzed$1(QueryExecution.scala:68)
		  at org.apache.spark.sql.catalyst.QueryPlanningTracker.measurePhase(QueryPlanningTracker.scala:111)
		  at org.apache.spark.sql.execution.QueryExecution.$anonfun$executePhase$1(QueryExecution.scala:133)
		  at org.apache.spark.sql.SparkSession.withActive(SparkSession.scala:763)
		  at org.apache.spark.sql.execution.QueryExecution.executePhase(QueryExecution.scala:133)
		  at org.apache.spark.sql.execution.QueryExecution.analyzed$lzycompute(QueryExecution.scala:68)
		  at org.apache.spark.sql.execution.QueryExecution.analyzed(QueryExecution.scala:66)
		  at org.apache.spark.sql.execution.QueryExecution.assertAnalyzed(QueryExecution.scala:58)
		  at org.apache.spark.sql.Dataset$.$anonfun$ofRows$2(Dataset.scala:99)
		  at org.apache.spark.sql.SparkSession.withActive(SparkSession.scala:763)
		  at org.apache.spark.sql.Dataset$.ofRows(Dataset.scala:97)
		  at org.apache.spark.sql.SparkSession.$anonfun$sql$1(SparkSession.scala:606)
		  at org.apache.spark.sql.SparkSession.withActive(SparkSession.scala:763)
		  at org.apache.spark.sql.SparkSession.sql(SparkSession.scala:601)
		  ... 51 elided
		  
		---------------------------------------------------------------------------------------------------------
		for DataFrame
			- SQL syntax error is now caught at compile time
			- But SQL analysis error is still not caught at run time
		---------------------------------------------------------------------------------------------------------

		scala> postsDF.selct("Score").show(5)
		<console>:33: error: value selct is not a member of org.apache.spark.sql.DataFrame
			   postsDF.selct("Score").show(5)
					   ^

		scala> postsDF.select("Score").show(5)
		+-----+
		|Score|
		+-----+
		|   45|
		|   91|
		|   42|
		|   51|
		|   22|
		+-----+
		only showing top 5 rows


		scala> postsDF.select("Scre").show(5)
		org.apache.spark.sql.AnalysisException: cannot resolve '`Scre`' given input columns: [AnswerCount, Id, OwnerUserId, PostTypeId, Score, ViewCount];;
		'Project ['Scre]
		+- Filter AtLeastNNulls(n, ViewCount#909)
		   +- Project [Id#904, PostTypeId#905, Score#908, ViewCount#909, AnswerCount#916, OwnerUserId#910]
			  +- Relation[Id#904,PostTypeId#905,AcceptedAnswerId#906,CreationDate#907,Score#908,ViewCount#909,OwnerUserId#910,LastEditorUserId#911,LastEditDate#912,Title#913,LastActivityDate#914,Tags#915,AnswerCount#916,CommentCount#917,FavoriteCount#918] csv

		  at org.apache.spark.sql.catalyst.analysis.package$AnalysisErrorAt.failAnalysis(package.scala:42)
		  at org.apache.spark.sql.catalyst.analysis.CheckAnalysis$$anonfun$$nestedInanonfun$checkAnalysis$1$2.applyOrElse(CheckAnalysis.scala:143)
		  at org.apache.spark.sql.catalyst.analysis.CheckAnalysis$$anonfun$$nestedInanonfun$checkAnalysis$1$2.applyOrElse(CheckAnalysis.scala:140)
		  at org.apache.spark.sql.catalyst.trees.TreeNode.$anonfun$transformUp$2(TreeNode.scala:333)
		  at org.apache.spark.sql.catalyst.trees.CurrentOrigin$.withOrigin(TreeNode.scala:72)
		  at org.apache.spark.sql.catalyst.trees.TreeNode.transformUp(TreeNode.scala:333)
		  at org.apache.spark.sql.catalyst.plans.QueryPlan.$anonfun$transformExpressionsUp$1(QueryPlan.scala:106)
		  at org.apache.spark.sql.catalyst.plans.QueryPlan.$anonfun$mapExpressions$1(QueryPlan.scala:118)
		  at org.apache.spark.sql.catalyst.trees.CurrentOrigin$.withOrigin(TreeNode.scala:72)
		  at org.apache.spark.sql.catalyst.plans.QueryPlan.transformExpression$1(QueryPlan.scala:118)
		  at org.apache.spark.sql.catalyst.plans.QueryPlan.recursiveTransform$1(QueryPlan.scala:129)
		  at org.apache.spark.sql.catalyst.plans.QueryPlan.$anonfun$mapExpressions$3(QueryPlan.scala:134)
		  at scala.collection.TraversableLike.$anonfun$map$1(TraversableLike.scala:238)
		  at scala.collection.mutable.ResizableArray.foreach(ResizableArray.scala:62)
		  at scala.collection.mutable.ResizableArray.foreach$(ResizableArray.scala:55)
		  at scala.collection.mutable.ArrayBuffer.foreach(ArrayBuffer.scala:49)
		  at scala.collection.TraversableLike.map(TraversableLike.scala:238)
		  at scala.collection.TraversableLike.map$(TraversableLike.scala:231)
		  at scala.collection.AbstractTraversable.map(Traversable.scala:108)
		  at org.apache.spark.sql.catalyst.plans.QueryPlan.recursiveTransform$1(QueryPlan.scala:134)
		  at org.apache.spark.sql.catalyst.plans.QueryPlan.$anonfun$mapExpressions$4(QueryPlan.scala:139)
		  at org.apache.spark.sql.catalyst.trees.TreeNode.mapProductIterator(TreeNode.scala:237)
		  at org.apache.spark.sql.catalyst.plans.QueryPlan.mapExpressions(QueryPlan.scala:139)
		  at org.apache.spark.sql.catalyst.plans.QueryPlan.transformExpressionsUp(QueryPlan.scala:106)
		  at org.apache.spark.sql.catalyst.analysis.CheckAnalysis.$anonfun$checkAnalysis$1(CheckAnalysis.scala:140)
		  at org.apache.spark.sql.catalyst.analysis.CheckAnalysis.$anonfun$checkAnalysis$1$adapted(CheckAnalysis.scala:92)
		  at org.apache.spark.sql.catalyst.trees.TreeNode.foreachUp(TreeNode.scala:177)
		  at org.apache.spark.sql.catalyst.analysis.CheckAnalysis.checkAnalysis(CheckAnalysis.scala:92)
		  at org.apache.spark.sql.catalyst.analysis.CheckAnalysis.checkAnalysis$(CheckAnalysis.scala:89)
		  at org.apache.spark.sql.catalyst.analysis.Analyzer.checkAnalysis(Analyzer.scala:130)
		  at org.apache.spark.sql.catalyst.analysis.Analyzer.$anonfun$executeAndCheck$1(Analyzer.scala:156)
		  at org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper$.markInAnalyzer(AnalysisHelper.scala:201)
		  at org.apache.spark.sql.catalyst.analysis.Analyzer.executeAndCheck(Analyzer.scala:153)
		  at org.apache.spark.sql.execution.QueryExecution.$anonfun$analyzed$1(QueryExecution.scala:68)
		  at org.apache.spark.sql.catalyst.QueryPlanningTracker.measurePhase(QueryPlanningTracker.scala:111)
		  at org.apache.spark.sql.execution.QueryExecution.$anonfun$executePhase$1(QueryExecution.scala:133)
		  at org.apache.spark.sql.SparkSession.withActive(SparkSession.scala:763)
		  at org.apache.spark.sql.execution.QueryExecution.executePhase(QueryExecution.scala:133)
		  at org.apache.spark.sql.execution.QueryExecution.analyzed$lzycompute(QueryExecution.scala:68)
		  at org.apache.spark.sql.execution.QueryExecution.analyzed(QueryExecution.scala:66)
		  at org.apache.spark.sql.execution.QueryExecution.assertAnalyzed(QueryExecution.scala:58)
		  at org.apache.spark.sql.Dataset$.$anonfun$ofRows$1(Dataset.scala:91)
		  at org.apache.spark.sql.SparkSession.withActive(SparkSession.scala:763)
		  at org.apache.spark.sql.Dataset$.ofRows(Dataset.scala:89)
		  at org.apache.spark.sql.Dataset.org$apache$spark$sql$Dataset$$withPlan(Dataset.scala:3644)
		  at org.apache.spark.sql.Dataset.select(Dataset.scala:1456)
		  at org.apache.spark.sql.Dataset.select(Dataset.scala:1473)
		  ... 51 elided
		  
		---------------------------------------------------------------------------------------------------------
		for Dataset
			- SQL syntax error is caught at compile time
			- SQL analysis error is also caught at run time
		---------------------------------------------------------------------------------------------------------

		scala> postsDS.first.Scre
		<console>:33: error: value Scre is not a member of Post
			   postsDS.first.Scre
							 ^

		scala> val firstScore: String = postsDS.first.Score
		<console>:32: error: type mismatch;
		 found   : Integer
		 required: String
			   val firstScore: String = postsDS.first.Score
													  ^

		scala>
		
	3 - What's a Dataset?
		
		- strongly typed collection of objects
		- domain specific objects
		- type safe
		- functional programming
		- query optimization
		- API (more than just a ROW in SQL)
		
		- Dataset is a class
		- DataFrame is not a class, but a type in Scala
		
	4 - What Do you Need for Dataset?
		
		scala> case class Post(Id: Integer, UserId: String, Score: Integer)
		defined class Post

		scala> val post = Post(1, "1", 25)
		post: Post = Post(1,1,25)

		scala> post.
		Id      UserId     copy     hashCode       productElement    productPrefix
		Score   canEqual   equals   productArity   productIterator   toString

		scala> post.Id
		res0: Integer = 1

		scala> post.UserId
		res1: String = 1

		scala> post.Score
		res2: Integer = 25

		scala>
		
		case class is mutable
		
		scala> post.UserId = "2"
		<console>:25: error: reassignment to val
			   post.UserId = "2"
						   ^

		scala>
		
	5 - Creating Dataset
		
		- .toDS()
			- create Dataset from memory
			
		- .createDataset()
			- create Dataset from RDD
			
		- .as[CaseClass of some DataFrame]
			- create Dataset from DataFrame
			
		
		- create Dataset from memory -----------------------------------------------------------
		
		scala> val primitiveDS = Seq(10, 20, 30).toDS()
		primitiveDS: org.apache.spark.sql.Dataset[Int] = [value: int]

		scala> primitiveDS.map(x => x + 1).show()
		+-----+
		|value|
		+-----+
		|   11|
		|   21|
		|   31|
		+-----+


		scala> val complexDS = Seq(("Xavier", 1), ("Irene", 2)).toDS()
		complexDS: org.apache.spark.sql.Dataset[(String, Int)] = [_1: string, _2: int]

		scala> complexDS.show()
		+------+---+
		|    _1| _2|
		+------+---+
		|Xavier|  1|
		| Irene|  2|
		+------+---+


		scala> complexDS.filter(x => x._1 == "Xavier").show()
		+------+---+
		|    _1| _2|
		+------+---+
		|Xavier|  1|
		+------+---+


		scala>
		
		- create Dataset from RDD -----------------------------------------------------------
		
		scala> val postsRDD = sc.textFile("/user/cloudera/stackexchange/simple_titles_txt")
		postsRDD: org.apache.spark.rdd.RDD[String] = /user/cloudera/stackexchange/simple_titles_txt MapPartitionsRDD[7] at textFile at <console>:24

		scala> val postsDSfromRDD = spark.createDataset(postsRDD)
		postsDSfromRDD: org.apache.spark.sql.Dataset[String] = [value: string]

		scala> postsDSfromRDD.show(3)
		+--------------------+
		|               value|
		+--------------------+
		|How can I add lin...|
		|                    |
		|How can I show re...|
		+--------------------+
		only showing top 3 rows


		scala>
		
		- create Dataset from DataFrame -----------------------------------------------------------
		
		scala> import org.apache.spark.sql.types._
		import org.apache.spark.sql.types._

		scala> val postsSchema =
			 |   StructType(Array(
			 |               StructField("Id", IntegerType),
			 | StructField("PostTypeId", IntegerType),
			 | StructField("AcceptedAnswerId", IntegerType),
			 | StructField("CreationDate", TimestampType),
			 | StructField("Score", IntegerType),
			 | StructField("ViewCount", IntegerType),
			 | StructField("OwnerUserId", IntegerType),
			 | StructField("LastEditorUserId", IntegerType),
			 | StructField("LastEditDate", TimestampType),
			 | StructField("Title", StringType),
			 | StructField("LastActivityDate", TimestampType),
			 | StructField("Tags", StringType),
			 | StructField("AnswerCount", IntegerType),
			 | StructField("CommentCount", IntegerType),
			 | StructField("FavoriteCount", IntegerType)))
		postsSchema: org.apache.spark.sql.types.StructType = StructType(StructField(Id,IntegerType,true), StructField(PostTypeId,IntegerType,true), StructField(AcceptedAnswerId,IntegerType,true), StructField(CreationDate,TimestampType,true), StructField(Score,IntegerType,true), StructField(ViewCount,IntegerType,true), StructField(OwnerUserId,IntegerType,true), StructField(LastEditorUserId,IntegerType,true), StructField(LastEditDate,TimestampType,true), StructField(Title,StringType,true), StructField(LastActivityDate,TimestampType,true), StructField(Tags,StringType,true), StructField(AnswerCount,IntegerType,true), StructField(CommentCount,IntegerType,true), StructField(FavoriteCount,IntegerType,true))

		scala> case class Post(Id: Int, PostTypeId: Int, Score: Integer, ViewCount: Integer, AnswerCount: Integer, OwnerUserId:
		Integer)
		defined class Post

		scala> val posts_all = spark.read.schema(postsSchema).csv("/user/cloudera/stackexchange/posts_all_csv")
		posts_all: org.apache.spark.sql.DataFrame = [Id: int, PostTypeId: int ... 13 more fields]

		scala> val postsDF = posts_all.select($"Id", $"PostTypeId", $"Score", $"ViewCount", $"AnswerCount", $"OwnerUserId").na.drop("any", Seq("ViewCount"))
		postsDF: org.apache.spark.sql.DataFrame = [Id: int, PostTypeId: int ... 4 more fields]

		scala> postsDF.printSchema
		root
		 |-- Id: integer (nullable = true)
		 |-- PostTypeId: integer (nullable = true)
		 |-- Score: integer (nullable = true)
		 |-- ViewCount: integer (nullable = true)
		 |-- AnswerCount: integer (nullable = true)
		 |-- OwnerUserId: integer (nullable = true)

		scala> val postsDFfromDF = postsDF.as[Post]
		postsDFfromDF: org.apache.spark.sql.Dataset[Post] = [Id: int, PostTypeId: int ... 4 more fields]

		scala>
		
		scala> val postsDSfromDF = postsDF.as[Post]
		postsDSfromDF: org.apache.spark.sql.Dataset[Post] = [Id: int, PostTypeId: int ... 4 more fields]

		scala> postsDSfromDF.groupByKey(row => row.OwnerUserId).count().show()
		+-----+--------+
		|  key|count(1)|
		+-----+--------+
		|  496|      13|
		|  833|       1|
		| 6357|       2|
		| 9376|       1|
		|11858|       1|
		|13285|       1|
		|16503|       1|
		| 5803|       7|
		|17420|       1|
		|19984|       3|
		|20683|       1|
		|20735|       1|
		|22097|       1|
		|21700|       2|
		|23336|       1|
		| 1238|       1|
		|26425|       1|
		|26755|       2|
		|29719|       1|
		|30903|       1|
		+-----+--------+
		only showing top 20 rows


		scala>
		
	6 - Dataset Operations
		
		- typed transformation -------------------------------------------------
		
		scala> posts_all
		res17: org.apache.spark.sql.DataFrame = [Id: int, PostTypeId: int ... 13 more fields]

		scala> val postsDS = posts_all.select($"Id", $"PostTypeId", $"Score", $"ViewCount", $"AnswerCount", $"OwnerUserId").as[Post]
		postsDS: org.apache.spark.sql.Dataset[Post] = [Id: int, PostTypeId: int ... 4 more fields]

		scala> postsDS
		res18: org.apache.spark.sql.Dataset[Post] = [Id: int, PostTypeId: int ... 4 more fields]

		scala> val postsLessDS = postsDS.filter('ViewCount < 533)
		postsLessDS: org.apache.spark.sql.Dataset[Post] = [Id: int, PostTypeId: int ... 4 more fields]

		scala> postsLessDS.count()
		res19: Long = 8013

		scala> val postsNotDS = postsDS.select('Id, 'ViewCount)
		postsNotDS: org.apache.spark.sql.DataFrame = [Id: int, ViewCount: int]

		scala
		
		DataFrame -> Dataset -> DataFrame
		posts_all -> postsDS -> postsNotDS
		
		- untyped transformation -------------------------------------------------
		
		scala> postsDS.describe("ViewCount").show()
		+-------+------------------+
		|summary|         ViewCount|
		+-------+------------------+
		|  count|             12307|
		|   mean|1633.6557243844966|
		| stddev| 9676.480312982949|
		|    min|                 4|
		|    max|            554639|
		+-------+------------------+


		scala> postsDS.filter(p => (p.ViewCount == 533)).show()
		+---+----------+-----+---------+-----------+-----------+
		| Id|PostTypeId|Score|ViewCount|AnswerCount|OwnerUserId|
		+---+----------+-----+---------+-----------+-----------+
		+---+----------+-----+---------+-----------+-----------+


		scala> postsDS.show(5)
		+---+----------+-----+---------+-----------+-----------+
		| Id|PostTypeId|Score|ViewCount|AnswerCount|OwnerUserId|
		+---+----------+-----+---------+-----------+-----------+
		|  1|         1|   45|    10084|          2|          2|
		|  2|         2|   24|     null|       null|          5|
		|  3|         1|   91|    45278|          4|         11|
		|  4|         1|   42|    14206|          4|         12|
		|  5|         2|   58|     null|       null|         19|
		+---+----------+-----+---------+-----------+-----------+
		only showing top 5 rows


		scala> postsDS.filter(p => (p.ViewCount == 10084)).show()
		+---+----------+-----+---------+-----------+-----------+
		| Id|PostTypeId|Score|ViewCount|AnswerCount|OwnerUserId|
		+---+----------+-----+---------+-----------+-----------+
		|  1|         1|   45|    10084|          2|          2|
		+---+----------+-----+---------+-----------+-----------+


		scala> postsDS.filter(p => p.OwnerUserId == 51).count()
		res24: Long = 570

		scala> val miniDS = postsDS.map(p => (p.Id, p.Score))
		miniDS: org.apache.spark.sql.Dataset[(Int, Integer)] = [_1: int, _2: int]

		scala> val miniDF = postsDS.select($"Id", $"Score")
		miniDF: org.apache.spark.sql.DataFrame = [Id: int, Score: int]
		
		scala> postsDF.groupBy($"UserId").
		agg   as   avg   count   max   mean   min   pivot   sum   toString

		scala> postsDF.groupBy($"UserId").

		scala>
		
		Dataset -> map() -> Dataset
		
		
		Dataset -> select() -> DataFrame
		
	7 - RDDs vs DataFrames vs Dataset
		
		RDD
			- Lower-level API
				- Unstructured data
				- Fine tune
				- Manage low level details
				- Complex data types
			
		DataFrame
			- Untyped Higher-level API
				- Structured data
				- Semi-structured data
				- "Think in SQL"
				- Performance is key
			
		Dataset
			- Typed Higher-level API
				- Structured data
				- Semi-structured data
				- Type safety
				- Functional API
				
			
11 - Final Takeaway and Continuing the Journey with Spark
		
	1 - Final Takeaway
		
	2 - Continuing the Journey with Spark
		
		- CDH
			- Cloudera Distributed Hadoop
		- Cloudera Altus
			- Platform-as-a-service (PAAS)
		- Cloudera Data Science Workbench
			- Web application for working with Big Data
		- Spark Streaming
			- Structured Streaming
		- TensorFlow
			- Machine learning applications, created by Google
			- TensorFrame, runs on Spark DataFrame
			
	