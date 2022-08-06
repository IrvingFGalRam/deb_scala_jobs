## In order to work with this Scala Archetype, it is recommended to use IntelliJ and make sure to: 

### 1. Download and erase al other Scala versions but 2.12.14 (in my case ivy2's version)

  - File > Project Structure > Global Libraries > + > Scala SDK > Download > scala-sdk-2.12.14

### 2. Running locally: you have to send the path to read and write via the INPUT_PATH and OUTPUT_PATH environment vars

Executing for the first time right click the .scala and Run 'file.scala'
Then go to Run Configurations > Environment Variables

### 3. Running on GCP Dataproc: you have to create a Compute Engine cluster with the following specs

  - Using [Dataproc - 2.X version](https://cloud.google.com/dataproc/docs/concepts/versioning/dataproc-release-2.0) with Scala 2.12.14 and Spark 3.1.3
  - Setting properties: spark spark.jars.packages com.databricks:spark-xml_2.12:0.13.0,org.apache.spark:spark-mllib_2.12:3.1.3,org.apache.spark:spark-avro_2.12:3.1.3

#### 3.1. You have to compile as a Jar in order to upload to GCS

  - Open the terminal > run the command "sbt package"
  - Upload the scala-jobs_2.12-0.1.1.jar to GCS

### 4. Submit the jobs with the configurations

  - JobType: Spark
  - Main class: org.example.TransformReviewLogs
  - Jar Files: gs://<YOUR_BUKET>/scala-jobs_2.12-0.1.1.jar

### 5. Thats it