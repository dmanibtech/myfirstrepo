// Databricks notebook source
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.{LabeledPoint, LinearRegressionWithSGD}

// COMMAND ----------

case class Person(rating: String, income: Double, age: Int)

// COMMAND ----------

def prepareFeatures(people: Seq[Person]): Seq[org.apache.spark.mllib.linalg.Vector] = {
  val maxIncome = people map(_ income) max
  val maxAge = people map(_ age) max

  people map (p =>
    Vectors dense(
      if (p.rating == "A") 0.7 else if (p.rating == "B") 0.5 else 0.3,
      p.income / maxIncome,
      p.age.toDouble / maxAge))}

// COMMAND ----------

def prepareFeaturesWithLabels(features: Seq[org.apache.spark.mllib.linalg.Vector]): Seq[LabeledPoint] = (0d to 1 by (1d / features.length)) zip(features) map(l => LabeledPoint(l._1, l._2))

// COMMAND ----------

val people = List(
  Person("C", 1000, 50),
  Person("C", 1000, 55),
  Person("C", 1500, 60),
  Person("C", 650, 65),
  Person("C", 1200, 70),
  Person("C", 1000, 75),
  Person("C", 500, 80),
  Person("C", 600, 85),
  Person("B", 1000, 50),
  Person("B", 1000, 55),
  Person("B", 1200, 60),
  Person("B", 1500, 65),
  Person("B", 650, 70),
  Person("B", 500, 75),
  Person("B", 700, 80),
  Person("A", 500, 50),
  Person("A", 1200, 55),
  Person("A", 600, 60),
  Person("A", 700, 65),
  Person("A", 800, 70),
  Person("A", 978, 75))


// COMMAND ----------

  val data = sc.parallelize(prepareFeaturesWithLabels(prepareFeatures(people)))
val splits = data randomSplit Array(0.8, 0.2)
val training = splits(0) cache
val test = splits(1) cache

// COMMAND ----------

val algorithm = new LinearRegressionWithSGD()
val model = algorithm run training
val prediction = model predict(test map(_ features))
val predictionAndLabel = prediction zip(test map(_ label))


// COMMAND ----------


predictionAndLabel.collect.foreach((result) => println(s"predicted label: ${result._1}, actual label: ${result._2}"))

// COMMAND ----------

predictionAndLabel.collect

// COMMAND ----------

