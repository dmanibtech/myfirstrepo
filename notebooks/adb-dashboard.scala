// Databricks notebook source
// MAGIC %python
// MAGIC 
// MAGIC dbutils.widgets.("X123", "1", [str(x) for x in range(1, 10)])
// MAGIC dbutils.widgets.get("X123")

// COMMAND ----------


import spark.implicits._

case class Order(orderId:Int, orderDate:String, customerId:Int, amount:Int)
case class Customer(id:BigInt, name:String, email:String)



// COMMAND ----------

val customers = spark.read.json("/mnt/salesdata1/customers.json").as[Customer]
val orders = spark.read
                  .option("inferSchema",true)
                  .option("header",true)
                  .option("sep",",")
                  .csv("/mnt/salesdata1/orders.csv").as[Order]



// COMMAND ----------

customers.createOrReplaceTempView("customers")
orders.createOrReplaceTempView("orders")

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC SELECT c.id, c.name, sum(o.amount), count(o.orderId) 
// MAGIC FROM customers c INNER JOIN orders o ON c.id = o.customerId 
// MAGIC GROUP BY c.id, c.name

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC SELECT c.id, c.name, sum(o.amount), count(o.orderId) 
// MAGIC FROM customers c INNER JOIN orders o ON c.id = o.customerId 
// MAGIC GROUP BY c.id, c.name

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC # Databricks
// MAGIC ![Image Title](https://i.vimeocdn.com/portrait/18609368_300x300)

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC # Sales Perf Dashboard

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC \\(c = \\pm\\sqrt{a^2 + b^2} \\)
// MAGIC 
// MAGIC \\( f(\beta)= -Y_t^T X_t \beta + \sum log( 1+{e}^{X_t\bullet\beta}) + \frac{1}{2}\delta^t S_t^{-1}\delta\\)
// MAGIC 
// MAGIC where \\(\delta=(\beta - \mu_{t-1})\\)

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC $$\sum_{i=0}^n i^2 = \frac{(n^2+n)(2n+1)}{6}$$

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC <a href="$./first-notebook">Note book 1 for Basics</a>
// MAGIC 
// MAGIC (
// MAGIC Note: for quick reference
// MAGIC https://math.meta.stackexchange.com/questions/5020/mathjax-basic-tutorial-and-quick-reference
// MAGIC )

// COMMAND ----------

val colorsRDD = sc.parallelize(
	Array(
		(197,27,125), (222,119,174), (241,182,218), (253,244,239), (247,247,247), 
		(230,245,208), (184,225,134), (127,188,65), (77,146,33)))

val colors = colorsRDD.collect()

displayHTML(s"""
<!DOCTYPE html>
<meta charset="utf-8">
<style>

path {
  fill: yellow;
  stroke: #000;
}

circle {
  fill: #fff;
  stroke: #000;
  pointer-events: none;
}

.PiYG .q0-9{fill:rgb${colors(0)}}
.PiYG .q1-9{fill:rgb${colors(1)}}
.PiYG .q2-9{fill:rgb${colors(2)}}
.PiYG .q3-9{fill:rgb${colors(3)}}
.PiYG .q4-9{fill:rgb${colors(4)}}
.PiYG .q5-9{fill:rgb${colors(5)}}
.PiYG .q6-9{fill:rgb${colors(6)}}
.PiYG .q7-9{fill:rgb${colors(7)}}
.PiYG .q8-9{fill:rgb${colors(8)}}

</style>
<body>
<script src="https://cdnjs.cloudflare.com/ajax/libs/d3/3.5.6/d3.min.js"></script>
<script>

var width = 960,
    height = 500;

var vertices = d3.range(100).map(function(d) {
  return [Math.random() * width, Math.random() * height];
});

var svg = d3.select("body").append("svg")
    .attr("width", width)
    .attr("height", height)
    .attr("class", "PiYG")
    .on("mousemove", function() { vertices[0] = d3.mouse(this); redraw(); });

var path = svg.append("g").selectAll("path");

svg.selectAll("circle")
    .data(vertices.slice(1))
  .enter().append("circle")
    .attr("transform", function(d) { return "translate(" + d + ")"; })
    .attr("r", 2);

redraw();

function redraw() {
  path = path.data(d3.geom.delaunay(vertices).map(function(d) { return "M" + d.join("L") + "Z"; }), String);
  path.exit().remove();
  path.enter().append("path").attr("class", function(d, i) { return "q" + (i % 9) + "-9"; }).attr("d", String);
}

</script>
  """)


// COMMAND ----------

