// Databricks notebook source
// MAGIC %python
// MAGIC dbutils.fs.ls("/mnt/dados/Bronze")

// COMMAND ----------

val path = "dbfs:/mnt/dados/Bronze/dataset_imoveis"
val dados = spark.read.format("Delta").load(path)
display(dados)

// COMMAND ----------

import org.apache.spark.sql.functions.col

// COMMAND ----------

display(dados.select("anuncio.*"))

// COMMAND ----------

display(dados.select("anuncio.*","anuncio.endereco.*"))

// COMMAND ----------

val dados_detalhados = dados.select("anuncio.*","anuncio.endereco.*")
display(dados_detalhados)

// COMMAND ----------

val df_silver = dados_detalhados.drop("caracteristicas", "endereco")
display(df_silver)

// COMMAND ----------

val path = "dbfs:/mnt/dados/Silver/dataset_imoveis"
df_silver.write.format("delta").mode(SaveMode.Overwrite).save(path)

// COMMAND ----------


