// Databricks notebook source
// MAGIC %python
// MAGIC dbutils.fs.ls("/mnt/dados/Inbound")

// COMMAND ----------

// DBTITLE 1,Criação da variavel com os dados a partir do json
val path = "dbfs:/mnt/dados/Inbound/dados_brutos_imoveis.json"
val dados = spark.read.json(path)

// COMMAND ----------

// DBTITLE 1,Visualização da variavel com os dados
display(dados)

// COMMAND ----------

// DBTITLE 1,Apagando as colunas imagens e usuário
val dados_anuncio = dados.drop("imagens", "usuario")
display(dados_anuncio)

// COMMAND ----------

// DBTITLE 1,Importando a biblioteca col 
import org.apache.spark.sql.functions.col

// COMMAND ----------

// DBTITLE 1,Incluindo a coluna id
val df_bronze = dados_anuncio.withColumn("id", col("anuncio.id"))
display(df_bronze)

// COMMAND ----------

// DBTITLE 1,Salvando os dados em formato delta após transformação
val path = "dbfs:/mnt/dados/Bronze/dataset_imoveis"
df_bronze.write.format("delta").mode(SaveMode.Overwrite).save(path)

// COMMAND ----------

val df_endereco = df_bronze.withColumn("endereco", col("anuncio.endereco"))
display(df_endereco)

// COMMAND ----------


