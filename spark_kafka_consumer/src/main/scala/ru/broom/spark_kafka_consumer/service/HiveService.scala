package ru.broom.spark_kafka_consumer.service

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.CatalogTableType

class HiveService(session: SparkSession) extends java.io.Serializable {

  def setManagedTable(tableName: String, database: String): Unit = {
    val identifier = TableIdentifier(tableName, Some(database))
    val oldTable = session.sessionState.catalog.getTableMetadata(identifier)
    val newTableType = CatalogTableType.MANAGED
    val alteredTable = oldTable.copy(tableType = newTableType)
    session.sessionState.catalog.alterTable(alteredTable)
  }

  def createDatabaseIfNotExists(dbName: String): Unit = {
    session.sql(s"create database if not exists $dbName;")
  }

}
