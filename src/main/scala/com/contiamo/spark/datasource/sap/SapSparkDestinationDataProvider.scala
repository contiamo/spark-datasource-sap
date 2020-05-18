package com.contiamo.spark.datasource.sap

import java.util.Properties
import java.util.concurrent.ConcurrentHashMap

import com.sap.conn.jco.ext.{DestinationDataEventListener, DestinationDataProvider, Environment}

object SapSparkDestinationDataProvider {
  protected val destPropertiesMap = new ConcurrentHashMap[String, Properties]()

  def register(options: Map[String, String]): String = {
    // TODO is this robust?
    val destKey = s"SAP-${options.toString.hashCode.toString}"
    val properties = new Properties()
    options.foreach { case (k, v) => properties.setProperty(k, v) }

    destPropertiesMap.putIfAbsent(destKey, properties)

    if (!Environment.isDestinationDataProviderRegistered) {
      val provider = new SapSparkDestinationDataProvider()
      Environment.registerDestinationDataProvider(provider)
    }

    destKey
  }
}

class SapSparkDestinationDataProvider extends DestinationDataProvider {

  override def getDestinationProperties(destKey: String): Properties =
    SapSparkDestinationDataProvider.destPropertiesMap.get(destKey)

  override def supportsEvents(): Boolean = false

  override def setDestinationDataEventListener(
    destinationDataEventListener: DestinationDataEventListener
  ): Unit = {}
}