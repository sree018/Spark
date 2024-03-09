package com.srinu.spark.etl.farmework.drools

import com.srinu.spark.etl.farmework.logging.Logging
import org.kie.api.{KieBase, KieServices}
import org.kie.api.runtime.KieContainer
import org.kie.api.KieBaseConfiguration

class KieSessionUtils extends Logging with Serializable {

  def GetKnowledgeSession(): KieBase = {
    val kieServices: KieServices = KieServices.Factory.get()
    val kieContainer: KieContainer = kieServices.newKieClasspathContainer()
    val config: KieBaseConfiguration = kieServices.newKieBaseConfiguration()
    val kieBase: KieBase = kieContainer.newKieBase(config)
    kieBase
  }
}
