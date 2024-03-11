package com.srinu.study.etl.framework.drools

import com.srinu.study.etl.framework.logging.Logging
import org.kie.api._
import org.kie.api.KieServices
import org.kie.api.builder.{KieFileSystem, ReleaseId}
import org.kie.api.builder.Message.Level

case class Transaction(amount: Double, Product: String)

class KieSessionUtils extends Logging {
  def classify(transaction: Transaction): Unit = {
    val kieServices = KieServices.Factory.get()
    val releaseId: ReleaseId = kieServices.newReleaseId("com.srinu.study.etl.framework", "etl_framework", "1.0.0")
    if (releaseId == null) {
      throw new Exception("Failed to create ReleaseId. Check parameters.")
    } else {
      logger.info(s"ReleaseId created successfully: $releaseId")
    }
    val kieFileSystem: KieFileSystem = kieServices.newKieFileSystem()
    kieFileSystem.write(kieServices.getResources.newClassPathResource("src/main/resources/rulesFile.drl"))
    kieServices.newKieBuilder(kieFileSystem).buildAll()
    val kieContainer = kieServices.newKieContainer(releaseId)
    val kieSession = kieContainer.newStatelessKieSession()
    kieSession.execute(transaction)
  }
}
