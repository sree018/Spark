package com.srinu.study.etl.framework.drools

import com.srinu.study.etl.framework.logging.Logging
import com.typesafe.config.Config
import org.apache.spark.sql._
import org.kie.api.{KieBase, KieServices}
import org.kie.api.runtime.KieContainer
import org.kie.api.runtime.StatelessKieSession
import org.kie.internal.command.CommandFactory


case class Transaction(sno:Int,first_name:String,last_name:String,requestAmount:Int,creditScore:Int)


class DroolsRules(spark: SparkSession, config: Config, inputDate: String, drlFilePath: String) extends Logging
  with Serializable {
  private var approved = false
  def applyDrools(): Unit = {

    import spark.implicits._

    val inputData = Seq((1, "John", "Doe", 10000, 568),
      (2, "John", "Greg", 12000, 654),
      (3, "Mary", "Sue", 100, 568),
      (4, "Greg", "Darcy", 1000000, 788),
      (5, "Jane", "Stuart", 10, 788))
    val applicants = inputData.toDF("sno","first_name","last_name","requestAmount","creditScore").as[Transaction]
    applicants.show()
     val rules:KieBase= loadRules()
  }
/*
  public static KieBase loadRules() {
    KieServices kieServices = KieServices.Factory.get();
    KieContainer kieContainer = kieServices.getKieClasspathContainer();

    return kieContainer.getKieBase();
  }
 */
  def loadRules(): KieBase = {
    val kieServices:KieServices = KieServices.Factory.get()
    val kieContainer:KieContainer = kieServices.getKieClasspathContainer()
    kieContainer.getKieBase()
  }
  /*
   public static Applicant applyRules(KieBase base, Applicant a) {
    StatelessKieSession session = base.newStatelessKieSession();
    session.execute(CommandFactory.newInsert(a));
    return a;
  }
   */
  def setApproved(_approved: Boolean): Unit = {
    approved = _approved
  }

  def applyRules( base:KieBase, trans:Transaction):Transaction={
    val session:StatelessKieSession = base.newStatelessKieSession()
    session.execute(CommandFactory.newInsert(trans))
    trans
  }
}