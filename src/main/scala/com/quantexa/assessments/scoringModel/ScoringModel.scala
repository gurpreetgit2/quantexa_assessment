package com.quantexa.assessments.scoringModel

import com.quantexa.assessments.accounts.AccountAssessment.AccountData
import com.quantexa.assessments.customerAddresses.CustomerAddress.AddressData
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import com.quantexa.assessments.accounts.AccountAssessment.CustomerAccountOutput
import com.quantexa.assessments.customerAddresses.CustomerAddress.{
  customerAccountDS => resources
}
import org.apache.spark.sql.Dataset

/** * Part of the Quantexa solution is to flag high risk countries as a link to
  * these countries may be an indication of tax evasion.
  *
  * For this question you are required to populate the flag in the ScoringModel
  * case class where the customer has an address in the British Virgin Islands.
  *
  * This flag must be then used to return the number of customers in the dataset
  * that have a link to a British Virgin Islands address.
  */

object ScoringModel extends App {

  // Create a spark context, using a local master so Spark runs on the local machine
  val spark = SparkSession
    .builder()
    .master("local[*]")
    .appName("ScoringModel")
    .getOrCreate()

  // importing spark implicits allows functions such as dataframe.as[T]

  // Set logger level to Warn
  Logger.getRootLogger.setLevel(Level.WARN)

  import spark.implicits._

  case class CustomerDocument(
      customerId: String,
      forename: String,
      surname: String,
      // Accounts for this customer
      accounts: Seq[AccountData],
      // Addresses for this customer
      address: Seq[AddressData]
  )

  case class ScoringModel(
      customerId: String,
      forename: String,
      surname: String,
      // Accounts for this customer
      accounts: Seq[AccountData],
      // Addresses for this customer
      address: Seq[AddressData],
      linkToBVI: Boolean
  )

  // END GIVEN CODE

  val customerDocumentDS = spark.read
    .parquet("src/main/resources/customerDocumentDS.parquet")
    .as[CustomerDocument]

  val BVIAddress = "British Virgin Islands"

  val scoringModelDS: Dataset[ScoringModel] = customerDocumentDS.map { obj =>
    ScoringModel(
      obj.customerId,
      obj.forename,
      obj.surname,
      obj.accounts,
      obj.address,
      linkToBVI = false // default values as FALSE
    )
  }

  val scoringModelWithAddrBoolDS = scoringModelDS.map { obj =>
    val hasBVIAddress =
      obj.address.exists(addr => addr.address.contains(BVIAddress))

    obj.copy(linkToBVI = hasBVIAddress)
  }

  val countOfBVIAddresses =
    scoringModelWithAddrBoolDS
      .filter { obj =>
        obj.linkToBVI == true
      }
      .count()
  print("\n======= Count Of BVI addresses ========\n")
  print(countOfBVIAddresses)
  print("\n========================================\n")

}
