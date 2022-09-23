package part2_event_sourcing

import akka.actor.{Actor, ActorLogging, ActorRef, ActorSystem, Props}
import akka.persistence.PersistentActor

import java.util.Date

object MultiplePersist extends App {

  /*
  Diligent accountant: with every invoice , will persist TMO events
  - a tax record for the fiscal authority
  - an invoice record for personal logs or some auditing authority
   */

  //COMMANDs
  case class Invoice(recipient: String, date: Date, amount: Int)
  //EVENTs
  case class TaxRecord(taxId: String, recordId: Int, date: Date, totalAmount: Int)
  case class InvoiceRecord(invoiceRecordedId: Int, recipient: String, date: Date, amount: Int)

  object DiligentAccountant {
    def props(taxId: String, taxAuthority: ActorRef) : Props = Props(new DiligentAccountant(taxId, taxAuthority))
  }
  class DiligentAccountant(taxId: String, taxAuthority: ActorRef) extends PersistentActor with ActorLogging {

    //state
    var latestTaxRecordId = 0
    var latestInvoiceRecordId = 0

    override def persistenceId: String = "diligent-accountant-3000"

    override def receiveCommand: Receive = {
      case Invoice(recipient,date,amount) =>
        //journal ! TaxRecord
        persist(TaxRecord(taxId, latestTaxRecordId, date, amount/3)) { record => //completed 1st
          taxAuthority ! record
          latestTaxRecordId += 1

          persist("I hearby declare this tax record to be true and complete") { declaration => //completed 3rd
            taxAuthority ! declaration
          }
        }

        persist(InvoiceRecord(latestInvoiceRecordId, recipient, date, amount)) { invoiceRecord =>   //completed 2nd
          taxAuthority ! invoiceRecord
          latestInvoiceRecordId += 1

          persist("I hearby declare this Invoice record to be true and complete") { declaration =>  //completed 4th
            taxAuthority ! declaration
          }
        }
    }

    override def receiveRecover: Receive = {
      case event => log.info("Pending recover implementation")
    }
  }

  class TaxAuthority extends Actor with ActorLogging {
    override def receive: Receive = {
      case message => log.info(s"Received $message")
    }
  }

  val system = ActorSystem("MultiPersistDemo")
  val taxAuthority = system.actorOf(Props[TaxAuthority],"HMRC")
  val accountant = system.actorOf(DiligentAccountant.props("UK52325-87238342",taxAuthority))

  accountant ! Invoice("The Sofa 1 million", new Date,2000)

}
