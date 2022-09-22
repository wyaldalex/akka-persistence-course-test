package part2_event_sourcing

import akka.actor.{ActorLogging, ActorSystem, Props}
import akka.persistence.PersistentActor
import part2_event_sourcing.PersistentActorsTest.Accountant.Invoice

import java.util.Date

object PersistentActorsTest extends App {

  /*
  Scenario: we have a business and an accountant keep track of the records
   */


  object Accountant {
    //The Command (message) we expect to receive, not gurantee it will be persisted
    case class Invoice(recipient: String, date: Date, amount: Int)
    //Events , these are the actually persisted ones
    case class InvoiceRecorded(id: Int, recipient: String, date: Date, amount: Int)
    case class InvoiceBulk(invoices: List[Invoice])
    //Special Messages for graceful shutdown
    case object Shutdown
    def props : Props = Props(new Accountant)
  }
  class Accountant extends  PersistentActor with ActorLogging {
    import Accountant._
    //actor state:
    var latestInvoiceId = 0
    var totalAmount = 0

    /*
    Unique ID used for journal
     */
    override def persistenceId: String = "simple-accountant-1"

    /*
    The normal receive method
     */
    override def receiveCommand: Receive = {
      case Invoice(recipient,date,amount) =>
        /*
        1) create event to persist
        2) persist the event, then pass the returned callback after event is persisted
        3) we update the state of the actor once the persisted event is done
        * */
        log.info(s"Receive invoice for amount $amount")
        persist(InvoiceRecorded(latestInvoiceId,recipient,date,amount)) { e=>
          //update the state
          //Why is this safe???? Akka persistence gurantees that no other thread will modify the state during this callback
          latestInvoiceId += 1
          totalAmount += amount
          log.info(s"Persisted ${e} as invoice ${e.id}, for total amount $amount")
        }

      case InvoiceBulk(invoices) =>
        /*
        1) create events(plural)
        2) persist all the events
        3) update the actor state when each event is persisted
         */
        val invoicesIds = latestInvoiceId to (latestInvoiceId + invoices.size)
        val events = invoices.zip(invoicesIds).map { pair =>
          val id = pair._2
          val invoice = pair._1
          InvoiceRecorded(id, invoice.recipient, invoice.date, invoice.amount)
        }
        persistAll(events) { e =>
          latestInvoiceId += 1
          totalAmount += e.amount
          log.info(s"Bulk Persisted ${e} as invoice ${e.id}, for total amount ${e.amount}")
        }

      case Shutdown =>
        log.info(s"Warning, Shut Down activated latest invoice: $latestInvoiceId")
        context.stop(self)
      case "print" =>
        log.info(s"Current status is invoice: $latestInvoiceId and total amount: $totalAmount")

    }
    /*
    Handler that will be called on recovery, this only should update the actor state in a replay!
     */
    override def receiveRecover: Receive = {
       //best practice is follow the normal receive state
      case InvoiceRecorded(id,recipient,date,amount) =>
        latestInvoiceId += 1
        totalAmount += amount
        log.info(s"Recovered event $id for recipient $recipient date $date and amount $amount")
    }

    //Override these for error handling when a persistent error occurs
    /*
    this method is called if persisting failed.
    The actor will be STOPPED

    Best practice: start actor again after a while
    (use Backoff supervisor)
     */
    override def onPersistFailure(cause: Throwable, event: Any, seqNr: Long): Unit = {
      log.error(s"Failed to persist event: $event with cause: $cause")
      super.onPersistFailure(cause, event, seqNr)
    }

    /*
    Called if the Journal fails to persist the event
    The actor is resumed
     */
    override def onPersistRejected(cause: Throwable, event: Any, seqNr: Long): Unit = {
      log.error(s"Persist rejected for event: $event with cause: $cause")
      super.onPersistRejected(cause, event, seqNr)
    }

  }

  val system = ActorSystem("PersistentSystem")
  val accountant = system.actorOf(Accountant.props, "simpleAccountant")


  //Inserting one by one
/*
  for(i <- 1 to 10) {
    accountant ! Accountant.Invoice("The Sofa Company", new Date, i * 1000)
  } */
 //Inserting in bulk
  val newInvoices = for (i <- 1 to 5) yield Invoice(s"Awesome Toy 4000 ${i}", new Date, i * 2000)
  //accountant ! Accountant.InvoiceBulk(newInvoices.toList)

  /*
  * NEVER PERSIST EVENTS FROM A FUTURE!!!!
  * BREAKS ENCAPSULATION FOR ACTORS NOOOO!! D:
  *  */
  //Define your own graceful shutdown using context.stop(self)
  //accountant ! Accountant.Shutdown


}
