package part2_event_sourcing

import akka.actor.{ActorLogging, ActorSystem, Props}
import akka.persistence.PersistentActor

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
          log.info(s"Peristed ${e} as invoice ${e.id}, for total amount $amount")
        }
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

  }

  val system = ActorSystem("PersistentSystem")
  val accountant = system.actorOf(Accountant.props, "simpleAccountant")

  for(i <- 1 to 10) {
    accountant ! Accountant.Invoice("The Sofa Company", new Date, i * 1000)
  }

}
