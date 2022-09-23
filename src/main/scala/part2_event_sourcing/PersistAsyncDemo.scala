package part2_event_sourcing

import akka.actor.{Actor, ActorLogging, ActorRef, ActorSystem, Props}
import akka.persistence.PersistentActor

object PersistAsyncDemo extends App {

  case class Command(contents: String)
  case class Event(contents: String)

  object CriticalStreamProcessor {
    def props(eventAggregator: ActorRef) = Props(new CriticalStreamProcessor(eventAggregator))
  }
  class CriticalStreamProcessor(eventAggregator: ActorRef) extends PersistentActor with ActorLogging {

    override def persistenceId: String = "critical-stream-processor"

    override def receiveCommand: Receive = {
      case Command(contents) =>
        eventAggregator ! s"Processing $contents"

        persistAsync(Event(contents)) { e =>
          eventAggregator ! e
        }
        //some change
        val processedContents = contents + "_processed"
        persistAsync(Event(processedContents)) { e =>
          eventAggregator ! e
        }
        //Only use persistAsync if you dont care about the order in which the actor state is modified
    }

    override def receiveRecover: Receive = {
      case message => log.info(s"Recovered $message")
    }

  }

  object EventAggregator{
    def props : Props = Props(new EventAggregator)
  }
  class EventAggregator extends Actor with ActorLogging {
    override def receive: Receive = {
      case message => log.info(s"Event Agreggator received: $message")
    }
  }

  val system = ActorSystem("PersistentAsyncDemo")
  val eventAggregator = system.actorOf(EventAggregator.props, "eventagregar")
  val criticalStreamProcessor = system.actorOf(CriticalStreamProcessor.props(eventAggregator), "SuperStreamProcessor")

  criticalStreamProcessor ! Command("First Command ")
  criticalStreamProcessor ! Command("Second Command ")


}
