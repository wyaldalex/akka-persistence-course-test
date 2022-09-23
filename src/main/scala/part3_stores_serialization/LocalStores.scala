package part3_stores_serialization

import akka.actor.{ActorLogging, ActorSystem, Props}
import akka.persistence.{PersistentActor, RecoveryCompleted, SaveSnapshotFailure, SaveSnapshotSuccess, SnapshotOffer}
import com.typesafe.config.ConfigFactory

import scala.util.Success

object LocalStores extends App {

  object SimplePersistenActor {
    def props : Props =  Props(new SimplePersistenActor)
  }
  class SimplePersistenActor extends PersistentActor with ActorLogging {
    //mutable state
    var nMessages = 0
    override def persistenceId: String = "simple-persistent-actor-3000"

    override def receiveCommand: Receive = {
      case "print" =>
        log.info(s"I have peristed $nMessages")
      case "snap" =>
        saveSnapshot(nMessages)
      case SaveSnapshotSuccess(metadata) =>
        log.info(s"Snapshot saved succesfully with metadata $metadata")
      case SaveSnapshotFailure(metadata,cause) =>
        log.info(s"Snapshot failed with metadata $metadata and error $cause")
      case message => {
        persist(message) { _ =>
          log.info(s"Persisting $message")
          nMessages += 1
        }
      }
    }

    override def receiveRecover: Receive = {
      case RecoveryCompleted =>
        log.info("Recovery completed")
      case SnapshotOffer(_, payload: Int) =>
        log.info(s"Recovered snapshot: $payload")
        nMessages = payload
      case message =>
        log.info(s"Recovered $message")
        nMessages += 1
    }

  }

  val localStoreActorSystem = ActorSystem("localStoresSystem", ConfigFactory.load().getConfig("localStores"))
  val persistentActor = localStoreActorSystem.actorOf(SimplePersistenActor.props,"simplePersistentActor")

  for(i <- 1 to 10) {
    persistentActor ! s"I love Akka Batch 1 [${i}]"
  }
  persistentActor ! "print"
  persistentActor ! "snap"

  for (i <- 11 to 20) {
    persistentActor ! s"I love Akka Batch 2 [${i}]"
  }

  persistentActor ! "print"

}
