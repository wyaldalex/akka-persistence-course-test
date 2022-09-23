package part3_stores_serialization

import akka.actor.{ActorSystem}
import com.typesafe.config.ConfigFactory
import part3_stores_serialization.LocalStores.SimplePersistenActor

//select * from journal;
//select * from public.snapshot;
object PostgressDBJournal extends App {
  val localStoreActorSystem = ActorSystem("postgresSystem", ConfigFactory.load().getConfig("postgresDemo"))
  val persistentActor = localStoreActorSystem.actorOf(SimplePersistenActor.props, "simplePersistentActor")

  for (i <- 1 to 10) {
    persistentActor ! s"I love Akka Batch 1 [${i}]"
  }
  persistentActor ! "print"
  persistentActor ! "snap"

  for (i <- 11 to 20) {
    persistentActor ! s"I love Akka Batch 2 [${i}]"
  }

  persistentActor ! "print"
}
