package part2_event_sourcing

import akka.actor.{ActorLogging, ActorSystem, Props}
import akka.persistence.PersistentActor

import scala.annotation.tailrec

object PersistentActorExercise extends App {


  object ElectionSystemActor {
    //Command
    case class Vote(citizenPid: String, candidate: String)
    case object Result
    //Event
    case class VoteRecorded(id: Int,citizenPid: String, candidate: String)

    def props : Props = Props(new ElectionSystemActor())

    def countResults(votes: List[(String,String)]) : Map[String,Int] = {
      @tailrec
      def go(counter: Int,acc: Map[String,Int]): Map[String,Int] = {
        if (counter >= votes.size) acc
        else go(counter + 1,
          if (acc.contains(votes(counter)._2)) acc + (votes(counter)._2  -> (acc(votes(counter)._2) + 1))
          else acc + (votes(counter)._2 -> 1)
        )

      }
      go(0,Map.empty)
    }

  }
  class ElectionSystemActor extends PersistentActor with ActorLogging {
    import ElectionSystemActor._

    //Persistent Actor State
    var voteNumber : Int = 1
    var votes : Map[String,String] = Map.empty

    override def persistenceId: String = "actor-system-3000"

    override def receiveCommand: Receive = {
      case Vote(citizenPid,candidate) => {
        log.info(s"Processing vote for voter with id $citizenPid")
        if (votes.contains(citizenPid)) {
          log.info(s"Voter Fraud detected, voter with id $citizenPid already voted")
        } else {
          log.info(s"Processing vote for voter id $citizenPid")
          persist(VoteRecorded(voteNumber,citizenPid, candidate)) { voteRecorded =>
            log.info(s"Vote persisted for citizen voter $citizenPid")
            //update the state
            voteNumber += 1
            votes = votes + (citizenPid -> candidate)
          }
        }
      }

      case Result =>
        log.info(s"Total map $votes")
        log.info(s"Total votes $voteNumber")
        log.info( "The results are: " +  countResults(votes.toList))
    }

    override def receiveRecover: Receive = {
      //recover state
      case VoteRecorded(id,citizenPid,candidate) =>
        log.info(s"Recovering vote for citizen voter $citizenPid")
        //update the state
        voteNumber += 1
        votes = votes + (citizenPid -> candidate)
    }

  }

  val actorSystem = ActorSystem("electionSystem")
  val electionSystemActor = actorSystem.actorOf(ElectionSystemActor.props, "electionSystemActor")

  //votes casted
/*
  val votesCasted = (1 to 10).map(id => {
    ElectionSystemActor.Vote(id.toString + "CITIZEN-30001",
      if (id % 3 == 0) "Zamora" else "Lara"
    )
  }).foreach(vote => electionSystemActor ! vote)
*/


  electionSystemActor ! ElectionSystemActor.Result

}
