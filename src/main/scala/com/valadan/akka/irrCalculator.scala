package com.valadan.akka

import java.util.concurrent.CountDownLatch
import scala.collection.mutable.ListBuffer
import akka.actor._
import akka.routing.RoundRobinRouter
import akka.routing.Broadcast

object irrCalculator extends App {

  calculateWithActors(nrOfWorkers = 12, nrOfMessages=500000, maxIterationCount = 20, absoluteAccuracy = 1E-7)

  sealed trait IrrMessage
  case class Calculate(maxIterationCount: Int, absoluteAccuracy: Double, values: Array[Double]) extends IrrMessage
  case class WorkResult(maxIterationCount: Int, absoluteAccuracy: Double, values: Array[Double]) extends IrrMessage
  case class Result(irr: Double, values: Array[Double]) extends IrrMessage
  case class ShutdownWorkers extends IrrMessage
  case class ShutdownListener extends IrrMessage

  def untilLessThan(maxIterationCount: Int, absoluteAccuracy: Double, values: Array[Double]): Double = {
    var guess = 0.05
    for (i <- 0 until maxIterationCount) {
      var result = calculateIrrFor(guess, values)
      if (Math.abs(result) <= absoluteAccuracy) {
        return guess - result
      }
      guess = guess - result
    }
    Double.NaN
  }
  
  def calculateIrrFor(guess: Double, values: Array[Double]): Double = {

    // the value of the function (NPV) and its derivate can be calculated in the same loop
    var fValue = 0.0;
    var fDerivative = 0.0;
    for (i <- 0 until values.length) {
      fValue += values(i) / Math.pow(1.0 + guess, i);
      fDerivative += -i * values(i) / Math.pow(1.0 + guess, i + 1);
    }
    fValue / fDerivative

  }

  class Worker() extends Actor {
    
    def receive = {
      case WorkResult(maxIterationCount, absoluteAccuracy, values) =>      
        sender ! Result(untilLessThan(maxIterationCount, absoluteAccuracy, values), values) // perform the work
      case ShutdownWorkers =>
      	sender ! ShutdownWorkers
    }
  }

  class Master(nrOfWorkers: Int,
    nrOfMessages: Int,
    maxIterationCount: Int,
    absoluteAccuracy: Double,
    listener: ActorRef, 
    latch: CountDownLatch) extends Actor {

    val workerRouter = context.actorOf(
      Props[Worker].withRouter(RoundRobinRouter(nrOfWorkers)), name = "workerRouter")
      
    var duration = System.currentTimeMillis
    var nrOfResults = 0;

    def receive = {
      case Calculate(maxIterationCount, absoluteAccuracy, values) =>
        workerRouter ! WorkResult(maxIterationCount, absoluteAccuracy, values)
      case Result(irr, values) =>
        listener ! Result(irr, values)
        nrOfResults=nrOfResults+1
        if(nrOfResults==nrOfMessages) {
	        workerRouter ! ShutdownWorkers
        }
      case ShutdownWorkers =>
        workerRouter ! Broadcast(Actors.poisonPill)
		workerRouter ! Actors.poisonPill	
		listener ! ShutdownListener
      case ShutdownListener =>
        listener ! Actors.poisonPill
        context.stop(self)     
    }
    
    override def  postStop = {
      duration = System.currentTimeMillis - duration
      println("\n\tCalculation complete: \t\t%s"
          .format(duration))
      latch.countDown
    }
      
  }

  class Listener(buf: ListBuffer[Double]) extends Actor {
    def receive = {
      case Result(irr, values) =>
        buf.append(irr)
      case ShutdownListener =>
        sender ! ShutdownListener
    }
  }

  def calculateWithActors(nrOfWorkers: Int, nrOfMessages:Int, maxIterationCount: Int, absoluteAccuracy: Double) {
	
    val latch = new CountDownLatch(1)
    // Create an Akka system
    val system = ActorSystem("IrrSystem")
    
    val buf = ListBuffer.empty[Double]
    
    // create the result listener, which will print the result
    val listener = system.actorOf(Props(new Listener(buf)), name = "listener")
    
    // create the master
    val master = system.actorOf(Props(new Master(
      nrOfWorkers, nrOfMessages, maxIterationCount, absoluteAccuracy, listener, latch)),
      name = "master")

    1 to 250000 foreach { _ => master ! Calculate(maxIterationCount, absoluteAccuracy, Array(-1000, 50, 50, 50, 50, 1035)) }
    1 to 250000 foreach { _ => master ! Calculate(maxIterationCount, absoluteAccuracy, Array(-1000, 70, 70, 70, 70, 1070)) }

    latch.await
    
    println("\n\tStopping: \t\t%s"
            		.format(buf.length))
    system.shutdown

  }
}
