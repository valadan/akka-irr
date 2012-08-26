package com.valadan.akka

import akka.actor._
import akka.routing.RoundRobinRouter

object irrCalculator extends App {

  calculateWithActors(nrOfWorkers = 15, maxIterationCount = 20, absoluteAccuracy = 1E-7)

  //calculate(maxIterationCount = 20, absoluteAccuracy = 1E-7)

  sealed trait IrrMessage
  case class Calculate(guess: Double, values: Array[Double]) extends IrrMessage
  case class Work(guess: Double, values: Array[Double], attempt: Int) extends IrrMessage
  case class IterationResult(guess: Double, ratio: Double, values: Array[Double], attempt: Int) extends IrrMessage
  case class Result(irr: Double, values: Array[Double], duration: Long)

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

  class Worker extends Actor {

    def receive = {
      case Work(guess, values, attempt) =>
        val ratio = calculateIrrFor(guess, values)
        sender ! IterationResult(guess - ratio, ratio, values, attempt + 1) // perform the work
    }
  }

  class Master(nrOfWorkers: Int,
    maxIterationCount: Int,
    absoluteAccuracy: Double,
    listener: ActorRef) extends Actor {

    val start: Long = System.currentTimeMillis

    val workerRouter = context.actorOf(
      Props[Worker].withRouter(RoundRobinRouter(nrOfWorkers)), name = "workerRouter")

    def receive = {
      case Calculate(guess, values) =>
        workerRouter ! Work(guess, values, 0)
      case IterationResult(guess, ratio, values, attempt) =>
        if (Math.abs(ratio) <= absoluteAccuracy)
          listener ! Result(guess, values, System.currentTimeMillis - start)
        else if (attempt > maxIterationCount)
          listener ! Result(Double.NaN, values, System.currentTimeMillis - start)
        else
          workerRouter ! Work(guess, values, attempt)
    }
  }

  class Listener extends Actor {
    def receive = {
      case Result(irr, values, duration) =>
        println("\n\tIRR approximation: \t\t%s\n\tCalculation time: \t%s"
          .format(irr, duration))
    }
  }

  def calculateWithActors(nrOfWorkers: Int, maxIterationCount: Int, absoluteAccuracy: Double) {
    // Create an Akka system
    val system = ActorSystem("IrrSystem")

    // create the result listener, which will print the result
    val listener = system.actorOf(Props[Listener], name = "listener")

    // create the master
    val master = system.actorOf(Props(new Master(
      nrOfWorkers, maxIterationCount, absoluteAccuracy, listener)),
      name = "master")

    // start the calculation
//    println("\n\tStarting Calculation at: \t%s"
//      .format(System.currentTimeMillis))
    1 to 50000 foreach { _ => master ! Calculate(0.05, Array(-1000, 50, 50, 50, 50, 1035)) }
    1 to 50000 foreach { _ => master ! Calculate(0.05, Array(-1000, 70, 70, 70, 70, 1070)) }
//    println("\n\tFinished Starting Calculation at: \t%s"
//      .format(System.currentTimeMillis))

  }

  def calculate(maxIterationCount: Int, absoluteAccuracy: Double) {
    val start = System.currentTimeMillis
    1 to 50000 foreach { _ =>
      println("\n\tIRR approximation: \t\t%s\n\tCalculation time: \t%s"
        .format(untilLessThan(maxIterationCount, absoluteAccuracy, Array(-1000, 50, 50, 50, 50, 1035)), System.currentTimeMillis - start))
    }
    1 to 50000 foreach { _ =>
      println("\n\tIRR approximation: \t\t%s\n\tCalculation time: \t%s"
        .format(untilLessThan(maxIterationCount, absoluteAccuracy, Array(-1000, 70, 70, 70, 70, 1070)), System.currentTimeMillis - start))
    }
  }

  def untilLessThan(maxIterationCount: Int, absoluteAccuracy: Double, values: Array[Double]): Double = {
    var guess = 0.0
    for (i <- 0 until maxIterationCount) {
      var result = calculateIrrFor(guess, values)
      if (Math.abs(result) <= absoluteAccuracy) {
        return guess - result
      }
      guess = guess - result
    }
    Double.NaN
  }
}