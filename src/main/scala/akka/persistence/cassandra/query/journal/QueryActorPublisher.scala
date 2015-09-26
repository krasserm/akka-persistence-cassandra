package akka.persistence.cassandra.query.journal

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future}

import akka.actor.{Actor, ActorLogging}
import akka.pattern.pipe
import akka.stream.actor.ActorPublisher
import akka.stream.actor.ActorPublisherMessage.{Cancel, Request, SubscriptionTimeoutExceeded}

//TODO: Should next state decision be based on current state?
//TODO: Database timeout and failure handling.
//TODO: Query timeout and retry
//TODO: Write actual tests for buffer size, delivery buffer etc.
//TODO: Test a lot in buffer but not too many demands. - Check
//TODO: Test buffer size limit - Check
//TODO: Buffer size limit - Check
//TODO: Refresh interval - Check
//TODO: Free the implementation of buffer from interface. E.g. we want it to be a set rather than vector sometimes.
//TODO: Causal ordering - if results 100-150 are received before 50-100 the stream must wait. - Check (this should not be an issue if we only have one job)
abstract class QueryActorPublisher[MessageType, State](
    refreshInterval: Option[FiniteDuration],
    maxBufSize: Long)
  extends ActorPublisher[MessageType]
  with DeliveryBuffer[MessageType]
  with BufferOperations[MessageType, State]
  with ActorLogging {

  case class More(buf: Vector[MessageType])
  case object Continue

  val tickTask = refreshInterval.map(i => context.system.scheduler.schedule(i, i, self, Continue)(context.dispatcher))

  override def postStop(): Unit = {
    tickTask.map(_.cancel())
    super.postStop()
  }

  override def receive: Receive = starting()

  /**
   * Initial state. Initialises state and buffer.
   *
   * @return Receive.
   */
  def starting(): Receive = {
    case Request(_) =>
      println("------------------------------------------START------------------------------------------")
      context.become(nextBehavior(Vector.empty[MessageType], initialState))
  }

  /**
   * The stream is idle awaiting either Continue after defined refreshInterval value was reached
   * or Request for more data from subscribers.
   *
   * @param buffer Buffer of values to be delivered to subscribers.
   * @param state Stream state.
   * @return Receive.
   */
  def idle(buffer: Vector[MessageType], state: State): Receive = {
    case Continue =>
      println("IDLE CONTINUE")
      context.become(nextBehavior(buffer, state))

    // TODO: DO we want to check for more on request or wait for update?
    case Request(_) =>
      println("IDLE REQUEST")
      context.become(nextBehavior(deliverBuf(buffer), state))
  }

  /**
   * The stream requested more data and is awaiting the response. It can not leave this state until
   * the response is received to ensure only one request is in flight at any time to ensure causality.
   *
   * @param buffer Buffer of values to be delivered to subscribers.
   * @param state Stream state.
   * @return Receive.
   */
  def streaming(buffer: Vector[MessageType], state: State): Receive = {
    case _: Cancel | SubscriptionTimeoutExceeded =>
      context.stop(self)

    case More(newBuffer) =>
      println("STREAMING MORE")
      val (updatedBuffer, updatedState) = updateBuffer(buffer, newBuffer, state)
      println(s"BUFFER $updatedBuffer")
      context.become(nextBehavior(deliverBuf(updatedBuffer), updatedState, Some(state)))

    case Request(_) =>
      println("STREAMING REQUEST")
      context.become(streaming(deliverBuf(buffer), state))
  }

  private[this] def requestMore(state: State, max: Long)(implicit ec: ExecutionContext): Unit =
    query(state, max).map(More).pipeTo(self)

  private[this] def complete() = onCompleteThenStop()

  private[this] def nextBehavior(
      buffer: Vector[MessageType],
      newState: State,
      oldState: Option[State] = None): Receive = {
    println(s"DECIDING BASED ON BUFFER $buffer, NEWSTATE $newState, OLDSTATE $oldState, DEMAND $totalDemand")
    if(buffer.isEmpty && !fullUpdate(newState, oldState)) {
      if(refreshInterval.isDefined && !completionCondition(newState)) {
        println("BECOMING IDLE")
        idle(buffer, newState)
      } else {
        println("BECOMING END")
        complete()
        Actor.emptyBehavior
      }
      // TODO: How aggressively do we want to fill the buffer. Change to || do keep it full.
    } else if (totalDemand > 0 && buffer.size < maxBufSize) {
      println("BECOMING STREAMING")
      requestMore(newState, maxBufSize - buffer.size)
      streaming(buffer, newState)
    } else {
      println("BECOMING IDLE  ")
      idle(buffer, newState)
    }
  }

  protected def query(state: State, max: Long): Future[Vector[MessageType]]
  protected def initialState: State
  protected def completionCondition(state: State): Boolean
  protected def fullUpdate(state: State, oldState: Option[State]): Boolean
}
