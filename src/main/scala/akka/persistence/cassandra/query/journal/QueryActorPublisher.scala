package akka.persistence.cassandra.query.journal

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future}

import akka.actor.{Actor, ActorLogging}
import akka.pattern.pipe
import akka.stream.actor.ActorPublisher
import akka.stream.actor.ActorPublisherMessage.{Cancel, Request, SubscriptionTimeoutExceeded}

//TODO: Optimizations - managing the buffer size efficienly, e.g. based on nr requests etc.
//TODO: Database timeout, retry and failure handling.
//TODO: Write actual tests for buffer size, delivery buffer etc.
//TODO: Test a lot in buffer but not too many demands. - Check
//TODO: Test buffer size limit - Check
//TODO: Buffer size limit - Check
//TODO: Refresh interval - Check
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

  override def receive: Receive = starting

  /**
   * Initial state. Initialises state and buffer.
   *
   * @return Receive.
   */
  val starting: Receive = {
    case Request(_) =>
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
    case _: Cancel | SubscriptionTimeoutExceeded => context.stop(self)
    case Request(_) => context.become(nextBehavior(deliverBuf(buffer), state))
    case Continue => context.become(nextBehavior(buffer, state))
  }

  /**
   * The stream requested more data and is awaiting the response. It can not leave this state until
   * the response is received to ensure only one request is in flight at any time to ensure causality.
   *
   * @param buffer Buffer of values to be delivered to subscribers.
   * @param state Stream state.
   * @return Receive.
   */
  def requesting(buffer: Vector[MessageType], state: State): Receive = {
    case _: Cancel | SubscriptionTimeoutExceeded => context.stop(self)
    case Request(_) => context.become(requesting(deliverBuf(buffer), state))
    case More(newBuffer) =>
      val (updatedBuffer, updatedState) = updateBuffer(buffer, newBuffer, state)
      context.become(nextBehavior(deliverBuf(updatedBuffer), updatedState, Some(state)))
  }

  private[this] def nextBehavior(
      buffer: Vector[MessageType],
      newState: State,
      oldState: Option[State] = None): Receive =
    if(shouldComplete(buffer, newState, oldState)) {
      onCompleteThenStop()
      Actor.emptyBehavior
    } else if(shouldRequestMore(buffer, newState, oldState)) {
      requestMore(newState, maxBufSize - buffer.size)
      requesting(buffer, newState)
    } else {
      idle(buffer, newState)
    }

  private[this] def requestMore(state: State, max: Long)(implicit ec: ExecutionContext): Unit =
    query(state, max).map(More).pipeTo(self)

  private [this] def stateChanged(state: State, oldState: Option[State]): Boolean =
    oldState.fold(true)(state == _)

  private[this] def bufferEmptyAndStateUnchanged(buffer: Vector[MessageType], newState: State, oldState: Option[State] = None) =
    buffer.isEmpty && !stateChanged(newState, oldState)

  // TODO: How aggressively do we want to fill the buffer. Change to totaldemand || ... do keep it full.
  private[this] def shouldRequestMore(buffer: Vector[MessageType], newState: State, oldState: Option[State] = None) =
    !bufferEmptyAndStateUnchanged(buffer, newState, oldState) && totalDemand > 0 && buffer.size < maxBufSize

  private[this] def shouldComplete(buffer: Vector[MessageType], newState: State, oldState: Option[State] = None) =
    bufferEmptyAndStateUnchanged(buffer, newState, oldState) && (!refreshInterval.isDefined || completionCondition(newState))

  protected def query(state: State, max: Long): Future[Vector[MessageType]]
  protected def initialState: State
  protected def completionCondition(state: State): Boolean
}
