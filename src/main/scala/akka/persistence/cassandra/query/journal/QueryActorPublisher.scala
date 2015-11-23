package akka.persistence.cassandra.query.journal

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future}

import akka.actor.{Actor, ActorLogging}
import akka.pattern.pipe
import akka.stream.actor.ActorPublisher
import akka.stream.actor.ActorPublisherMessage.{Cancel, Request, SubscriptionTimeoutExceeded}

//TODO: Optimizations - manage the buffer size efficienly, e.g. based on nr requests, remaining elements etc.
//TODO: Database timeout, retry and failure handling.
//TODO: Write tests for buffer size, delivery buffer etc.
/**
 * Abstract Query publisher. Can be integrated with Akka Streams as a Source.
 * Intended to be extended by concrete Query publisher classes. This class manages the stream lifecycle,
 * live stream updates, refreshInterval, max buffer size and causal consistency given an
 * offset queryable data source. Causality is achieved by only a single request for data in flight
 * at any point in time. Implementer must provide concrete initial state, update operation
 * and end condition.
 *
 * @param refreshInterval Refresh interval.
 * @param maxBufferSize Maximal buffer size.
 * @tparam MessageType Type of message.
 * @tparam State Type of state.
 */
private[journal] abstract class QueryActorPublisher[MessageType, State](
    refreshInterval: Option[FiniteDuration],
    maxBufferSize: Long)
  extends ActorPublisher[MessageType]
  with DeliveryBuffer[MessageType]
  with ActorLogging {

  private[this] case class More(buf: Vector[MessageType])
  private[this] case object Continue

  private[this] val tickTask =
    refreshInterval.map(i => context.system.scheduler.schedule(i, i, self, Continue)(context.dispatcher))

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
  private[this] val starting: Receive = {
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
  private[this] def idle(buffer: Vector[MessageType], state: State): Receive = {
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
  private[this] def requesting(buffer: Vector[MessageType], state: State): Receive = {
    case _: Cancel | SubscriptionTimeoutExceeded => context.stop(self)
    case Request(_) => context.become(requesting(deliverBuf(buffer), state))
    case More(newBuffer) =>
      val (updatedBuffer, updatedState) = updateBuffer(buffer, newBuffer, state)
      context.become(nextBehavior(deliverBuf(updatedBuffer), updatedState, Some(state)))
  }

  // Impure. Uses env and side effects.
  // Decision based on state only and not current behavior.
  private[this] def nextBehavior(
      buffer: Vector[MessageType],
      newState: State,
      oldState: Option[State] = None): Receive =
    if (shouldComplete(buffer, refreshInterval, newState, oldState)) {
      onCompleteThenStop()
      Actor.emptyBehavior
    } else if (shouldRequestMore(buffer, totalDemand, maxBufferSize, newState, oldState)) {
      import context.dispatcher
      requestMore(newState, maxBufferSize - buffer.size)
      requesting(buffer, newState)
    } else {
      idle(buffer, newState)
    }

  private[this] def requestMore(state: State, max: Long)(implicit ec: ExecutionContext): Unit =
    query(state, max).map(More).pipeTo(self)

  private [this] def stateChanged(state: State, oldState: Option[State]): Boolean =
    oldState.fold(true)(state != _)

  private[this] def bufferEmptyAndStateUnchanged(buffer: Vector[MessageType], newState: State, oldState: Option[State] = None) =
    buffer.isEmpty && !stateChanged(newState, oldState)

  // TODO: How aggressively do we want to fill the buffer. Change to totaldemand || ... do keep it full.
  private[this] def shouldRequestMore(buffer: Vector[MessageType], demand: Long, maxBufferSize: Long, newState: State, oldState: Option[State] = None) =
    !bufferEmptyAndStateUnchanged(buffer, newState, oldState) && demand > 0 && buffer.size < maxBufferSize

  private[this] def shouldComplete(buffer: Vector[MessageType], refreshInterval: Option[FiniteDuration], newState: State, oldState: Option[State] = None) =
    bufferEmptyAndStateUnchanged(buffer, newState, oldState) && (!refreshInterval.isDefined || completionCondition(newState))


  /**
   * To be implemented by subclasses to define initial state, query, state update when query result
   * is received and completion condition.
   */
  protected def query(state: State, max: Long): Future[Vector[MessageType]]
  protected def initialState: State
  protected def updateBuffer(buf: Vector[MessageType], newBuf: Vector[MessageType], state: State): (Vector[MessageType], State)
  protected def completionCondition(state: State): Boolean
}
