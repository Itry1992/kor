package kafka.server

import com.tong.kafka.common.errors.{ApiException, InvalidRequestException, UnsupportedCompressionTypeException}
import com.tong.kafka.common.header.internals.RecordHeader
import com.tong.kafka.common.internals.FatalExitError
import com.tong.kafka.common.message.JoinGroupResponseData.JoinGroupResponseMember
import com.tong.kafka.common.message.ListOffsetsResponseData.ListOffsetsTopicResponse
import com.tong.kafka.common.message.MetadataResponseData.{MetadataResponsePartition, MetadataResponseTopic}
import com.tong.kafka.common.message.ProduceRequestData.PartitionProduceData
import com.tong.kafka.common.message._
import com.tong.kafka.common.protocol.{ApiKeys, Errors}
import com.tong.kafka.common.record._
import com.tong.kafka.common.replica.ClientMetadata
import com.tong.kafka.common.replica.ClientMetadata.DefaultClientMetadata
import com.tong.kafka.common.requests.FindCoordinatorRequest.CoordinatorType
import com.tong.kafka.common.requests.ProduceResponse.PartitionResponse
import com.tong.kafka.common.requests._
import com.tong.kafka.common.utils.{ByteUtils, Time}
import com.tong.kafka.common.{Node, TopicIdPartition, TopicPartition, Uuid}
import com.tong.kafka.server.common.MetadataVersion
import com.tong.kafka.tlq.TlqHolder
import com.tongtech.client.consumer.{PullCallback, PullResult, PullStatus}
import com.tongtech.client.consumer.common.PullType
import com.tongtech.client.message.{MessageExt, Message => TlqMessage}
import com.tongtech.client.producer.{SendCallback, SendResult}
import kafka.network.RequestChannel
import kafka.utils.Implicits.MapExtensionMethods
import kafka.utils.Logging

import java.nio.ByteBuffer
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{ConcurrentHashMap, CountDownLatch}
import java.util.{Collections, Optional}
import java.{lang, util}
import scala.collection.mutable.ArrayBuffer
import scala.collection.{immutable, mutable}
import scala.jdk.CollectionConverters._
import scala.jdk.StreamConverters._

class AdapterRequestHandler(val requestChannel: RequestChannel,
                            apiVersionManager: ApiVersionManager,
                            time: Time,
                           ) extends ApiRequestHandler with Logging {
  this.logIdent = "AdapterRequestHandler"
  val MIN_INCREMENTAL_FETCH_SESSION_EVICTION_MS: Long = 120000
  private val fetchManager = new FetchManager(Time.SYSTEM,
    new FetchSessionCache(1000,
      MIN_INCREMENTAL_FETCH_SESSION_EVICTION_MS))
  //对应kafka配置项：message.format.version，3.0默认值：IBP_3_0_IV1
  //用于老版本消息格式兼容
  val messageFormatVersion = MetadataVersion.fromVersionString(MetadataVersion.IBP_3_0_IV1.version)
  val tlqProduce = TlqHolder.getProducer;
  val tlqCustomer = TlqHolder.getCustomer

  override def handle(request: RequestChannel.Request, requestLocal: RequestLocal): Unit = {
    trace(s"Handling request:${request.requestDesc(true)} from connection ${request.context.connectionId};" +
      s"securityProtocol:${request.context.securityProtocol},principal:${request.context.principal}")

    try {
      if (!apiVersionManager.isApiEnabled(request.header.apiKey)) {
        // The socket server will reject APIs which are not exposed in this scope and close the connection
        // before handing them to the request handler, so this path should not be exercised in practice
        throw new IllegalStateException(s"API ${request.header.apiKey} is not enabled")
      }
      request.header.apiKey match {
        case ApiKeys.API_VERSIONS => handleApiVersionsRequest(request)
        case ApiKeys.METADATA => handleMetadataRequest(request)
        case ApiKeys.PRODUCE => handleProduceRequest(request, requestLocal)
        case ApiKeys.INIT_PRODUCER_ID => handleInitProducerIdRequest(request, requestLocal)
        case ApiKeys.FIND_COORDINATOR => handleFindCoordinatorRequest(request)
        case ApiKeys.JOIN_GROUP => handleJoinGroupRequest(request: RequestChannel.Request, requestLocal: RequestLocal)
        case ApiKeys.SYNC_GROUP => handleSyncGroupRequest(request, requestLocal)
        case ApiKeys.OFFSET_FETCH => handleOffsetFetchRequest(request: RequestChannel.Request)
        case ApiKeys.LIST_OFFSETS => handleListOffsetRequest(request: RequestChannel.Request)
        case ApiKeys.HEARTBEAT => handleHeartbeatRequest(request)
        case ApiKeys.FETCH => handleFetchRequest(request)
        case ApiKeys.OFFSET_COMMIT => handleOffsetCommitRequest(request, requestLocal)
        case _ => trace(s"Some API key not be handler apiKey: ${request.header.apiKey}")
      }
    } catch {
      case e: FatalExitError => throw e
      case e: Throwable =>
        error(s"Unexpected error handling request ${request.requestDesc(true)} " +
          s"with context ${request.context}", e)
        sendErrorOrCloseConnection(request, e, 0)
    }
  }

  def sendErrorOrCloseConnection(
                                  request: RequestChannel.Request,
                                  error: Throwable,
                                  throttleMs: Int
                                ): Unit = {
    val requestBody = request.body[AbstractRequest]
    val response = requestBody.getErrorResponse(throttleMs, error)
    if (response == null)
      requestChannel.closeConnection(request, requestBody.errorCounts(error))
    else
      requestChannel.sendResponse(request, response, None)
  }

  def handleProduceRequest(request: RequestChannel.Request, requestLocal: RequestLocal): Unit = {
    val produceRequest = request.body[ProduceRequest]
    val requestSize = request.sizeInBytes
    val unauthorizedTopicResponses = mutable.Map[TopicPartition, PartitionResponse]()
    val nonExistingTopicResponses = mutable.Map[TopicPartition, PartitionResponse]()
    val invalidRequestResponses = mutable.Map[TopicPartition, PartitionResponse]()
    val authorizedRequestInfo = mutable.Map[TopicPartition, MemoryRecords]()
    produceRequest.data().topicData().forEach(topic => topic.partitionData().forEach((pt: PartitionProduceData) => {
      val topicPartition = new TopicPartition(topic.name, pt.index)
      if (!TopicMap.initialize(topic.name())) {
        nonExistingTopicResponses += topicPartition -> new PartitionResponse(Errors.UNKNOWN_TOPIC_OR_PARTITION)
      }
      val memoryRecords = pt.records().asInstanceOf[MemoryRecords]
      try {
        ProduceRequest.validateRecords(request.header.apiVersion, memoryRecords)
        authorizedRequestInfo += (topicPartition -> memoryRecords)
      } catch {
        case e: ApiException =>
          invalidRequestResponses += topicPartition -> new PartitionResponse(Errors.forException(e))
      }
    }))

    def sendResponseCallback(responseStatus: Map[TopicPartition, PartitionResponse]): Unit = {
      val mergedResponseStatus = responseStatus ++ unauthorizedTopicResponses ++ nonExistingTopicResponses ++ invalidRequestResponses
      var errorInResponse = false

      mergedResponseStatus.forKeyValue { (topicPartition, status) =>
        if (status.error != Errors.NONE) {
          errorInResponse = true
          debug("Produce request with correlation id %d from client %s on partition %s failed due to %s".format(
            request.header.correlationId,
            request.header.clientId,
            topicPartition,
            status.error.exceptionName))
        }
      }

      // Record both bandwidth and request quota-specific values and throttle by muting the channel if any of the quotas
      // have been violated. If both quotas have been violated, use the max throttle time between the two quotas. Note
      // that the request quota is not enforced if acks == 0.
      val timeMs = time.milliseconds()


      // Send the response immediately. In case of throttling, the channel has already been muted.
      if (produceRequest.acks == 0) {
        // no operation needed if producer request.required.acks = 0; however, if there is any error in handling
        // the request, since no response is expected by the producer, the server will close socket server so that
        // the producer client will know that some error has happened and will refresh its metadata
        if (errorInResponse) {
          val exceptionsSummary = mergedResponseStatus.map { case (topicPartition, status) =>
            topicPartition -> status.error.exceptionName
          }.mkString(", ")
          info(
            s"Closing connection due to error during produce request with correlation id ${request.header.correlationId} " +
              s"from client id ${request.header.clientId} with ack=0\n" +
              s"Topic and partition to exceptions: $exceptionsSummary"
          )
          requestChannel.closeConnection(request, new ProduceResponse(mergedResponseStatus.asJava).errorCounts)
        } else {
          requestChannel.sendNoOpResponse(request)
        }
      } else {
        requestChannel.sendResponse(request, new ProduceResponse(mergedResponseStatus.asJava, 0), None)
      }
    }

    if (authorizedRequestInfo.isEmpty)
      sendResponseCallback(Map.empty)
    else {
      // request
      val responseStatus = mutable.Map[TopicPartition, PartitionResponse]()
      val partitionSize = authorizedRequestInfo.keySet.size
      val countDownLatch = new CountDownLatch(partitionSize)
      authorizedRequestInfo.foreach {
        //memeryRecords 包含 List<BatchRecord>
        case (topicPartition, records: MemoryRecords) => {
          val batchesIterator: util.Iterator[MutableRecordBatch] = records.batches().iterator()
          while (batchesIterator.hasNext) {
            val batch: MutableRecordBatch = batchesIterator.next();

            var count = batch.countOrNull()
            if (count == 0) {
              count = (batch.lastOffset() - batch.baseOffset() + 1).toInt
            }
            val endCount = new AtomicInteger(0)
            val baseOffset = TopicMap.getOffset(topicPartition)
            TopicMap.addOffset(topicPartition, Some(count))
            trace(s"baseOffset ${batch.baseOffset()},lastOffset ${batch.lastOffset()} count ${batch.countOrNull()}")
            //，仅保留最后一个批次的最后一条消息
            val recordIterator = batch.iterator()
            var offset_index = 0;
            while (recordIterator.hasNext) {
              val record: Record = recordIterator.next()
              val message = new TlqMessage()
              message.setTopicOrQueue(TlqHolder.topic)
              var dataLength = 1 //magic 1byte first
              dataLength += ByteUtils.sizeOfVarint(record.keySize())
              dataLength += ByteUtils.sizeOfVarint(record.valueSize())
              dataLength += record.keySize() + record.valueSize()
              dataLength += ByteUtils.sizeOfVarint(record.headers().length)
              record.headers().foreach(h => {
                val header = h.asInstanceOf[RecordHeader]
                dataLength += ByteUtils.sizeOfVarint(header.getKeyBuffer.remaining())
                dataLength += ByteUtils.sizeOfVarint(Option(header.getValueBuffer).map(r => r.remaining()).getOrElse(0))
                dataLength += header.getKeyBuffer.remaining() + Option(header.getValueBuffer).map(r => r.remaining()).getOrElse(0)
              })
              dataLength += ByteUtils.sizeOfVarint(offset_index)
              dataLength += ByteUtils.sizeOfVarlong(record.timestamp())
              if (batch.magic > RecordBatch.MAGIC_VALUE_V1) {
                val batch1 = batch.asInstanceOf[DefaultRecordBatch]
                dataLength += ByteUtils.sizeOfVarlong(batch1.baseTimestamp())
              }

              //magic
              //key_length ->
              //key
              //value_length ->
              //value
              //header count ->
              //header key size
              //header key
              //header value size
              //header value
              //offset_index
              //timestamp
              //base_timestamp


              val bytes = new Array[Byte](dataLength)
              val buffer = ByteBuffer.wrap(bytes)
              buffer.put(batch.magic())
              ByteUtils.writeVarint(record.keySize(), buffer)
              buffer.put(record.key())
              ByteUtils.writeVarint(record.valueSize(), buffer)
              buffer.put(record.value())
              ByteUtils.writeVarint(record.headers().length, buffer)
              record.headers().foreach(h => {
                val header = h.asInstanceOf[RecordHeader]
                ByteUtils.writeVarint(header.getKeyBuffer.remaining(), buffer)
                buffer.put(header.getKeyBuffer.slice())
                val headerValue = header.getValueBuffer
                if (headerValue != null) {
                  ByteUtils.writeVarint(headerValue.remaining(), buffer)
                  buffer.put(header.getValueBuffer.slice())
                } else {
                  ByteUtils.writeVarint(0, buffer)
                }
              })
              ByteUtils.writeVarint(offset_index, buffer)
              ByteUtils.writeVarlong(record.timestamp(), buffer)
              if (batch.magic() > RecordBatch.MAGIC_VALUE_V1) {
                val batch1 = batch.asInstanceOf[DefaultRecordBatch]
                ByteUtils.writeVarlong(batch1.baseTimestamp(), buffer)
              }
              if (buffer.remaining() != 0) {
                throw new RuntimeException("some error ")
              }
              message.setBody(bytes)
              offset_index += 1
              tlqProduce.send(message, new SendCallback {
                override def onSuccess(sendResult: SendResult): Unit = {
                  val i = endCount.incrementAndGet()
                  if (i == count)
                    countDownLatch.countDown();
                }

                override def onException(throwable: Throwable): Unit = {
                  val i = endCount.getAndIncrement()
                  if (i == count)
                    countDownLatch.countDown();
                }
              })
            }
            responseStatus += (topicPartition ->
              new PartitionResponse(
                Errors.NONE,
                baseOffset,
                time.milliseconds(),
                baseOffset,
              )
              )
            //          if (magic > RecordBatch.MAGIC_VALUE_V1)
            //            return new DefaultRecordBatch(batchSlice);
            //          else
            //            return new AbstractLegacyRecordBatch.ByteBufferLegacyRecordBatch(batchSlice);

            //构建PartitionResponse
            //      error_code => INT16
            //      base_offset => INT64
            //      log_append_time_ms => INT64
            //      log_start_offset => INT64
            // new PartitionResponse(
            //            result.error,
            //            result.info.firstOffset.map(_.messageOffset).getOrElse(-1),
            //            result.info.logAppendTime,
            //            result.info.logStartOffset,
            //            result.info.recordErrors.asJava,
            //            result.info.errorMessage
            //          )
            responseStatus += (topicPartition ->
              new PartitionResponse(
                Errors.NONE,
                baseOffset.toLong,
                time.milliseconds(),
                baseOffset.toLong,
              )
              )
          }
        }
      }
      if (produceRequest.acks() != 0) {
        countDownLatch.await();
      }
      sendResponseCallback(responseStatus.toMap)
    }

  }


  def handleFindCoordinatorRequest(request: RequestChannel.Request): Unit = {
    val version = request.header.apiVersion
    val findCoordinatorRequest = request.body[FindCoordinatorRequest]
    if (version < FindCoordinatorRequest.MIN_BATCHED_VERSION) {
      val keyType = CoordinatorType.forId(findCoordinatorRequest.data().keyType())
      val key = findCoordinatorRequest.data().key()
      val responseBody = new FindCoordinatorResponse(
        new FindCoordinatorResponseData()
          .setErrorCode(Errors.NONE.code())
          .setErrorMessage(Errors.NONE.message())
          .setNodeId(TopicMap.node.id())
          .setHost(TopicMap.node.host())
          .setPort(TopicMap.node.port)
          .setThrottleTimeMs(0))
      trace("Sending FindCoordinator response %s for correlation id %d to client %s."
        .format(responseBody, request.header.correlationId, request.header.clientId))
      requestChannel.sendResponse(request, responseBody, None)
      return
    }
    //v4及之上支持此查找多个协调者
    val coordinators = findCoordinatorRequest.data.coordinatorKeys.asScala.map { key =>
      val keyType = CoordinatorType.forId(findCoordinatorRequest.data().keyType())
      val key = findCoordinatorRequest.data().key()
      new FindCoordinatorResponseData.Coordinator()
        .setKey(key)
        .setErrorCode(Errors.NONE.code())
        .setErrorMessage(Errors.NONE.message())
        .setNodeId(TopicMap.node.id())
        .setHost(TopicMap.node.host())
        .setPort(TopicMap.node.port)
    }
    val response = new FindCoordinatorResponse(
      new FindCoordinatorResponseData()
        .setCoordinators(coordinators.asJava)
        .setThrottleTimeMs(0))
    trace("Sending FindCoordinator response %s for correlation id %d to client %s."
      .format(response, request.header.correlationId, request.header.clientId))
    requestChannel.sendResponse(request, response, None)

  }


  def handleApiVersionsRequest(request: RequestChannel.Request): Unit = {
    // Note that broker returns its full list of supported ApiKeys and versions regardless of current
    // authentication state (e.g., before SASL authentication on an SASL listener, do note that no
    // Kafka protocol requests may take place on an SSL listener before the SSL handshake is finished).
    // If this is considered to leak information about the broker version a workaround is to use SSL
    // with client authentication which is performed at an earlier stage of the connection where the
    // ApiVersionRequest is not available.
    def createResponseCallback(requestThrottleMs: Int): ApiVersionsResponse = {
      val apiVersionRequest = request.body[ApiVersionsRequest]
      if (apiVersionRequest.hasUnsupportedRequestVersion) {
        apiVersionRequest.getErrorResponse(requestThrottleMs, Errors.UNSUPPORTED_VERSION.exception)
      } else if (!apiVersionRequest.isValid) {
        apiVersionRequest.getErrorResponse(requestThrottleMs, Errors.INVALID_REQUEST.exception)
      } else {
        apiVersionManager.apiVersionResponse(requestThrottleMs)
      }
    }

    requestChannel.sendResponse(request, createResponseCallback(0), None)

  }

  def handleMetadataRequest(request: RequestChannel.Request): Unit = {
    //需要返回topic 对应的 partition info 包括leader 和副本信息,broker信息

    val metadataRequest = request.body[MetadataRequest]
    val requestVersion = request.header.apiVersion
    val isAllTopic = metadataRequest.isAllTopics
    // Topic IDs are not supported for versions 10 and 11. Topic names can not be null in these versions.
    if (!metadataRequest.isAllTopics) {
      metadataRequest.data.topics.forEach { topic =>
        if (topic.name == null && metadataRequest.version < 12) {
          throw new InvalidRequestException(s"Topic name can not be null for version ${metadataRequest.version}")
        } else if (topic.topicId != Uuid.ZERO_UUID && metadataRequest.version < 12) {
          throw new InvalidRequestException(s"Topic IDs are not supported in requests for version ${metadataRequest.version}")
        }
      }
    }
    var topics = metadataRequest.topics()
    if (isAllTopic) {
      //mock some topic
      topics = List("TEST_TOPIC", "DEFAULT_TOPIC").asJava
    }
    val brokers = List(TopicMap.node)
    //COMPACT_NULLABLE_STRING
    val clusterId = "0"
    //INT32
    val controllerId = 0
    val topicsMetaData: List[MetadataResponseTopic] = topics.stream().map(t =>
      new MetadataResponseTopic()
        .setErrorCode(Errors.NONE.code())
        .setName(t)
        //UUID
        .setTopicId(TopicMap.getOrCreateTopic(t))
        .setIsInternal(false)
        .setPartitions(List[MetadataResponsePartition](
          new MetadataResponsePartition()
            //      error_code => INT16
            //      partition_index => INT32
            //      leader_id => INT32
            //      leader_epoch => INT32
            //      replica_nodes => INT32
            //      isr_nodes => INT32
            //      offline_replicas => INT32
            .setErrorCode(Errors.NONE.code())
            .setPartitionIndex(0)
            .setLeaderId(TopicMap.node.id())
            .setLeaderEpoch(0)
            .setReplicaNodes(List(TopicMap.node.id()).map(r => r: Integer).asJava)
            .setIsrNodes(List(TopicMap.node.id()).map(r => r: Integer).asJava)
            .setOfflineReplicas(List[Integer]().asJava),
          new MetadataResponsePartition()
            //      error_code => INT16
            //      partition_index => INT32
            //      leader_id => INT32
            //      leader_epoch => INT32
            //      replica_nodes => INT32
            //      isr_nodes => INT32
            //      offline_replicas => INT32
            .setErrorCode(Errors.NONE.code())
            .setPartitionIndex(1)
            .setLeaderId(TopicMap.node.id())
            .setLeaderEpoch(0)
            .setReplicaNodes(List(TopicMap.node.id()).map(r => r: Integer).asJava)
            .setIsrNodes(List(TopicMap.node.id()).map(r => r: Integer).asJava)
            .setOfflineReplicas(List[Integer]().asJava)
        ).asJava)
    ).toScala(List)

    val clusterAuthorizedOperations = Int.MinValue // Default value in the schema
    val response = MetadataResponse.prepareResponse(
      requestVersion,
      0,
      brokers.asJava,
      clusterId,
      controllerId,
      topicsMetaData.asJava,
      clusterAuthorizedOperations
    )
    requestChannel.sendResponse(request, response, None)
  }

  def handleInitProducerIdRequest(request: RequestChannel.Request, requestLocal: RequestLocal): Unit = {
    //    request.session.clientAddress
    val initProducerIdRequest = request.body[InitProducerIdRequest]
    val transactionalId = initProducerIdRequest.data.transactionalId

    val responseData = new InitProducerIdResponseData()
      .setProducerId(TopicMap.getProductId())
      .setProducerEpoch(0)
      .setThrottleTimeMs(0)
      .setErrorCode(Errors.NONE.code())
    val responseBody = new InitProducerIdResponse(responseData)
    requestChannel.sendResponse(request, responseBody, None)
  }

  //加入组，后续会向该节点执行SyncGroup,
  // 应该等到join_group超时时间后再返回所有的成员，供leader进行分配
  def handleJoinGroupRequest(request: RequestChannel.Request, requestLocal: RequestLocal): Unit = {
    val joinGroupRequest = request.body[JoinGroupRequest]
    val groupInstanceId = Option(joinGroupRequest.data.groupInstanceId)
    //   Only return MEMBER_ID_REQUIRED error if joinGroupRequest version is >= 4
    // and groupInstanceId is configured to unknown.
    val requireKnownMemberId = joinGroupRequest.version >= 4 && groupInstanceId.isEmpty

    def getMemberId(): String = {
      s"${request.header.clientId()}-${Uuid.randomUuid().toString}"
    }

    val emptyProtocolName = if (request.context.apiVersion() >= 7)
      null
    else
      ""

    def getJoinGroupResponseByError(errors: Errors) = {

      new JoinGroupResponse(
        new JoinGroupResponseData()
          .setThrottleTimeMs(0)
          .setErrorCode(errors.code())
          //客户端后续的offset，SYNC_GROUP等会携带该字段
          .setGenerationId(-1)
          .setProtocolType(None.orNull)
          .setProtocolName(emptyProtocolName)
          .setLeader("")
          //如果为true 可以使客户端跳过分配，leader返回的分配结果将空结果
          .setSkipAssignment(false)
          .setMemberId(s"${request.header.clientId()}-${Uuid.randomUuid().toString}")
          .setMembers(List.empty.asJava)
      )
    }

    val requestMemberId = Option(joinGroupRequest.data().memberId())
    val isMemberIdNull = requestMemberId.getOrElse("").equals("")
    val responseBody: JoinGroupResponse = if (requireKnownMemberId && isMemberIdNull) {
      getJoinGroupResponseByError(Errors.MEMBER_ID_REQUIRED)
    } else {
      val memberId =
        if (isMemberIdNull) requestMemberId.getOrElse(getMemberId())
        else
          getMemberId()
      val protocols: util.List[JoinGroupRequestData.JoinGroupRequestProtocol] = joinGroupRequest.data().protocols().valuesList()
      val pickedProtocol = Option(protocols.get(0))
      new JoinGroupResponse(
        new JoinGroupResponseData()
          .setThrottleTimeMs(0)
          .setErrorCode(Errors.NONE.code())
          //客户端后续的offset，SYNC_GROUP等会携带该字段
          .setGenerationId(0)
          .setProtocolType(joinGroupRequest.data().protocolType())
          .setProtocolName(pickedProtocol.map(_.name()).getOrElse(emptyProtocolName))
          .setLeader(memberId)
          //如果为true 可以使客户端跳过分配，leader返回的分配结果将空结果
          .setSkipAssignment(false)
          .setMemberId(memberId)
          .setMembers(List[JoinGroupResponseMember](new JoinGroupResponseMember()
            .setGroupInstanceId("0")
            .setMemberId(memberId)
            .setMetadata(pickedProtocol.map(_.metadata()).getOrElse(new Array[Byte](0)))
          ).asJava))
    }
    requestChannel.sendResponse(request, responseBody, None)

  }

  //返回分配结果，如果是leader,且上一步Sync_group允许leader分配，会携带分配结果
  def handleSyncGroupRequest(request: RequestChannel.Request, requestLocal: RequestLocal): Unit = {
    val syncGroupRequest = request.body[SyncGroupRequest]

    val assignmentMap = immutable.Map.newBuilder[String, Array[Byte]]
    syncGroupRequest.data.assignments.forEach { assignment =>
      assignmentMap += (assignment.memberId -> assignment.assignment)
    }

    def getResponseByError(error: Errors) = {
      new SyncGroupResponse(
        new SyncGroupResponseData()
          .setErrorCode(error.code)
          .setProtocolType(None.orNull)
          .setProtocolName(None.orNull)
          .setAssignment(Array.empty)
          .setThrottleTimeMs(0)
      )
    }

    val memberId = Option(syncGroupRequest.data().memberId())
    if (memberId.getOrElse("").isEmpty || !assignmentMap.result().contains(syncGroupRequest.data().memberId())
    ) {
      requestChannel.sendResponse(request, getResponseByError(Errors.UNKNOWN_MEMBER_ID), None)
    }
    val syncGroupResponse = new SyncGroupResponse(
      new SyncGroupResponseData()
        .setErrorCode(Errors.NONE.code)
        .setProtocolType(Option(syncGroupRequest.data().protocolType()).orNull)
        .setProtocolName(Option(syncGroupRequest.data().protocolName()).orNull)
        .setAssignment(memberId.map(m => assignmentMap.result().getOrElse(m, Array.empty)).getOrElse(Array.empty))
        .setThrottleTimeMs(0)
    )
    requestChannel.sendResponse(request, syncGroupResponse, None)
  }

  def handleOffsetFetchRequest(request: RequestChannel.Request): Unit = {
    val header = request.header
    val offsetFetchRequest = request.body[OffsetFetchRequest]
    val groupIds = offsetFetchRequest.groupIds().asScala
    val groupToTopicPartitions: mutable.Map[String, util.List[TopicPartition]] = offsetFetchRequest.groupIdsToPartitions().asScala
    val groupToErrorMap = mutable.Map.empty[String, Errors]
    val groupToPartitionData = mutable.Map.empty[String, util.Map[TopicPartition, OffsetFetchResponse.PartitionData]]


    groupIds.foreach(groupId => {
      val groupMap = mutable.Map.empty[TopicPartition, OffsetFetchResponse.PartitionData]

      groupToTopicPartitions.get(groupId).foreach(t => t.forEach((partition: TopicPartition) => {
        val partitionData = new OffsetFetchResponse.PartitionData(-1, Optional.ofNullable(-1), "", Errors.NONE)
        groupMap += (partition -> partitionData)
      }))
      groupToErrorMap += (groupId -> Errors.NONE)
      groupToPartitionData += (groupId -> groupMap.asJava)
    })

    val offsetFetchResponse = new OffsetFetchResponse(0,
      groupToErrorMap.asJava, groupToPartitionData.asJava)
    requestChannel.sendResponse(request, offsetFetchResponse, None)
  }

  def handleHeartbeatRequest(request: RequestChannel.Request): Unit = {
    val heartbeatRequest = request.body[HeartbeatRequest]

    def sendResponseCallback(error: Errors): Unit = {
      def createResponse(requestThrottleMs: Int): AbstractResponse = {
        val response = new HeartbeatResponse(
          new HeartbeatResponseData()
            .setThrottleTimeMs(requestThrottleMs)
            .setErrorCode(error.code))
        trace("Sending heartbeat response %s for correlation id %d to client %s."
          .format(response, request.header.correlationId, request.header.clientId))
        response
      }

      requestChannel.sendResponse(request, createResponse(0), None)
    }
    //Errors.REBALANCE_IN_PROGRESS 来通知客户端发起reBalance
    sendResponseCallback(Errors.NONE)
  }


  def handleListOffsetRequest(request: RequestChannel.Request): Unit = {
    val version = request.header.apiVersion
    val clientId = request.header.clientId
    val offsetRequest = request.body[ListOffsetsRequest]
    val useOldStyleOffset = version == 0

    def getOffset(topicPartition: TopicPartition, timestamp: Int): Int = {
      if (timestamp == ListOffsetsRequest.EARLIEST_TIMESTAMP) {
        return 0
      }
      TopicMap.getOffset(topicPartition)
    }

    def getErrorListOffsetsPartitionResponse(errors: Errors, partitionIndex: Int) = {
      new ListOffsetsResponseData.ListOffsetsPartitionResponse()
        .setErrorCode(errors.code())
        .setPartitionIndex(partitionIndex)
    }

    val topics = offsetRequest.data().topics().asScala.map((offsetsTopic) => {
      val partitionResponse = offsetsTopic.partitions().asScala.map(partition => {
        val tp = new TopicPartition(offsetsTopic.name(), partition.partitionIndex())
        val response = new ListOffsetsResponseData.ListOffsetsPartitionResponse()
          .setErrorCode(Errors.NONE.code())
          .setPartitionIndex(partition.partitionIndex())
          .setLeaderEpoch(0)
        if (useOldStyleOffset) {
          val maxNumOffsets = partition.maxNumOffsets()
          //v0老版本会返回之前的分段offset,根据Segments分段
          //todo 如何兼容待处理
          response.setOldStyleOffsets(List[lang.Long](getOffset(tp, partition.timestamp().toInt)).asJava)
        } else {
          response.setOffset(if (partition.timestamp() == ListOffsetsRequest.EARLIEST_TIMESTAMP) {
            0
          } else {
            getOffset(tp, partition.partitionIndex())
          })
        }
        response

      })
      new ListOffsetsTopicResponse().setName(offsetsTopic.name())
        .setPartitions(partitionResponse.asJava)
    })

    val response = new ListOffsetsResponse(new ListOffsetsResponseData()
      .setThrottleTimeMs(0)
      .setTopics(topics.asJava))
    requestChannel.sendResponse(request, response, None)
  }


  def handleFetchRequest(request: RequestChannel.Request): Unit = {
    val versionId = request.header.apiVersion
    val clientId = request.header.clientId
    val fetchRequest = request.body[FetchRequest]
    val metadata = fetchRequest.metadata()
    val topicNames =
      if (fetchRequest.version() >= 13) {
        //version 13 之后会分配id给topic
        TopicMap.getTopicMapView().asJava
      } else {
        Collections.emptyMap[Uuid, String]()
      }

    val fetchData = fetchRequest.fetchData(topicNames)
    val forgottenTopics = fetchRequest.forgottenTopics(topicNames)


    val fetchContext = fetchManager.newContext(
      fetchRequest.version,
      fetchRequest.metadata,
      fetchRequest.isFromFollower,
      fetchData,
      forgottenTopics,
      topicNames)

    val erroneous = mutable.ArrayBuffer[(TopicIdPartition, FetchResponseData.PartitionData)]()
    val interesting = mutable.ArrayBuffer[(TopicIdPartition, FetchRequest.PartitionData)]()

    fetchContext.foreachPartition { (topicIdPartition, partitionData) =>
      if (topicIdPartition.topic == null) {
        //todo 其他初步验证错误
        erroneous += topicIdPartition -> FetchResponse.partitionResponse(topicIdPartition, Errors.UNKNOWN_TOPIC_ID)
      } else
        interesting += topicIdPartition -> partitionData
    }


    def maybeDownConvertStorageError(error: Errors): Errors = {
      // If consumer sends FetchRequest V5 or earlier, the client library is not guaranteed to recognize the error code
      // for KafkaStorageException. In this case the client library will translate KafkaStorageException to
      // UnknownServerException which is not retriable. We can ensure that consumer will update metadata and retry
      // by converting the KafkaStorageException to NotLeaderOrFollowerException in the response if FetchRequest version <= 5
      if (error == Errors.KAFKA_STORAGE_ERROR && versionId <= 5) {
        Errors.NOT_LEADER_OR_FOLLOWER
      } else {
        error
      }
    }

    //数据兼容行处理
    def maybeConvertFetchedData(tp: TopicIdPartition,
                                partitionData: FetchResponseData.PartitionData): FetchResponseData.PartitionData = {
      // We will never return a logConfig when the topic is unresolved and the name is null. This is ok since we won't have any records to convert.
      //对应kafka 配置compression.type，默认值producer
      if ( /*logConfig.exists(_.compressionType == ZStdCompressionCodec.name)*/ false && versionId < 10) {
        trace(s"Fetching messages is disabled for ZStandard compressed partition $tp. Sending unsupported version response to $clientId.")
        FetchResponse.partitionResponse(tp, Errors.UNSUPPORTED_COMPRESSION_TYPE)
      } else {
        // Down-conversion of fetched records is needed when the on-disk magic value is greater than what is
        // supported by the fetch request version.
        // If the inter-broker protocol version is `3.0` or higher, the log config message format version is
        // always `3.0` (i.e. magic value is `v2`). As a result, we always go through the down-conversion
        // path if the fetch version is 3 or lower (in rare cases the down-conversion may not be needed, but
        // it's not worth optimizing for them).
        // If the inter-broker protocol version is lower than `3.0`, we rely on the log config message format
        // version as a proxy for the on-disk magic value to maintain the long-standing behavior originally
        // introduced in Kafka 0.10.0. An important implication is that it's unsafe to downgrade the message
        // format version after a single message has been produced (the broker would return the message(s)
        // without down-conversion irrespective of the fetch version).
        val unconvertedRecords = FetchResponse.recordsOrFail(partitionData)
        val version = messageFormatVersion.highestSupportedRecordVersion
        val magic = version.value
        val downConvertMagic =
          if (magic > RecordBatch.MAGIC_VALUE_V0 && versionId <= 1)
            Some(RecordBatch.MAGIC_VALUE_V0)
          else if (magic > RecordBatch.MAGIC_VALUE_V1 && versionId <= 3)
            Some(RecordBatch.MAGIC_VALUE_V1)
          else
            None


        downConvertMagic match {
          case Some(magic) =>
            //对应配置message.downconversion.enable 默认为true
            // For fetch requests from clients, check if down-conversion is disabled for the particular partition
            if (!fetchRequest.isFromFollower && /* !logConfig.forall(_.messageDownConversionEnable)*/ false) {
              trace(s"Conversion to message format ${downConvertMagic.get} is disabled for partition $tp. Sending unsupported version response to $clientId.")
              FetchResponse.partitionResponse(tp, Errors.UNSUPPORTED_VERSION)
            } else {
              try {
                trace(s"Down converting records from partition $tp to message format version $magic for fetch request from $clientId")
                // Because down-conversion is extremely memory intensive, we want to try and delay the down-conversion as much
                // as possible. With KIP-283, we have the ability to lazily down-convert in a chunked manner. The lazy, chunked
                // down-conversion always guarantees that at least one batch of messages is down-converted and sent out to the
                // client.
                new FetchResponseData.PartitionData()
                  .setPartitionIndex(tp.partition)
                  .setErrorCode(maybeDownConvertStorageError(Errors.forCode(partitionData.errorCode)).code)
                  .setHighWatermark(partitionData.highWatermark)
                  .setLastStableOffset(partitionData.lastStableOffset)
                  .setLogStartOffset(partitionData.logStartOffset)
                  .setAbortedTransactions(partitionData.abortedTransactions)
                  .setRecords(new LazyDownConversionRecords(tp.topicPartition, unconvertedRecords, magic, fetchContext.getFetchOffset(tp).get, time))
                  .setPreferredReadReplica(partitionData.preferredReadReplica())
              } catch {
                case e: UnsupportedCompressionTypeException =>
                  trace("Received unsupported compression type error during down-conversion", e)
                  FetchResponse.partitionResponse(tp, Errors.UNSUPPORTED_COMPRESSION_TYPE)
              }
            }
          case None =>
            new FetchResponseData.PartitionData()
              .setPartitionIndex(tp.partition)
              .setErrorCode(maybeDownConvertStorageError(Errors.forCode(partitionData.errorCode)).code)
              .setHighWatermark(partitionData.highWatermark)
              .setLastStableOffset(partitionData.lastStableOffset)
              .setLogStartOffset(partitionData.logStartOffset)
              .setAbortedTransactions(partitionData.abortedTransactions)
              .setRecords(unconvertedRecords)
              .setPreferredReadReplica(partitionData.preferredReadReplica)
              .setDivergingEpoch(partitionData.divergingEpoch)
        }
      }
    }


    // the callback for process a fetch response, invoked before throttling
    def processResponseCallback(responsePartitionData: Seq[(TopicIdPartition, FetchPartitionData)]): Unit = {
      val partitions = new util.LinkedHashMap[TopicIdPartition, FetchResponseData.PartitionData]
      val reassigningPartitions = mutable.Set[TopicIdPartition]()
      responsePartitionData.foreach { case (tp, data) =>
        val abortedTransactions = data.abortedTransactions.map(_.asJava).orNull
        val lastStableOffset = data.lastStableOffset.getOrElse(FetchResponse.INVALID_LAST_STABLE_OFFSET)
        if (data.isReassignmentFetch) reassigningPartitions.add(tp)
        val partitionData = new FetchResponseData.PartitionData()
          .setPartitionIndex(tp.partition)
          .setErrorCode(maybeDownConvertStorageError(data.error).code)
          .setHighWatermark(data.highWatermark)
          .setLastStableOffset(lastStableOffset)
          .setLogStartOffset(data.logStartOffset)
          .setAbortedTransactions(abortedTransactions)
          .setRecords(data.records)
          .setPreferredReadReplica(data.preferredReadReplica.getOrElse(FetchResponse.INVALID_PREFERRED_REPLICA_ID))
        data.divergingEpoch.foreach(partitionData.setDivergingEpoch)
        partitions.put(tp, partitionData)
      }
      erroneous.foreach { case (tp, data) => partitions.put(tp, data) }

      var unconvertedFetchResponse: FetchResponse = null

      def createResponse(throttleTimeMs: Int): FetchResponse = {
        // Down-convert messages for each partition if required
        val convertedData = new util.LinkedHashMap[TopicIdPartition, FetchResponseData.PartitionData]
        unconvertedFetchResponse.data().responses().forEach { topicResponse =>
          topicResponse.partitions().forEach { unconvertedPartitionData =>
            val tp = new TopicIdPartition(topicResponse.topicId, new TopicPartition(topicResponse.topic, unconvertedPartitionData.partitionIndex()))
            val error = Errors.forCode(unconvertedPartitionData.errorCode)
            if (error != Errors.NONE)
              debug(s"Fetch request with correlation id ${request.header.correlationId} from client $clientId " +
                s"on partition $tp failed due to ${error.exceptionName}")
            convertedData.put(tp, maybeConvertFetchedData(tp, unconvertedPartitionData))
          }
        }

        // Prepare fetch response from converted data
        val response =
          FetchResponse.of(unconvertedFetchResponse.error, throttleTimeMs, unconvertedFetchResponse.sessionId, convertedData)

        response
      }

      // Get the actual response. This will update the fetch context.
      unconvertedFetchResponse = fetchContext.updateAndGenerateResponseData(partitions)
      val responsePartitionsSize = unconvertedFetchResponse.data().responses().stream().mapToInt(_.partitions().size()).sum()
      trace(s"Sending Fetch response with partitions.size=$responsePartitionsSize, " +
        s"metadata=${unconvertedFetchResponse.sessionId} ${}")


      // Send the response immediately.
      requestChannel.sendResponse(request, createResponse(5), None)
    }


    if (interesting.isEmpty) {
      processResponseCallback(Seq.empty)
    } else {

      val defaultFetchMaxBytes = 55 * 1024 * 1024
      val fetchMaxBytes = Math.min(fetchRequest.maxBytes, defaultFetchMaxBytes)
      val fetchMinBytes = Math.min(fetchRequest.minBytes, fetchMaxBytes)

      val clientMetadata: Option[ClientMetadata] = if (versionId >= 11) {
        // Fetch API version 11 added preferred replica logic
        Some(new DefaultClientMetadata(
          fetchRequest.rackId,
          clientId,
          request.context.clientAddress,
          request.context.principal,
          request.context.listenerName.value))
      } else {
        None
      }
      val params = FetchParams(
        requestVersion = versionId,
        replicaId = fetchRequest.replicaId,
        maxWaitMs = fetchRequest.maxWait,
        minBytes = fetchMinBytes,
        maxBytes = fetchMaxBytes,
        isolation = FetchIsolation(fetchRequest),
        clientMetadata = clientMetadata
      )

      def buildRecordsByTlqMessage(message: mutable.Buffer[MessageExt], baseOffset: Long): MemoryRecords = {
        def readVarintVaule(byteBuffer: ByteBuffer) = {
          val length = ByteUtils.readVarint(byteBuffer)
          val value = byteBuffer.slice()
          value.limit(length)
          byteBuffer.position(byteBuffer.position() + length)
          (length, value)
        }

        val records: mutable.Buffer[SimpleRecord] = message.map((message) => {
          val body = message.getBody
          val buffer = ByteBuffer.wrap(body)
          val magic = buffer.get()
          val (keySize, key) = readVarintVaule(buffer)
          val (valueSize, value) = readVarintVaule(buffer)
          val headerCount = ByteUtils.readVarint(buffer)
          val headers = new ArrayBuffer[RecordHeader]()
          for (i <- 0 until  headerCount) {
            val (_, hKey: ByteBuffer) = readVarintVaule(buffer)
            val (_, hValue: ByteBuffer) = readVarintVaule(buffer)
            headers += new RecordHeader(hKey, hValue)
          }
          val offsetIndex = ByteUtils.readVarint(buffer)
          val timestamp = ByteUtils.readVarlong(buffer)
          new SimpleRecord(timestamp, key, value, headers.toArray)
        })

        MemoryRecords.withRecords(RecordBatch.CURRENT_MAGIC_VALUE, baseOffset, CompressionType.NONE, records.toList: _*)
      }

      val countDownLatch = new CountDownLatch(interesting.size)
      val result = new mutable.ArrayBuffer[(TopicIdPartition, FetchPartitionData)]
      interesting.foreach { case (topicIdPartition, partitionData) =>
        tlqCustomer.pullMessage(PullType.PullContinue, -1, 4, new PullCallback {
          override def onSuccess(pullResult: PullResult): Unit = {
            if (pullResult.getPullStatus==PullStatus.FOUND){
              info("success pull from tlq")
            }
            val fetchPartitionData = FetchPartitionData(
              error = Errors.NONE,
              //最高以提交的记录
              highWatermark = 999999,
              logStartOffset = 0,
              records = if (pullResult.getMsgFoundList.isEmpty) {
                MemoryRecords.EMPTY
              } else {
                buildRecordsByTlqMessage(pullResult.getMsgFoundList.asScala, partitionData.fetchOffset)
              },
              divergingEpoch = Some(new FetchResponseData.EpochEndOffset().setEpoch(0).setEndOffset(99999)),
              lastStableOffset = None,
              abortedTransactions = None,
              //下一次拉取的目的地
              preferredReadReplica = Some(TopicMap.node.id()),
              //对客户端没有意义
              isReassignmentFetch = false
            )
            result += (topicIdPartition -> fetchPartitionData)
            countDownLatch.countDown()
          }

          override def onException(throwable: Throwable): Unit = {
            val fetchPartitionData = FetchPartitionData(
              error = Errors.NONE,
              //最高以提交的记录
              highWatermark = partitionData.fetchOffset,
              logStartOffset = 0,
              records = MemoryRecords.EMPTY,
              divergingEpoch = Some(new FetchResponseData.EpochEndOffset().setEpoch(0).setEndOffset(99999)),
              lastStableOffset = Option(partitionData.fetchOffset),
              abortedTransactions = None,
              //下一次拉取的目的地
              preferredReadReplica = Some(TopicMap.node.id()),
              //对客户端没有意义
              isReassignmentFetch = false
            )
            countDownLatch.countDown()
            result += (topicIdPartition -> fetchPartitionData)
          }
        })


      }
      countDownLatch.await()
      processResponseCallback(result.toList)

    }
  }

  def handleOffsetCommitRequest(request: RequestChannel.Request, requestLocal: RequestLocal): Unit = {
    val header = request.header
    val offsetCommitRequest = request.body[OffsetCommitRequest]
    val nonExistingTopicErrors = mutable.Map[TopicPartition, Errors]()

    def sendResponseCallback(commitStatus: Map[TopicPartition, Errors]): Unit = {
      val combinedCommitStatus = commitStatus ++ nonExistingTopicErrors
      if (isDebugEnabled)
        combinedCommitStatus.forKeyValue { (topicPartition, error) =>
          if (error != Errors.NONE) {
            debug(s"Offset commit request with correlation id ${header.correlationId} from client ${header.clientId} " +
              s"on partition $topicPartition failed due to ${error.exceptionName}")
          }
        }
      requestChannel.sendResponse(request,
        new OffsetCommitResponse(0, combinedCommitStatus.asJava), None)
    }

    val commitStatus = mutable.Map[TopicPartition, Errors]()
    for (topicRequest <- offsetCommitRequest.data().topics().asScala) {
      val topicName = topicRequest.name();
      for (partitionReq <- topicRequest.partitions().asScala) {
        val tp = new TopicPartition(topicName, partitionReq.partitionIndex())
        if (!TopicMap.isTopicExist(topicName)) {
          nonExistingTopicErrors += (tp -> Errors.UNKNOWN_TOPIC_OR_PARTITION)
        } else {
          commitStatus += (tp -> Errors.NONE)
        }

      }
    }
    sendResponseCallback(commitStatus.toMap)

  }

}

case class FetchPartitionData(error: Errors = Errors.NONE,
                              highWatermark: Long,
                              logStartOffset: Long,
                              records: Records,
                              divergingEpoch: Option[FetchResponseData.EpochEndOffset],
                              lastStableOffset: Option[Long],
                              abortedTransactions: Option[List[FetchResponseData.AbortedTransaction]],
                              preferredReadReplica: Option[Int],
                              isReassignmentFetch: Boolean)

object TopicMap {
  private val topicIdMap = new ConcurrentHashMap[String, Uuid].asScala
  private val lastRecordMap = new ConcurrentHashMap[TopicPartition, Record].asScala
  private val topicOffsetMap = new ConcurrentHashMap[TopicPartition, AtomicInteger].asScala
  private val productId = new AtomicInteger(0)
  private val groupInstanceIdMap = new ConcurrentHashMap[String, String].asScala
  //  private val
  val node = new Node(11, "localhost", 9999)

  def getOrCreateTopic(topic: String): Uuid = {
    topicIdMap.putIfAbsent(topic, Uuid.randomUuid())
    topicIdMap.getOrElse(topic, Uuid.ZERO_UUID)
  }

  def isTopicExist(topic: String): Boolean = {
    topicIdMap.keySet.contains(topic)
  }

  def getTopicMapView(): Map[Uuid, String] = {
    val value = mutable.Map.empty[Uuid, String]
    topicIdMap.foreach(r => value += r.swap)
    value.toMap
  }

  def initialize(topic: String): Boolean = {
    topicIdMap.contains(topic)
  }

  def getOrSetLastRecord(topic: TopicPartition, record: Option[Record]): Option[Record] = {
    record.map(r => lastRecordMap.put(topic, r))
    lastRecordMap.get(topic)
  }

  def addOffset(topic: TopicPartition, size: Option[Int]): Unit = {
    topicOffsetMap.putIfAbsent(topic, new AtomicInteger(0))
    topicOffsetMap.get(topic).map(_.getAndAdd(size.getOrElse(1)))
  }

  def getOffset(topic: TopicPartition): Int = {
    topicOffsetMap.get(topic).map(_.get()).getOrElse(0)
  }

  def getProductId(): Int = {
    productId.getAndIncrement()
  }

  def getGroupInstanceId(groupName: String): String = {
    groupInstanceIdMap.putIfAbsent(groupName, Uuid.randomUuid().toString)
    groupInstanceIdMap.getOrElse(groupName, "")
  }


}



