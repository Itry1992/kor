package kafka.server.netty

import com.tong.kafka.common.network.ClientInformation
import com.tong.kafka.common.requests.RequestHeader

class AdapterRequest(val header: RequestHeader, val channelId: String, val clientInformation: ClientInformation,

                    ) {

}
