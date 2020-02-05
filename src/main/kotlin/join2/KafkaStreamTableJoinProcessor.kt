package join2

import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.kstream.Grouped
import org.apache.kafka.streams.kstream.Joined
import org.apache.kafka.streams.kstream.KStream
import org.apache.kafka.streams.kstream.KTable
import org.springframework.cloud.stream.annotation.EnableBinding
import org.springframework.cloud.stream.annotation.Input
import org.springframework.cloud.stream.annotation.Output
import org.springframework.cloud.stream.annotation.StreamListener
import org.springframework.messaging.handler.annotation.SendTo

@EnableBinding(KafkaStreamTableJoinProcessor.StreamProcessor::class)
class KafkaStreamTableJoinProcessor {

    @StreamListener
    @SendTo(StreamProcessor.OUTPUT)
    fun process(
        @Input(StreamProcessor.INPUT1) userClicksStream: KStream<String, Long>,
        @Input(StreamProcessor.INPUT2) userRegionsTable: KTable<String, String>
    ): KStream<String, Long> =
        userClicksStream
            .leftJoin(
                userRegionsTable,
                { clicks, region ->
                    if (region == null) {
                        RegionWithClicks("UNKNOWN", clicks)
                    }
                    else {
                        RegionWithClicks(region, clicks)
                    }
                },
                Joined.with(Serdes.String(), Serdes.Long(), null)
            )
            .map { _, regionWithClicks ->
                KeyValue(regionWithClicks.region, regionWithClicks.clicks)
            }
            .groupByKey(Grouped.with(Serdes.String(), Serdes.Long()))
            .reduce { firstClicks, secondClicks ->
                firstClicks + secondClicks
            }
            .toStream()

    internal interface StreamProcessor {
        @Input(INPUT1)
        fun usersClicksInStream(): KStream<String, Long>

        @Input(INPUT2)
        fun usersRegionsInStream(): KTable<String, String>

        @Output(OUTPUT)
        fun clicksRegionsOutStream(): KStream<String, Long>

        companion object {
            const val INPUT1 = "usersClicksIn"
            const val INPUT2 = "usersRegionsIn"
            const val OUTPUT = "clicksRegionsOut"
        }
    }

}