package join2

import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.common.utils.Bytes
import org.apache.kafka.streams.kstream.KTable
import org.apache.kafka.streams.kstream.Materialized
import org.apache.kafka.streams.state.KeyValueStore
import org.springframework.cloud.stream.annotation.EnableBinding
import org.springframework.cloud.stream.annotation.Input
import org.springframework.cloud.stream.annotation.StreamListener

@EnableBinding(KafkaStreamTableJoinMaterializer.StreamProcessor::class)
class KafkaStreamTableJoinMaterializer {

    @StreamListener
    fun process(
        @Input(StreamProcessor.INPUT) aggregatedStuff: KTable<String, Long>
    ): Unit =
        aggregatedStuff
            .mapValues(
                { it -> it },
                Materialized
                    .`as`<String, Long, KeyValueStore<Bytes, ByteArray>>(StreamProcessor.STORE)
                    .withKeySerde(Serdes.String())
                    .withValueSerde(Serdes.Long())
            )
            .ignore()

    internal interface StreamProcessor {
        @Input(INPUT)
        fun clicksRegionsInStream(): KTable<String, Long>

        companion object {
            const val INPUT = "clicksRegionsIn"
            const val STORE = "clicksRegionsTable"
        }
    }

}