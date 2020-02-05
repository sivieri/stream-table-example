package join2

import org.apache.kafka.clients.producer.Partitioner
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.Cluster
import org.apache.kafka.common.serialization.LongSerializer
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.kafka.streams.KeyValue
import org.springframework.kafka.core.DefaultKafkaProducerFactory
import org.springframework.kafka.core.KafkaTemplate

object Producers {

    @JvmStatic
    fun main(args: Array<String>) {
        val producerProps1 = mapOf(
            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to "localhost:9092",
            ProducerConfig.RETRIES_CONFIG to 0,
            ProducerConfig.BATCH_SIZE_CONFIG to 16384,
            ProducerConfig.LINGER_MS_CONFIG to 1,
            ProducerConfig.BUFFER_MEMORY_CONFIG to 33554432,
            ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java,
            ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG to LongSerializer::class.java,
            ProducerConfig.PARTITIONER_CLASS_CONFIG to CustomPartitioner::class.java
        )

        val userClicks =
            listOf(
                KeyValue("alice", 13L),
                KeyValue("bob", 4L),
                KeyValue("chao", 25L),
                KeyValue("bob", 19L),
                KeyValue("dave", 56L),
                KeyValue("eve", 78L),
                KeyValue("alice", 40L),
                KeyValue("fang", 99L)
            )

        val pf = DefaultKafkaProducerFactory<String, Long>(producerProps1)
        val template = KafkaTemplate(pf, true)
        template.defaultTopic = "users-clicks-count"

        userClicks.forEach { keyValue ->
            template.sendDefault(keyValue.key, keyValue.value)
        }

        val userRegions =
            listOf(
                KeyValue(
                    "alice",
                    "asia"
                ),  /* Alice lived in Asia originally... */
                KeyValue("bob", "americas"),
                KeyValue("chao", "asia"),
                KeyValue("dave", "europe"),
                KeyValue(
                    "alice",
                    "europe"
                ),  /* ...but moved to Europe some time later. */
                KeyValue("eve", "americas"),
                KeyValue("fang", "asia")
            )

        val producerProps2 = mapOf(
            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to "localhost:9092",
            ProducerConfig.RETRIES_CONFIG to 0,
            ProducerConfig.BATCH_SIZE_CONFIG to 16384,
            ProducerConfig.LINGER_MS_CONFIG to 1,
            ProducerConfig.BUFFER_MEMORY_CONFIG to 33554432,
            ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java,
            ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java,
            ProducerConfig.PARTITIONER_CLASS_CONFIG to CustomPartitioner::class.java
        )

        val pf1 = DefaultKafkaProducerFactory<String, String>(producerProps2)
        val template1 = KafkaTemplate(pf1, true)
        template1.defaultTopic = "users-regions"

        userRegions.forEach { keyValue ->
            template1.sendDefault(keyValue.key, keyValue.value)
        }
    }

}

class CustomPartitioner: Partitioner {
    override fun configure(configs: MutableMap<String, *>) {
    }

    override fun close() {
    }

    override fun partition(topic: String, key: Any, keyBytes: ByteArray, value: Any, valueBytes: ByteArray, cluster: Cluster): Int {
        val first = (key as String).first()
        return if (first in 'a'..'c') 0
        else 1
    }

}