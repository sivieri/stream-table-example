package join2

import org.apache.kafka.streams.processor.StreamPartitioner
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration

@Configuration
class MainConfig {

    @Bean
    fun clicksRegionsStreamPartitioner(): StreamPartitioner<String, Long> =
        StreamPartitioner { _, key, _, _ ->
            if (key.first() in 'a'..'c') 0
            else 1
        }

}