package join2

import org.apache.kafka.streams.state.QueryableStoreTypes.keyValueStore
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.cloud.stream.binder.kafka.streams.InteractiveQueryService
import org.springframework.context.ApplicationContext
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController


@RestController
class MainController {

    @Autowired
    private lateinit var interactiveQueryService: InteractiveQueryService

    @Autowired
    private lateinit var applicationContext: ApplicationContext

    @RequestMapping("/")
    fun home() {}

    @RequestMapping("/beans")
    fun beans(): String {
        val beanNames = applicationContext.beanDefinitionNames
        return beanNames.joinToString("<br>") { beanName ->
            beanName + " : " + applicationContext.getBean(beanName).javaClass.toString()
        }
    }

    @RequestMapping("/aggregated")
    fun aggregatedClicks(): String {
        val store = interactiveQueryService
            .getQueryableStore(
                KafkaStreamTableJoinMaterializer.StreamProcessor.STORE,
                keyValueStore<String, Long>()
            )
        return store
            .all()
            .asSequence()
            .toList()
            .joinToString("<br>") { keyValue ->
                "${keyValue.key}: ${keyValue.value}"
            }
    }

}