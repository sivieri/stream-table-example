package join2

import org.apache.commons.lang3.exception.ExceptionUtils
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.streams.errors.DefaultProductionExceptionHandler
import org.apache.kafka.streams.errors.ProductionExceptionHandler

class CustomProductionExceptionHandler: DefaultProductionExceptionHandler() {

    override fun handle(
        record: ProducerRecord<ByteArray, ByteArray>,
        exception: Exception
    ): ProductionExceptionHandler.ProductionExceptionHandlerResponse {
        println("HERE!!! ${exception::class.java}")
        println(ExceptionUtils.getStackTrace(exception))
        println("DOUNF!!!")
        return super.handle(record, exception)
    }

}