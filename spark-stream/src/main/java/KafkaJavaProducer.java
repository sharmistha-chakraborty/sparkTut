import java.util.Properties;
import java.util.Scanner;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class KafkaJavaProducer {
    public static void main ( String[] args ) {
        String bootstrapServers = "ip-20-0-31-210.ec2.internal:9092";
        Properties props = new Properties ( );
        props.put ( "bootstrap.servers" , bootstrapServers );
        props.put ( "key.serializer" ,
                "org.apache.kafka.common.serialization.StringSerializer" );
        props.put ( "value.serializer" ,
                "org.apache.kafka.common.serialization.StringSerializer" );
        String topic_name = args[0];
        int no_of_messages = Integer.parseInt ( args[1] );
        KafkaProducer < String, String > producer = new
                KafkaProducer <> ( props );
        @SuppressWarnings("resource")
        Scanner sc = new Scanner ( System.in );
        for (int i = 0; i < no_of_messages; i++) {
            ProducerRecord < String, String > record = new
                    ProducerRecord ( topic_name , sc.next ( ) );
            producer.send ( record );
        }
        producer.close ( );
    }
}
