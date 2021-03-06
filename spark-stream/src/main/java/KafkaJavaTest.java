import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class KafkaJavaTest {

    private static final String TOPIC_NAME = "topic";
    private static final int MAX_NO_MESSAGE_FOUND_COUNT = 2;


    public static Producer < Long, String > createProducer ( String topic ) {

        String bootstrapServers = "ip-20-0-31-210.ec2.internal:9092";
        Properties props = new Properties ( );
        props.put ( "bootstrap.servers" , bootstrapServers );
        props.put ( "key.serializer" ,
                "org.apache.kafka.common.serialization.StringSerializer" );
        props.put ( "value.serializer" ,
                "org.apache.kafka.common.serialization.StringSerializer" );
        return new
                KafkaProducer <> ( props );
    }


    public static Consumer < Long, String > createConsumer ( String topic ) {
        String bootstrapServers = "ip-20-0-31-210.ec2.internal:9092";
        Properties props = new Properties ( );
        props.put ( "group.id" , "group1" );
        props.put ( ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG ,
                bootstrapServers );
        props.put ( ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG ,
                StringDeserializer.class.getName ( ) );
        props.put ( ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG ,
                StringDeserializer.class.getName ( ) );
        // props.put(ConsumerConfig.GROUP_ID_CONFIG,"group1");
        props.put ( ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG , "false" );
        props.put ( ConsumerConfig.AUTO_OFFSET_RESET_CONFIG ,
                "earliest" );

        final Consumer < Long, String > consumer = new KafkaConsumer <> ( props );
        TopicPartition topicPartition0 = new TopicPartition ( topic ,
                0 );
        TopicPartition topicPartition1 = new TopicPartition ( topic ,
                1 );
        consumer.subscribe ( topicPartition0 , topicPartition1 );
        consumer.subscribe ( topic );

        return consumer;

    }


    private static void runConsumer () {
        Consumer < Long, String > consumer = createConsumer ( TOPIC_NAME );

        int noMessageToFetch = 0;

        while (true) {
            final Map < String, ConsumerRecords < Long, String > > consumerRecordsMap = consumer.poll ( 1000 );
            if (consumerRecordsMap.size ( ) == 0) {
                noMessageToFetch++;
                if (noMessageToFetch > MAX_NO_MESSAGE_FOUND_COUNT)
                    break;
                else
                    continue;
            }


            Collection < ConsumerRecords < Long, String > > consumerRecords = consumerRecordsMap.values ( );

            for (ConsumerRecords < Long, String > consumerRecordList : consumerRecords) {
                List < ConsumerRecord < Long, String > > consumerRecord = consumerRecordList.records ( );
                consumerRecord.forEach ( record -> {
                    try {
                        System.out.println ( "Record Key " + record.key ( ) );
                        System.out.println ( "Record value " + record.value ( ) );
                        System.out.println ( "Record partition " + record.partition ( ) );

                        System.out.println ( "Record offset " + record.offset ( ) );
                        System.out.println ( "Record offset " + record.topic () );


                    } catch ( Exception e ) {
                        System.out.println ( "Error" );
                        e.printStackTrace ( );
                    }
                } );
                consumer.commit ( true );
            }
        }
        consumer.close ( );
    }


    private static void runProducer () {
        Producer < Long, String > producer = createProducer ( TOPIC_NAME );

        for (int i = 0; i < MAX_NO_MESSAGE_FOUND_COUNT; i++) {
            ProducerRecord < Long, String > record = new
                    ProducerRecord <> ( TOPIC_NAME , "This is record no " + i );
            try {
                RecordMetadata metadata = producer.send ( record ).get ( );
                System.out.println ( "Record sent with key " + i + " to partition " + metadata.partition ( )
                        + " with offset " + metadata.offset ( ) );
            } catch ( ExecutionException e ) {
                System.out.println ( "Error in sending record" );
                System.out.println ( e );
            } catch ( InterruptedException e ) {
                System.out.println ( "Error in sending record" );
                System.out.println ( e );
            }
        }
    }

    public static void main ( String[] args ) {

		runProducer();
        runConsumer ( );
    }

}

