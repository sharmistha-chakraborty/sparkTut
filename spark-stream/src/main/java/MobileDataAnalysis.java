import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import scala.Tuple2;

import java.util.*;

public class MobileDataAnalysis {

    public static void main ( String[] arr ) throws InterruptedException {

        if (!Logger.getRootLogger ( ).getAllAppenders ( ).hasMoreElements ( )) {
            Logger.getRootLogger ( ).setLevel ( Level.WARN );
        }

        SparkConf sparkConf = new SparkConf ( );
        sparkConf.setAppName ( "streamDataAnalysis" );
        sparkConf.setMaster ( "local[2]" );
        try{
            JavaStreamingContext streamingContext = new JavaStreamingContext ( sparkConf , Durations.seconds ( 10 ) );
            Collection < String > topics = Collections.singletonList ( "mobileDataIngest" );
            streamingContext.checkpoint ( "./.checkpoint" );

            Map < String, Object > kafkaParams = new HashMap <> ( );
            kafkaParams.put ( "bootstrap.servers" , "ip-20-0-31-210.ec2.internal:9092" );
            kafkaParams.put ( "value.deserializer" , StringDeserializer.class );
            kafkaParams.put ( "key.deserializer" , StringDeserializer.class );
            kafkaParams.put ( "group.id" , "sparkStream" );
            kafkaParams.put ( "auto.offset.reset" , "latest" );
            kafkaParams.put ( "enable.auto.commit" , false );

            JavaInputDStream < ConsumerRecord < String, String > > stream =
                    KafkaUtils.createDirectStream ( streamingContext ,
                            LocationStrategies.PreferConsistent ( ) ,
                            ConsumerStrategies.Subscribe ( topics , kafkaParams ) );
            if (stream != null) {

                JavaPairDStream < String, String > results =
                        stream.mapToPair ( record -> new Tuple2 <> ( record.key ( ) , record.value ( ) ) );
                JavaDStream < String > lines = results.map ( Tuple2::_2 );
                lines.foreachRDD ( new VoidFunction2 < JavaRDD < String >, Time > ( ) {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public void call ( JavaRDD < String > rdd , Time time ) throws Exception {
                        if (rdd.count ( ) > 0) {
                            rdd.saveAsTextFile ( "/input/spark" + time.milliseconds ( ) );
                        }
                    }
                } );
                streamingContext.start ( );
                streamingContext.awaitTermination ( );

                streamingContext.stop ( );
            }
        } catch ( InterruptedException e ) {
            e.printStackTrace ( );
        }
    }
}


