import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.sql.*;
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

import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;
import java.sql.SQLException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ClickStreamAnalysis {
    private static final String regPattern = "^(\\S+) (\\S+) (\\S+) (\\[\\d{2}\\/(\\S+)\\/\\d{4}\\:\\d{2}\\:\\d{2}\\:\\d{2} \\+\\d{4}\\]) (\\\"(\\S+)+) \\/(\\S+)(\\S+)\\s*(\\S+)?\\s*\\\" (\\d{3}) (\\S+) (\\S+) (.*)";
    private static final String date = "(\\[\\d{2}\\/(\\S+)\\/\\d{4}\\:\\d{2}\\:\\d{2}\\:\\d{2}  \\d{4}\\])";
    static SparkConf sparkConf = new SparkConf ( )
            .setMaster ( "local[2]" )
            .setAppName ( "clickStreamAnalysis" );
    static JavaSparkContext sc = new JavaSparkContext ( sparkConf );
    private static SQLContext sqlContext = new SQLContext ( sc );

    public static void main(String [] args) throws SQLException, ClassNotFoundException {
        streamDataAnalysisDOSAttack ( );
        streamDataAnalysisResponseCode();

    }

    private static void sendMail()
    {
        String to = "ITSupportTeam@xxx.com";//change accordingly
        String from = "ITSecurityTeam@xxx.com";
        String host = "localhost";//or IP address

        //Get the session object
        Properties properties = System.getProperties();
        properties.setProperty("mail.smtp.host", host);
        Session session = Session.getDefaultInstance(properties);

        //compose the message
        try{
            MimeMessage message = new MimeMessage(session);
            message.setFrom(new InternetAddress(from));
            message.addRecipient( Message.RecipientType.TO,new InternetAddress (to));
            message.setSubject("DOSAttack Alert");
            message.setText("****Urgent****  ");

            // Send message
            Transport.send(message);
            System.out.println("message sent successfully....");

        }catch ( MessagingException mex) {mex.printStackTrace();}
}

    private static void streamDataAnalysisDOSAttack () throws SQLException, ClassNotFoundException {
        try {
            JavaStreamingContext streamingContext =
                    new JavaStreamingContext ( sc , Durations.seconds ( 1 ) );
            Collection < String > topics = Collections.singletonList ( "webDataIngest" );
            streamingContext.checkpoint ( "./.checkpoint" );

            Map < String, Object > kafkaParams = new HashMap <> ( );
            kafkaParams.put ( "bootstrap.servers" , "localhost:9092" );
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
                    int count = 0;

                    @Override
                    public void call ( JavaRDD < String > rdd , Time time ) throws Exception {
                        final Pattern pattern = Pattern.compile(regPattern, Pattern.MULTILINE);
                        final Matcher matcher = pattern.matcher(rdd.toString ());
                        String validRecords = matcher.group (1) +"," + matcher.group ( 4 )
                                + "," +matcher.group ( 8 )+","+matcher.group ( 11 ) + "," +matcher.group ( 12 ) + "," + matcher.group ( 14 );
                        count++;
                        if (count > 100) {
                            sendMail();
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

    private static void streamDataAnalysisResponseCode () throws SQLException, ClassNotFoundException {
        try {
            JavaStreamingContext streamingContext =
                    new JavaStreamingContext ( sc , Durations.seconds ( 300 ) );
            Collection < String > topics = Collections.singletonList ( "webDataIngest" );
            streamingContext.checkpoint ( "./.checkpoint" );

            Map < String, Object > kafkaParams = new HashMap <> ( );
            kafkaParams.put ( "bootstrap.servers" , "localhost:9092" );
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
                        final Pattern pattern = Pattern.compile(regPattern, Pattern.MULTILINE);
                        final Matcher matcher = pattern.matcher(rdd.toString ());
                        String validRecords = matcher.group (1) +"," + matcher.group ( 4 )
                                + "," +matcher.group ( 8 )+","+matcher.group ( 11 ) + "," +matcher.group ( 12 ) + "," + matcher.group ( 14 );



                        List<String> failStatusList = new ArrayList <> (  );
                        List<String> successStatusList = new ArrayList <> (  );

                        List<String> catList = new ArrayList <> (  );

                        String[] s = validRecords.split ( "," );
                        String status = s[4];
                        String categoryStr = s[2];
                        String[] catStrArr = categoryStr.split ( "/" );
                        String cat = catStrArr[0];
                        if(cat.equalsIgnoreCase ( "digital-cameras" )){
                            catList.add ( validRecords );
                        }
                        System.out.println ( "The number of requests containing category digital-camera"+catList.size () );
                        if ( !status.equals ( "200" ) ) {
                            failStatusList.add ( validRecords );
                            if(failStatusList.size ()>500) {
                                sendMail ( );
                            }
                        }else if(status.equals ( "200" )){
                           successStatusList.add ( validRecords );
                        }
                        System.out.println ( "The number of failure status code="+failStatusList.size () );
                        System.out.println ( "The number of success status code="+successStatusList.size ());

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
