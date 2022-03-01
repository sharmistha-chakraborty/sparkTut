import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import scala.Serializable;
import scala.Tuple2;

import java.sql.SQLException;
import java.util.*;

public class CreditCardFraudAnalysis {
    static SparkConf sparkConf = new SparkConf ( )
            .setMaster ( "local[2]" )
            .setAppName ( "creditCardFraudAnalysis" );
    static JavaSparkContext sc = new JavaSparkContext ( sparkConf );
    private static SQLContext sqlContext = new SQLContext ( sc );


    private static void batchDataAnalysis () throws SQLException, ClassNotFoundException {

        if (!Logger.getRootLogger ( ).getAllAppenders ( ).hasMoreElements ( )) {
            Logger.getRootLogger ( ).setLevel ( Level.WARN );
        }

        //Bureau/merchants/zip codes/transaction data are
        // ingested into the processing layer for data cleaning

        Dataset < Row > bureauCSV = sqlContext.read ( )
                .format ( "com.databricks.spark.csv" )
                .option ( "inferSchema" , "true" )
                .option ( "mode" , "DROPMALFORMED" )
                .option ( "header" , "true" )
                .load ( "./test_data/frauddet/bureau_data.csv" )
                .coalesce ( 1 );
        bureauCSV.withColumn ( "timeStamp" , bureauCSV.col ( "timeStamp" ).cast ( DataTypes.TimestampType ) );
        bureauCSV.createOrReplaceTempView ( "bureau_data" );
        bureauCSV.registerTempTable ( "bureau_data" );

        // Store the processed data into a HDFS directory
        bureauCSV.write ( ).mode ( "overwrite" ).format ( "csv" )
                .save ( "./test_data/bureau_data.csv" );

        StructType schemamerchantData = new StructType ( );
        schemamerchantData = schemamerchantData.add ( "mid" , DataTypes.StringType , true );
        schemamerchantData = schemamerchantData.add ( "merchant_id" , DataTypes.IntegerType , true );
        schemamerchantData = schemamerchantData.add ( "merchant_type" , DataTypes.StringType , true );
        schemamerchantData = schemamerchantData.add ( "status" , DataTypes.StringType , true );
        schemamerchantData = schemamerchantData.add ( "country_code" , DataTypes.StringType , true );
        schemamerchantData = schemamerchantData.add ( "postal_code" , DataTypes.StringType , true );
        schemamerchantData = schemamerchantData.add ( "joined_date" , DataTypes.TimestampType , true );
        schemamerchantData = schemamerchantData.add ( "updates_date" , DataTypes.TimestampType , true );
        schemamerchantData = schemamerchantData.add ( "acceptable_types" , DataTypes.StringType , true );

        Dataset < Row > merchantCSV = sqlContext.read ( )
                .format ( "com.databricks.spark.csv" )
                .schema ( schemamerchantData )
                .option ( "mode" , "DROPMALFORMED" )
                .option ( "header" , "true" )
                .load ( "./test_data/frauddet/merchant_data.csv" )
                .coalesce ( 1 );
        merchantCSV.printSchema ( );

        merchantCSV.createOrReplaceTempView ( "merchant_data" );
        merchantCSV.registerTempTable ( "merchant_data" );

        // Store the processed data into a HDFS directory
        merchantCSV.write ( ).mode ( "overwrite" ).format ( "csv" )
                .save ( "./test_data/merchant_data.csv" );


        StructType schemaZipCode = new StructType ( );
        schemaZipCode = schemaZipCode.add ( "country_code" , DataTypes.StringType , true );
        schemaZipCode = schemaZipCode.add ( "postal_code" , DataTypes.StringType , true );
        schemaZipCode = schemaZipCode.add ( "place_name" , DataTypes.StringType , true );
        schemaZipCode = schemaZipCode.add ( "admin_name1" , DataTypes.StringType , true );
        schemaZipCode = schemaZipCode.add ( "admin_code1" , DataTypes.StringType , true );
        schemaZipCode = schemaZipCode.add ( "admin_name2" , DataTypes.StringType , true );
        schemaZipCode = schemaZipCode.add ( "admin_code2" , DataTypes.StringType , true );
        schemaZipCode = schemaZipCode.add ( "admin_name3" , DataTypes.StringType , true );
        schemaZipCode = schemaZipCode.add ( "admin_code3" , DataTypes.StringType , true );
        schemaZipCode = schemaZipCode.add ( "latitude" , DataTypes.DoubleType , true );
        schemaZipCode = schemaZipCode.add ( "longitude" , DataTypes.DoubleType , true );
        schemaZipCode = schemaZipCode.add ( "accuracy" , DataTypes.IntegerType , true );

        Dataset < Row > zipCodeCSV = sqlContext.read ( )
                .format ( "com.databricks.spark.csv" )
                .schema ( schemaZipCode )
                .option ( "sep" , "\t" )
                .option ( "header" , true )
                .option ( "mode" , "DROPMALFORMED" )
                .option ( "encoding" , "UTF-8" )
                .load ( "./test_data/frauddet/zipcodes.txt" )
                .coalesce ( 1 );
        zipCodeCSV.createOrReplaceTempView ( "zip_codes" );
        zipCodeCSV.registerTempTable ( "zip_codes" );

        // Store the processed data into a HDFS directory
        zipCodeCSV.write ( ).mode ( "overwrite" )
                .csv ( "./test_data/zip_codes.csv" );

        StructType schemaTransactionData = new StructType ( );
        schemaTransactionData = schemaTransactionData.add ( "card_id" , DataTypes.IntegerType , true );
        schemaTransactionData = schemaTransactionData.add ( "user_id" , DataTypes.IntegerType , true );
        schemaTransactionData = schemaTransactionData.add ( "merchant_id" , DataTypes.StringType , true );
        schemaTransactionData = schemaTransactionData.add ( "amount" , DataTypes.IntegerType , true );
        schemaTransactionData = schemaTransactionData.add ( "timestamp" , DataTypes.TimestampType , true );
        schemaTransactionData = schemaTransactionData.add ( "type" , DataTypes.StringType , true );
        schemaTransactionData = schemaTransactionData.add ( "transaction_desc" , DataTypes.StringType , true );
        schemaTransactionData = schemaTransactionData.add ( "status" , DataTypes.StringType , true );

        Dataset < Row > transactionDataCSV = sqlContext.read ( )
                .format ( "com.databricks.spark.csv" )
                .schema ( schemaTransactionData )
                .option ( "mode" , "DROPMALFORMED" )
                .load ( "./test_data/frauddet/transactions.csv" )
                .coalesce ( 1 );

        transactionDataCSV.createOrReplaceTempView ( "transaction_data" );
        transactionDataCSV.registerTempTable ( "transaction_data" );

        // Store the processed data into a HDFS directory
        transactionDataCSV.write ( ).mode ( "overwrite" ).format ( "csv" )
                .save ( "./test_data/transaction_data.csv" );


        if (!bureauCSV.isEmpty ( ) && !merchantCSV.isEmpty ( ) &&
                !zipCodeCSV.isEmpty ( ) && !transactionDataCSV.isEmpty ( )) {
            List < Dataset > datasetList = new ArrayList <> ( );
            datasetList.add ( bureauCSV );
            datasetList.add ( merchantCSV );
            datasetList.add ( zipCodeCSV );
            datasetList.add ( transactionDataCSV );
            for (Dataset dataset : datasetList) {
                dataCleaningAndEnrichment ( dataset , sqlContext );
            }

            Dataset < Row > merchantDistanceData = sqlContext.sql ( "Select z.latitude, z.longitude," +
                    " m.merchant_id from zip_codes z join merchant_data m on\n" +
                    "z.postal_code = m.postal_code and z.country_code = m.country_code" );
            merchantDistanceData.show ( );
            merchantDistanceData.createOrReplaceTempView ( "merchant_distance" );
            merchantDistanceData.registerTempTable ( "merchant_distance" );

        }
    }

    //Do all necessary data clean-ups & data transformation needed for further steps

    private static void dataCleaningAndEnrichment ( Dataset < Row > datasetType , SQLContext sqlContext ) {
        Dataset < Row > dataSetWithoutNullRec = datasetType.na ( ).fill ( "" );
    }

    private static Dataset < Row > streamDataAnalysis () throws SQLException, ClassNotFoundException {
        final Dataset < Row >[] currentTransaction = null;
        Properties connection = getConnection ();
        try {
            JavaStreamingContext streamingContext =
                    new JavaStreamingContext ( sc , Durations.seconds ( 1 ) );
            Collection < String > topics = Collections.singletonList ( "posDataIngest" );
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
                SparkSession session = SparkSession.builder ( ).config ( sparkConf ).getOrCreate ( );

                lines.foreachRDD ( new VoidFunction < JavaRDD < String > > ( ) {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public void call ( JavaRDD < String > rdd ) throws SQLException, ClassNotFoundException {
                        if (rdd.count ( ) > 0) {

                            currentTransaction[0] = session.createDataFrame ( rdd , RealTimeRecord.class );
                            Dataset < Row > cardBlackListTable = returnUpdatedCardBlackListTable ( "card_blacklist" );
                            cardBlackListTable.createOrReplaceTempView ( "card_blackList" );
                            cardBlackListTable.registerTempTable ( "card_blackList" );
                            if((cardBlackListTable.select (cardBlackListTable.col ( "card_id" )))
                                    .equals( currentTransaction[0].select ( currentTransaction[0].col ( "cardId" )))) {
                                currentTransaction[0].write ( ).mode ( SaveMode.Append ).jdbc ( connection.getProperty ( "jdbcUrl" ) ,
                                        "transaction_updated_real" , connection );

                            }
                            Dataset < Row > posBlackListTable = returnUpdatedCardBlackListTable ( "pos_blacklist" );
                            posBlackListTable.createOrReplaceTempView ( "pos_blacklist" );
                            posBlackListTable.registerTempTable ( "pos_blacklist" );
                            if((posBlackListTable.select (posBlackListTable.col ( "merchant_id" )))
                                    .equals( currentTransaction[0].select ( currentTransaction[0].col ( "merchantId" )))) {
                                currentTransaction[0].write ( ).mode ( SaveMode.Append ).jdbc ( connection.getProperty ( "jdbcUrl" ) ,
                                        "transaction_updated_real" , connection );

                            }
                        }
                    }
                } );
                streamingContext.start ( );
                streamingContext.awaitTermination ( );

            }
        } catch ( InterruptedException e ) {
            e.printStackTrace ( );
        }
        return currentTransaction[0];
    }

    //Analyse Fraudulent transactions based on below three logics

    private static void captureFraudTrans () throws SQLException, ClassNotFoundException {

        //1. Distance over time (DoT) Logic to catch fraudulent transactions

        Dataset < Row > lastTransactionData = sqlContext.sql ( "select *, t.timestamp, md.latitude, md.longitude from transaction_data t join " +
                "merchant_distance md on t.merchant_id = md.merchant_id " +
                "order by t.timestamp limit 1" );
        lastTransactionData.createOrReplaceTempView ( "last_transaction_data" );
        lastTransactionData.registerTempTable ( "last_transaction_data" );


        Dataset < Row > currentTransactionData = streamDataAnalysis ( );
        currentTransactionData.createOrReplaceTempView ( "current_transaction_data" );
        currentTransactionData.registerTempTable ( "current_transaction_data" );

        int distanceDiff = sqlContext.sql ( "select (6371 * acos(" +
                "cos( radians(c.latitude) ) * cos( radians( l.latitude ) ) * cos( radians( l.longitude ) - radians(c.longitude) )" +
                "+ sin( radians(c.latitude) ) * sin( radians( l.latitude ) ) ) ) as distance " +
                " from last_transaction_data l join current_transaction_data c" +
                " on l.card_id = c.cardId" ).first ( ).getInt ( 0 );
        int timeDiff = sqlContext.sql ( "select (from_unixtime(l.timestamp,'mm') - " +
                "(from_unixtime(c.timestamp,'mm') as time " +
                "from last_transaction_data l join current_transaction_data c" ).first ( ).getInt ( 0 );

        int dot = distanceDiff / timeDiff;


        //2.Credit Score to find fraudulent transactions

        Dataset < Row > creditScoreData = sqlContext.sql ( "select t.user_id, b.credit_score from transaction_data t join bureau_data b" +
                " on t.user_id = b.user_id " +
                "where b.credit_score < 250" );
        int creditScore = creditScoreData.first ( ).getInt ( 1 );

        //3.Transaction Amount to find fraudulent transactions

        int utl = sqlContext.sql ( "select 2*(stddev(amount)+avg(amount))) as utl" +
                "from transaction_data" +
                "order by timestamp desc limit 5" ).first ( ).getInt ( 0 );
        Dataset < Row > amt = sqlContext.sql ( "select b.user_id, sum(t.amount), 5*(stddev(t.amount)+avg(t.amount))) as utl" +
                "from transaction_data t join bureau_data b on t.user_id = b.user_id" +
                "group by b.user_id" +
                "order by timestamp desc limit 5" );
        int ctl = amt.first ( ).getInt ( 2 );
        int amount = amt.first ( ).getInt ( 1 );
        String userId = amt.first ( ).getString ( 0 );

        Properties connection = getConnection ( );
        Dataset < Row > transactionDataHis = sqlContext.sql ( "select * from transaction_data" );

        int count = 0;
        if (dot > 13 || creditScore < 250 || amount > ctl) {
            transactionDataHis.withColumn ( "status" , transactionDataHis.col ( "status" )
                    .$eq$eq$eq ( "Suspicious" ) ).where ( "user_id =" + userId );
            transactionDataHis.write ( ).mode ( SaveMode.Append ).jdbc ( connection.getProperty ( "jdbcUrl" ) ,
                    "transaction_updated" , connection );
            count++;
            if(count==3) {
                posFraudTrans ( transactionDataHis , connection );
            }
        } else if (amount > utl && amount < ctl) {
            transactionDataHis.withColumn ( "status" , transactionDataHis.col ( "status" )
                    .$eq$eq$eq ( "Approval Pending" ) ).where ( "user_id =" + userId );
            transactionDataHis.write ( ).mode ( SaveMode.Append ).jdbc ( connection.getProperty ( "jdbcUrl" ) ,
                    "transaction_updated" , connection );
        } else {
            transactionDataHis.withColumn ( "status" , transactionDataHis.col ( "status" )
                    .$eq$eq$eq ( "Approved" ) ).where ( "user_id =" + userId );
            transactionDataHis.write ( ).mode ( SaveMode.Append ).jdbc ( connection.getProperty ( "jdbcUrl" ) ,
                    "transaction_updated" , connection );
        }

        Dataset < Row > updatedTransactions = returnUpdatedTransTable ( "transaction_updated" );
        updatedTransactions.createOrReplaceTempView ( "transactions" );
        updatedTransactions.registerTempTable ( "transactions" );

    }

    //Write scripts to implement Lost card & Fraud_PoS

    private static void lostCardTrans () throws SQLException, ClassNotFoundException {
        Properties connection = getConnection ();
        Dataset < Row > lostCardData =
                sqlContext.sql ( "select card_id, now() as lost_date "+
                "from transactions where status_trans = 'Suspicious' limit 3" );
        lostCardData.write ( ).mode ( SaveMode.Append ).jdbc ( connection.getProperty ( "jdbcUrl" ) ,
                "card_lost" , connection );

    }

    private static void posFraudTrans (Dataset<Row> posTransDot, Properties connection) throws SQLException, ClassNotFoundException {
        posTransDot.write ( ).mode ( SaveMode.Append ).jdbc ( connection.getProperty ( "jdbcUrl" ) ,
                "pos_blacklist" , connection );


    }

        //Update transaction table and insert data into card_blacklist and PoS_blacklist tables

    private static Properties getConnection () throws ClassNotFoundException, SQLException {
        String user = "admin";
        String pwd = "admin";
        String url = "jdbc:mysql://localhost:3306/testEdu?autoReconnect=true&useSSL=false";
        String jdbcDriver = "com.mysql.jdbc.Driver";
        Properties connection = new Properties ( );
        connection.put ( "jdbcDriver" , jdbcDriver );
        connection.put ( "jdbcUrl" , url );
        connection.put ( "user" , user );
        connection.put ( "password" , pwd );
        connection.put ( "dbname" , "testEdu" );

        //Broadcast < Properties > br = sc.broadcast ( connection );
        //Properties conn = br.getValue ( );
        Class.forName ( connection.getProperty ( "driver" ) );
        return connection;
    }

    private static Dataset < Row > returnUpdatedTransTable ( String tableName ) throws SQLException, ClassNotFoundException {

        Dataset < Row > updatedTransTable =
                sqlContext.read ( )
                        .format ( "jdbc" )
                        .option ( "driver" , "com.mysql.jdbc.Driver" )
                        .option ( "url" , "jdbc:mysql://localhost:3306/testEdu?autoReconnect=true&useSSL=false" )
                        .option ( "dbtable" , tableName )
                        .option ( "user" , "admin" )
                        .option ( "password" , "admin" )
                        .load ( );
        return updatedTransTable;
    }

    private static Dataset < Row > returnUpdatedCardBlackListTable ( String tableName ) throws SQLException, ClassNotFoundException {

        Dataset < Row > cardBlackListTable =
                sqlContext.read ( )
                        .format ( "jdbc" )
                        .option ( "driver" , "com.mysql.jdbc.Driver" )
                        .option ( "url" , "jdbc:mysql://localhost:3306/testEdu?autoReconnect=true&useSSL=false" )
                        .option ( "dbtable" , tableName )
                        .option ( "user" , "admin" )
                        .option ( "password" , "admin" )
                        .load ( );
        return cardBlackListTable;
    }

    public static void main ( String[] arr ) throws SQLException, ClassNotFoundException {
        batchDataAnalysis ( );
        streamDataAnalysis ( );
        captureFraudTrans ( );
        lostCardTrans ( );
        sc.stop ( );
    }

    static class RealTimeRecord implements Serializable {
        String cardId;
        String merchantId;
        Double amt;
        Integer timestamp;
        String type;
        String transDesc;
        Double latitude;
        Double longitude;

        public String getCardId () {
            return cardId;
        }

        public void setCardId ( String cardId ) {
            this.cardId = cardId;
        }

        public String getMerchantId () {
            return merchantId;
        }

        public void setMerchantId ( String merchantId ) {
            this.merchantId = merchantId;
        }

        public Double getAmt () {
            return amt;
        }

        public void setAmt ( Double amt ) {
            this.amt = amt;
        }

        public Integer getTimestamp () {
            return timestamp;
        }

        public void setTimestamp ( Integer timestamp ) {
            this.timestamp = timestamp;
        }

        public String getType () {
            return type;
        }

        public void setType ( String type ) {
            this.type = type;
        }

        public String getTransDesc () {
            return transDesc;
        }

        public void setTransDesc ( String transDesc ) {
            this.transDesc = transDesc;
        }

        public Double getLatitude () {
            return latitude;
        }

        public void setLatitude ( Double latitude ) {
            this.latitude = latitude;
        }

        public Double getLongitude () {
            return longitude;
        }

        public void setLongitude ( Double longitude ) {
            this.longitude = longitude;
        }


    }
}


