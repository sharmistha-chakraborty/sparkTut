import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

public class BatchDataAnalysis {


    public static void main ( String[] arr ) {
        if (!Logger.getRootLogger ( ).getAllAppenders ( ).hasMoreElements ( )) {
            Logger.getRootLogger ( ).setLevel ( Level.WARN );
        }
        SparkConf sparkConf = new SparkConf ( );
        sparkConf.setAppName ( "batchDataAnalysis" );
        sparkConf.setMaster ( "local[2]" );
        JavaSparkContext sc = new JavaSparkContext ( sparkConf );
        SparkSession spark = SparkSession.builder ( )
                .appName ( "XML to Dataframe" )
                .master ( "local" )
                .getOrCreate ( );

            SQLContext sqlContext = new SQLContext ( sc );

        //Infer schema of video_plays.xml and video_plays.csv files and ingest the data
        //Perform data cleaning steps like empty string replacements with actual NULL, data type
        //checks (including date format) and corrections/rejections, file name checks, empty file checks,
        //malformed record checks, and rejection, etc on both the batch and streaming data

        Dataset < Row > datasetXML = spark.read ( )
                .format ( "com.databricks.spark.xml" )
                .option ( "inferSchema" , "true" )
                .option ( "rowTag" , "record" )
                .load ( "./test_data/video_plays.xml" );

        Dataset < Row > datasetCSV = sqlContext.read ( )
                .format ( "com.databricks.spark.csv" )
                .option ( "inferSchema" , "true" )
                .option ( "mode" , "DROPMALFORMED" )
                .option ( "header" , "true" )
                .option ( "treatEmptyValuesAsNulls" , "true" )
                .option ( "timestampFormat" , "dd/MM/yyyy HH:MM:SS" )
                .load ( "./test_data/video_plays.csv" );

        if (!datasetCSV.isEmpty ( ) && !datasetXML.isEmpty ( )) {
            dataCleaningAndEnrichment ( datasetCSV , sqlContext );
            dataCleaningAndEnrichment ( datasetXML , sqlContext );

            sc.stop ( );
        }
    }

    //Use the following considerations during data enrichment
    //✓ If any value of like or dislike is NULL or absent, consider it as 0
    //✓ If fields like Geo_cd and creator_id are NULL or absent, consult the lookup tables
    //for fields Channel_id and Video_id respectively to get the values of Geo_cd and
    //creator _id
    //✓ If corresponding lookup entry is not found, consider the record of being invalid

    private static void dataCleaningAndEnrichment ( Dataset < Row > datasetType , SQLContext sqlContext ) {
        Dataset < Row > datasetLikedDisLikedNullValOrAbsentReplaceZero = datasetType
                .where ( datasetType.col ( "liked" ).isNull ( )
                        .and ( datasetType.col ( "liked" ).$eq$eq$eq ( "" ) )
                        .and ( datasetType.col ( "disliked" ).isNull ( ) )
                        .and ( datasetType.col ( "disliked" ).$eq$eq$eq ( "" ) ) )
                .withColumn ( "liked" , (datasetType.col ( "liked" ).$eq$eq$eq ( 0 )) )
                .withColumn ( "disliked" , (datasetType.col ( "disliked" ).$eq$eq$eq ( 0 )).cast ( DataTypes.StringType ) );

        Dataset < Row > datasetWithoutLikedAndDisliked = datasetType.except ( datasetLikedDisLikedNullValOrAbsentReplaceZero );
        Dataset < Row > newDataset = datasetWithoutLikedAndDisliked.union ( datasetLikedDisLikedNullValOrAbsentReplaceZero );
        newDataset.createOrReplaceTempView ( "processed_video_data" );
        newDataset.registerTempTable ( "processed_video_data" );
        // Store the processed data into a HDFS directory
        newDataset.write ( ).mode ( "overwrite" ).format ( "csv" ).save ( "./test_data/processed_video_data.csv" );

        //Fetch the most popular channels by the criteria of a maximum number of videos played, also
        //liked by unique users.
        sqlContext.sql ( "select channel_id, count(distinct(video_id)) as count_video, count(distinct(user_id)), " +
                "max(minutes_played) as max_video_played from processed_video_data group by channel_id," +
                "video_id order by max_video_played desc" );

        //Determine the total duration of videos played by each type of user, where the type of user can be 'subscribed' or
        // 'unsubscribed.' An unsubscribed user is the one whose video is either not present in the lookup table created from
        // the dataset - user-subscn.txt or has subscription_end_date earlier than the timestamp of the video played by him.
        Dataset < Row > userSubcnDataset = lookupTableCreation ( "user_subscn" , sqlContext );
        userSubcnDataset.createOrReplaceTempView ( "user_subscn" );

        Dataset < Row > durationAnalysis = sqlContext.sql ( "select T.user_type, T.duration from\n" +
                "(select \n" +
                "case " +
                "when from_unixtime(scn.subscription_start_date,'dd/MM/yyyy HH:mm:ss') is not null and from_unixtime(scn.subscription_end_date,'dd/MM/yyyy HH:mm:ss') is not null then 'subscribed'\n" +
                "when from_unixtime(scn.subscription_start_date,'dd/MM/yyyy HH:mm:ss') is null and from_unixtime(scn.subscription_end_date,'dd/MM/yyyy HH:mm:ss') is null then 'unsubscribed'\n" +
                "when from_unixtime(scn.subscription_end_date,'dd/MM/yyyy HH:mm:ss') < p.timestamp then 'unsubscribed'\n" +
                "end user_type,\n" +
                "from_unixtime(scn.subscription_start_date,'dd/MM/yyyy HH:mm:ss')   as scntime,\n" +
                "p.minutes_played as duration,\n" +
                "p.timestamp as time \n" +
                "from  processed_video_data p join user_subscn scn on\n" +
                "p.user_id = scn.user_id) T \n" +
                "group by  T.scntime,T.user_type, T.duration, T.time" );

        //Determine the list of connected creators. Connected creators are those whose videos are most watched by the unique users who
        // follow them. Output: It should include the columns – creator id, count of user
        Dataset < Row > userCreatorDataset = lookupTableCreation ( "user_creator" , sqlContext );
        userCreatorDataset.createOrReplaceTempView ( "user_creator" );

        sqlContext.sql ( "select u.creator_id, count(p.user_id) from \n" +
                "processed_video_data p join user_creator u\n" +
                "on p.user_id=u.user_id\n" +
                "group by u.creator_id" ).show ( 10 );

        //Determine which videos and creators are generating maximum revenue. Royalty applies to a video only if it was liked, completed successfully, or both. Output:
        // It should include the columns – video id, duration
        sqlContext.sql ( "select video_id, minutes_played as duration\n" +
                "from \n" +
                "processed_video_data \n" +
                "where liked=true and video_end_type=1" );

        //Determine the unsubscribed users who watched the videos for the longest duration.
        // Output: It should include the columns – user id, duration
        sqlContext.sql ( "select T.user_type, max(T.duration) from\n" +
                "(select \n" +
                "case \n" +
                "when from_unixtime(scn.subscription_start_date,'dd/MM/yyyy HH:mm:ss') is null and" +
                "from_unixtime(scn.subscription_end_date,'dd/MM/yyyy HH:mm:ss') is null then 'unsubscribed'\n" +
                "when from_unixtime(scn.subscription_end_date,'dd/MM/yyyy HH:mm:ss') < p.timestamp then 'unsubscribed'\n" +
                "end user_type, \n" +
                "p.minutes_played as duration \n" +
                "from  processed_video_data p join user_subscn scn on\n" +
                "p.user_id = scn.user_id) T \n" +
                "group by T.user_type, T.duration" ).show ( 10 );
    }


    private static Dataset < Row > lookupTableCreation ( String tableName , SQLContext sqlContext ) {

        //Create Lookup tables in any SQL/NoSQL database for the datasets video-creator.csv, usersubscn.csv, channel-geocd.csv, user-creator.csv
        Dataset < Row > datasetLookUpTable =
                sqlContext.read ( )
                        .format ( "jdbc" )
                        .option ( "driver" , "com.mysql.jdbc.Driver" )
                        .option ( "url" , "jdbc:mysql://localhost:3306/testEdu?autoReconnect=true&useSSL=false" )
                        .option ( "dbtable" , tableName )
                        .option ( "user" , "admin" )
                        .option ( "password" , "admin" )
                        .load ( );
        datasetLookUpTable.show ( );
        return datasetLookUpTable;


    }

}
