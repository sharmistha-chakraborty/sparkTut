import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

public class FlumeDataAnalysis {


    public static void main ( String[] arr ) {
        if (!Logger.getRootLogger ( ).getAllAppenders ( ).hasMoreElements ( )) {
            Logger.getRootLogger ( ).setLevel ( Level.WARN );
        }
        SparkConf sparkConf = new SparkConf ( );
        sparkConf.setAppName ( "flumeDataAnalysis" );
        sparkConf.setMaster ( "local[2]" );
        JavaSparkContext sc = new JavaSparkContext ( sparkConf );


        SQLContext sqlContext = new SQLContext ( sc );

        Dataset < Row > datasetCSV = sqlContext.read ( )
                .format ( "com.databricks.spark.csv" )
                .option ( "inferSchema" , "true" )
                .option ( "mode" , "DROPMALFORMED" )
                .option ( "header" , "true" )
                .load ( "/user/edureka_570471/flume_sink/transport/*" );

        if (!datasetCSV.isEmpty ( )) {
            processData ( datasetCSV , sqlContext );
            sc.stop ( );
        }
    }

    private static void processData ( Dataset < Row > datasetType , SQLContext sqlContext ) {
        datasetType.createOrReplaceTempView ( "flume_data" );
        datasetType.registerTempTable ( "flume_data" );

        //Find out the stops (StopName), which are frequently searched, and have the least distance (<3)
        // from the existing stops so that the operations team can check them and add them to the routes
        // or create the new route. Find out the top 20 StopNames.
        Dataset<Row> top20StopNames = sqlContext.sql ( "select StopID, count(StopID) as count_stop_id, StopName from flume_data" +
                " where MilesAway<3 group by StopID, StopName" +
                " order by count_stop_id desc" );
        top20StopNames.createOrReplaceTempView ("top20StopNames_Data"  );
        top20StopNames.registerTempTable ( "top20StopNames_Data" );
        top20StopNames.write ( ).mode ( "overwrite" ).format ( "csv" ).save ( "/user/edureka_570471/flumeOut/top20StopNames.csv" );

        //if we include these 20 stops in the bus route, what will be the max number of bookings we will get.
        sqlContext.sql ( "Select max(NumberOfBoardings), t.StopName from flume_data d join top20StopNames_Data t" +
                " on d.StopID=t.StopID group by t.StopName")
        .write ( ).mode ( "overwrite" ).format ( "csv" ).save ( "/user/edureka_570471/flumeOut/maxBooking.csv" );


    }

}
