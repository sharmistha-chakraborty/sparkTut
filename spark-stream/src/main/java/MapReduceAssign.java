import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.StringTokenizer;

public class MapReduceAssign {

    public static class Map extends Mapper < LongWritable, Text, Text, IntWritable > {
        public void map ( LongWritable key , Text value , Context context ) throws IOException, InterruptedException {
            String line = value.toString ( );
            StringTokenizer tokenizer = new StringTokenizer ( line );
            while (tokenizer.hasMoreTokens ( )) {
                value.set ( StringUtils.lowerCase (  tokenizer.nextToken ( )) );
                context.write ( value , new IntWritable ( 1 ) );
            }
        }
    }

        public static class Reduce extends Reducer < Text, IntWritable, Text, IntWritable > {
            public void reduce ( Text key , Iterable < IntWritable > values , Context context )
                    throws IOException, InterruptedException {
                int sum = 0;
                for (IntWritable x : values) {
                    sum += x.get ( );
                }
                if (sum < 100) {
                    context.write ( key , new IntWritable ( sum ) );
                }
            }
        }

        public static void main ( String[] args ) throws IOException, ClassNotFoundException, InterruptedException {
            Configuration conf = new Configuration ( );
            Job job = new Job ( conf , "word_occurence_less_100" );
            job.setJarByClass ( MapReduceAssign.class );
            job.setMapperClass ( Map.class );
            job.setReducerClass ( Reduce.class );
            job.setOutputKeyClass ( Text.class );
            job.setOutputValueClass ( IntWritable.class );
            job.setInputFormatClass ( TextInputFormat.class );
            job.setOutputFormatClass ( TextOutputFormat.class );
            Path outputPath = new Path ( args[1] );
            //Configuring the input/output path from the filesystem into the job
            FileInputFormat.addInputPath ( job , new Path ( args[0] ) );
            FileOutputFormat.setOutputPath ( job , new Path ( args[1] ) );
            //deleting the output path automatically from hdfs so that we don't have to delete it explicitly
            outputPath.getFileSystem ( conf ).delete ( outputPath );
            //exiting the job only if the flag value becomes false
            System.exit ( job.waitForCompletion ( true ) ? 0 : 1 );
        }

}


