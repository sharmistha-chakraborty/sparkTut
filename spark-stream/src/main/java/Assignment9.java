import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class Assignment9 {

     static class EmployeeMapper extends Mapper < LongWritable, Text,IntWritable,Text>
    {
        public void map(LongWritable k,Text v, Context con)throws IOException, InterruptedException
        {
            String line = v.toString();
            String[] w=line.split(",");
            int empNo = Integer.parseInt (w[0]);
            int sal=Integer.parseInt(w[5]);
            int dept=Integer.parseInt(w[7]);
            con.write(new IntWritable ( empNo), new Text( sal +","+ dept ));
        }
    }

     static class EmployeeReducer extends Reducer <IntWritable,Text, IntWritable,Text>
    {
        public void reduce(IntWritable empNo, Iterable<Text> vlist, Context con)
                throws IOException , InterruptedException
        {
            int max=0;
            Text val = new Text (  );

            for(Text v:vlist)
            {
                String line = v.toString();
                String[] w=line.split(",");
                int sal=Integer.parseInt(w[0]);
                int dept = Integer.parseInt ( w[1] );
                max=Math.max(max, sal);
                val = new Text (max + ","+ dept);
            }
            con.write(empNo, val);
        }

    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf=new Configuration();
        Job job=Job.getInstance(conf,"maxsalary");
        job.setJarByClass(Assignment9.class);
        job.setMapperClass(EmployeeMapper.class);
        job.setReducerClass(EmployeeReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path (args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }


}
