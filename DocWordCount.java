/// Author: Ashwin Venkatesh Prabhu
/// UNCC ID: 800960400
/// Email: avenka11@uncc.edu

package org.myorg;

import java.io.IOException;
import java.util.regex.Pattern;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

/// DocWordCount is a modification of the WordCount program, where this program outputs the 
/// word count for each distinct file.
/// Output of this program would be 'word#####filename	count', where '#####' is a delimiter
/// between word and filename. A tab is present after the filename which serves as a delimiter
/// between filename and count.
public class DocWordCount extends Configured implements Tool {

   private static final Logger LOG = Logger .getLogger( DocWordCount.class);
   
   /// main() method is the starting point of the program
   public static void main( String[] args) throws  Exception {
      int res  = ToolRunner .run( new DocWordCount(), args);
      System .exit(res);
   }
   
   /// run() method is responsible for defining a job object. In this case, it sets the input
   /// path and the output path. It sets the mapper and reducer classes, and the datatypes for
   /// Output Key/Value pair which is Text/IntWritable respectively.
   public int run( String[] args) throws  Exception {
      Job job  = Job .getInstance(getConf(), " docwordcount ");
      job.setJarByClass( this .getClass());

      FileInputFormat.addInputPaths(job,  args[0]);
      FileOutputFormat.setOutputPath(job,  new Path(args[ 1] + "/docwordcount"));
      job.setMapperClass( Map .class);
      job.setReducerClass( Reduce .class);
      job.setOutputKeyClass( Text .class);
      job.setOutputValueClass( IntWritable .class);

      return job.waitForCompletion( true)  ? 0 : 1;
   }
   
   /// This is the Mapper class which hosts the map function. Here, the input is taken from the
   /// input path specified and is processed and passed to reduce function
   public static class Map extends Mapper<LongWritable ,  Text ,  Text ,  IntWritable > {
      private final static IntWritable one  = new IntWritable( 1);
      private Text word  = new Text();

      private static final Pattern WORD_BOUNDARY = Pattern .compile("\\s*\\b\\s*");

      public void map( LongWritable offset,  Text lineText,  Context context)
        throws  IOException,  InterruptedException {
    	  	 
    	  	 /// All the input is converted to lowercase here. 
         String line  = lineText.toString().toLowerCase();
         Text currentWord  = new Text();
         
         /// Inbuilt FileSplit object is used to get the filename of the input line.
         FileSplit file = (FileSplit) context.getInputSplit();
         String fileName = file.getPath().getName();
         
         /// The line is split into words and a new string is created which looks like
         /// 'word#####fileName'. This is the Key here. The Value is a constant
         /// IntWritable object with value 1. These are added as Key/Value pairs and
         /// passed onto the reduce function.
         for ( String word  : WORD_BOUNDARY .split(line)) {
            if (word.isEmpty()) {
               continue;
            }
            currentWord  = new Text(word + "#####" + fileName);
            context.write(currentWord,one);
         }
      }
   }
   
   /// This is the Reducer class which hosts the reduce function. Here, the input is received
   /// from the map function, and the output is stored into the path specified
   public static class Reduce extends Reducer<Text ,  IntWritable ,  Text ,  IntWritable > {
      @Override 
      public void reduce( Text word,  Iterable<IntWritable > counts,  Context context)
         throws IOException,  InterruptedException {
         int sum  = 0;
         
         /// For every word which appears N number of times, this methods gets the value of N
         /// and stores it along with the word
         for ( IntWritable count  : counts) {
            sum  += count.get();
         }
         
         /// This line corresponds to the output 'word#####filename	count'
         context.write(word,  new IntWritable(sum));
      }
   }
}
