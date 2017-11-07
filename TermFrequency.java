/// Author: Ashwin Venkatesh Prabhu
/// UNCC ID: 800960400
/// Email: avenka11@uncc.edu

package org.myorg;

import java.io.IOException;
import java.util.regex.Pattern;
import java.lang.*;
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
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

/// TermFrequency is a modification of the DocWordCount program, where this program outputs the 
/// term frequency of each word appearing in a file.
/// Output of this program would be 'word#####filename	w', where '#####' is a delimiter
/// between word and filename. 'w' is the term frequency of the word in the file. A tab is present
/// after the filename which serves as a delimiter between filename and term frequency.
///
/// This file is not called directly, but is chained with a call to TFIDF.java. More on this is
/// explained in TFIDF.java
public class TermFrequency extends Configured implements Tool {

   private static final Logger LOG = Logger .getLogger( TermFrequency.class);
   
   /// main() method is the starting point of the program
   public static void main( String[] args) throws  Exception {
      int res  = ToolRunner .run( new TermFrequency(), args);
      System .exit(res);
   }

   /// run() method is responsible for defining a job object. In this case, it sets the input
   /// path and the output path. It sets the mapper and reducer classes, and the datatypes for
   /// Output Key/Value pair which is Text/DoubleWritable respectively.
   
   /// The output path here is internally handled and is the input for the map function for
   /// TFIDF. If OUTPUT_PATH is what the user passes in, then the output after calculating
   /// term frequency will be present at OUTPUT_PATH/tf
   public int run( String[] args) throws  Exception {
      Job job  = Job .getInstance(getConf(), " termfrequency ");
      job.setJarByClass( this .getClass());

      FileInputFormat.addInputPaths(job,  args[0]);
      FileOutputFormat.setOutputPath(job,  new Path(args[ 1] + "/tf"));
      job.setMapperClass( Map .class);
      job.setReducerClass( Reduce .class);
      job.setOutputKeyClass( Text .class);
      job.setOutputValueClass( DoubleWritable .class);

      return job.waitForCompletion( true)  ? 0 : 1;
   }
   
   /// This is the Mapper class which hosts the map function. Here, the input is taken from the
   /// input path specified and is processed and passed to reduce function. The code written inside
   /// this mapper function is same as that of DocWordCount map function
   public static class Map extends Mapper<LongWritable ,  Text ,  Text ,  DoubleWritable > {
      private final static DoubleWritable one  = new DoubleWritable( 1.0);
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
   public static class Reduce extends Reducer<Text ,  DoubleWritable ,  Text ,  DoubleWritable > {
      @Override 
      public void reduce( Text word,  Iterable<DoubleWritable > counts,  Context context)
         throws IOException,  InterruptedException {
         double sum  = 0.0;
         
         /// For every word which appears N number of times, this methods gets the value of N
         /// and stores it along with the word
         for ( DoubleWritable count  : counts) {
        	 	if (count.get() > 0.0) {
        	 		sum  += count.get();
        	 	}
         }
         
         /// TermFrequency, WF(t, d) = 1 + log10(TF(t, d)), if TF(t, d) > 0, and 0 otherwise
         /// Here, t is the term, and d is the document. TF(t, d) is No of times t appears 
         /// in document d
         DoubleWritable tf = new DoubleWritable(1.0 + Math.log10(sum));
         context.write(word, tf);
      }
   }
}
