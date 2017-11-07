/// Author: Ashwin Venkatesh Prabhu
/// UNCC ID: 800960400
/// Email: avenka11@uncc.edu

package org.myorg;

import java.io.IOException;
import java.util.List;
import java.util.Scanner;
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
import org.apache.hadoop.util.GenericOptionsParser;

/// Search.java will search for certain keyword passed as queries and output their
/// TFIDF values. The input for this program is the output fo TFIDF and the output
/// is stored in the path OUTPUT_PATH/search
public class Search extends Configured implements Tool {

   private static final Logger LOG = Logger .getLogger( Search.class);
   
   /// main() method is the starting point of the program
   public static void main( String[] args) throws  Exception {
      int res  = ToolRunner .run( new Search(), args);
      System .exit(res);
   }
   
   /// run() method is responsible for defining a job object. In this case, it sets the input
   /// path and the output path. It sets the mapper and reducer classes, and the datatypes for
   /// Output Key/Value pair which is Text/DoubleWritable respectively.
   /// Queries passed as command line arguments are extracted and passed as string to
   /// map/reduce functions, where each query is separated by a whitespace
   public int run( String[] args) throws  Exception {
	  
      Job job  = Job .getInstance(getConf(), " search ");
      job.setJarByClass( this .getClass());

      FileInputFormat.addInputPaths(job,  args[0]);
      FileOutputFormat.setOutputPath(job,  new Path(args[ 1] + "/search"));
      job.setMapperClass( Map .class);
      job.setReducerClass( Reduce .class);
      job.setOutputKeyClass( Text .class);
      job.setOutputValueClass( DoubleWritable .class);
      
      String queries = "";
      for (int i = 2; i < args.length; i++) {
    	  queries += args[i] + " ";
      }
      job.getConfiguration().set("queries", queries);
      
      return job.waitForCompletion( true)  ? 0 : 1;
   }
   
   /// This is the Mapper class which hosts the map function. Here, the input is taken from the
   /// input path (Output of TFIDF calculation) specified and is processed and passed to reduce function
   public static class Map extends Mapper<LongWritable ,  Text ,  Text ,  DoubleWritable > {
	  
	  public void map( LongWritable offset,  Text lineText,  Context context)
		        throws  IOException,  InterruptedException {
		  
		  /// Queries to be search for are extracted and split using whitespace delimiter
		  String[] queries = context.getConfiguration().get("queries").toString().split(" ");
		  
		  for (String q : queries) {
			  /// Checks if the query is present in the line text
			  if (lineText.toString().contains(q)) {
				  /// If the query is present, then the line is split of the delimiter '#####"
				  /// After the split, we get 'word' and 'filename	tfidf_value'
				  String[] word_value = lineText.toString().split("#####");
				  
				  /// 'filename	tfidf_value' are also split
				  String[] filename_tfidf = word_value[1].toString().split("\\b(\\s*\\t+)\\b");
				  
				  /// filename is converted to Text and tfidf_value is converted to DoubleWritable
				  /// and passed to reduce function
				  context.write(new Text(filename_tfidf[0]), new DoubleWritable(Double.parseDouble(filename_tfidf[1])));
			  }
			  
		  }
		  
	  }
   }
   
   /// This is the Reducer class which hosts the reduce function. Here, the input is received
   /// from the map function, and the output is stored into the path specified
   public static class Reduce extends Reducer<Text ,  DoubleWritable ,  Text ,  DoubleWritable > {
	   
	   @Override
	   public void reduce( Text word,  Iterable<DoubleWritable > counts,  Context context)
		         throws IOException,  InterruptedException {
		   double sum = 0.0;
		   
		   /// Take sum of all the tfidf values for all the files where the word appears 
		   for (DoubleWritable count : counts ) {
			   sum += count.get();
		   }
		   
		   /// The output is generated as word	tfidf_sum
		   context.write(word, new DoubleWritable(sum));
	   }
   }
}
