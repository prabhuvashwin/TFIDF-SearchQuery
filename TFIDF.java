/// Author: Ashwin Venkatesh Prabhu
/// UNCC ID: 800960400
/// Email: avenka11@uncc.edu

package org.myorg;

import java.io.IOException;
import java.util.regex.Pattern;
import java.util.HashMap;
import java.lang.*;
import org.myorg.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

/// TFIDF is calculated after the term frequency is calculated. The output of the term frequency
/// is used as the input for TFIDF
/// Output of this program would be 'word#####filename	tfidf', where '#####' is a delimiter
/// between word and filename. 'tfidf' is the TFIDF of the word in the file. A tab is present
/// after the filename which serves as a delimiter between filename and TFIDF value.
public class TFIDF extends Configured implements Tool {

   private static final Logger LOG = Logger .getLogger( TFIDF.class);
   
   /// main() method is the starting point of the program. TermFrequency is chained here, and before
   /// TFDIF map/reduce functions are called, TermFrequency map/reduce methods get executed
   public static void main( String[] args) throws  Exception {
	  ToolRunner .run( new TermFrequency(), args);
      int res  = ToolRunner .run( new TFIDF(), args);
      System .exit(res);
   }
   
   /// run() method is responsible for defining a job object. In this case, it sets the input
   /// path (which is the output of TermFrequency - OUTPUT_PATH/tf) and the output path (a new 
   /// folder is created for TFIDF output - OUTPUT_PATH/tfidf). It sets the mapper and reducer 
   /// classes, and the datatypes for Output Key/Value pair which is Text/DoubleWritable respectively.
   /// The functions also calculates the number of files in the input path and passes it to
   /// map/reduce function. The value is a part of TFIDF calculation
   public int run( String[] args) throws  Exception {
      
	  /// Number of files in the input is calculated
	  int numberOfFiles = FileSystem.get(getConf()).listStatus(new Path(args[0])).length;

      Job job = Job .getInstance(getConf(), " tfidf ");
      job.setJarByClass( this .getClass());
      
      /// Number of files in the input passed to the map/reduce functions
      job.getConfiguration().setInt("numberOfFiles", numberOfFiles);
            
      FileInputFormat.addInputPath(job, new Path(args[1] + "/tf"));
      FileOutputFormat.setOutputPath(job, new Path(args[1] + "/tfidf"));
      job.setMapperClass(Map .class);
      job.setReducerClass(Reduce .class);
      job.setMapOutputKeyClass(Text .class);
      job.setMapOutputValueClass(Text .class);
      job.setOutputKeyClass(Text .class);
      job.setOutputValueClass(DoubleWritable .class);
      
      return job.waitForCompletion(true) ? 0 : 1;
   }
   
   /// This is the Mapper class which hosts the map function. Here, the input is the output of 
   /// TermFrequency and is processed and passed to reduce function.
   /// An example output of this map function is,
   /// <"yellow", "file2.txt=1.0”> <"Hadoop", "file2.txt=1.0”> <”is”, “file2.txt=1.0”> 
   /// <”elephant”, “file2.txt=1.0”> <"yellow", "file1.txt=1.0”> <"Hadoop", "file1.txt=1.3010299956639813"> 
   /// <”is”, “file1.txt=1.0”> <”an”, “file2.txt=1.0”>
   public static class Map extends Mapper<LongWritable ,  Text ,  Text ,  Text > {
	  
	  public void map( LongWritable offset,  Text lineText,  Context context)
		        throws  IOException,  InterruptedException {
		  Text key_word = new Text();
		  Text value_filename_tf = new Text();
		  
		  /// The line here is split into two parts. Part 1 contains the 'word#####filename'
		  /// and Part 2 contains the term frequency value
		  String[] line = lineText.toString().split("\\b(\\s*\\t+)\\b");
		  
		  /// The Part 1 is again split on the delimiter '#####' and we get word and filename
		  /// separated
		  String[] key_word_filename = line[0].toString().split("#####");
		  
		  /// We now have the word from the line
		  key_word.set(key_word_filename[0]);
		  
		  /// A string is created here which combines the filename and term frequency values as
		  /// filename = termfrequency value
		  value_filename_tf.set(key_word_filename[1] + "=" + line[1]);
		  
		  /// The output is then <word, filename = termfrequency value>
		  context.write(key_word, value_filename_tf);
	  }
   }
   
   /// This is the Reducer class which hosts the reduce function. Here, the input is received
   /// from the map function, and the output is stored into OUTPUT_PATH/tfidf
   /// TFIDF calculation is explained below:
   /// TF-IDF consist of two parts - TF which is TermFrequency and IDF which is Inverse Document Frequency
   /// We already have TF values, and we have to calculate IDF values as a part of this reduce function
   /// IDF(t) = log10(1 + Total # of documents / # documents containing term t)
   /// TF-IDF(t, d) = WF(t, d) * IDF(t)
   public static class Reduce extends Reducer<Text ,  Text ,  Text ,  DoubleWritable > {
	   
	   @Override
	   public void reduce( Text word,  Iterable<Text > counts,  Context context)
		         throws IOException,  InterruptedException {
		   
		   /// This object will hold the TFIDF value
		   double tfidf = 0.0;
		   
		   /// This object will hold the 'word#####filename' in the final output
		   Text key = new Text();
		   
		   /// This object will hold the value of # of documents containing term t
		   int numberOfDocumentsWithWord = 0;
		   
		   /// This hashmap will hold all the filenames in which the word appears in
		   /// and the corresponding term frequencies
		   HashMap<String, Double> values = new HashMap<String, Double>();
		   
		   for (Text value : counts ) {
			   /// Value of each Key/Value pair is split on delimiter '='
			   String[] filename_tf = value.toString().split("=");
			   
			   /// # of total documents with term t is calculated
			   numberOfDocumentsWithWord++;
			   
			   /// The filename and termfrequency value for the word is added to the HashMap
			   values.put(filename_tf[0], Double.valueOf(filename_tf[1]));
		   }
		   
		   /// Am intermediate input (HashMap) is created here and it looks something like below:
		   /// <"Hadoop", ["file1.txt=1.3010299956639813", "file2.txt=1.0"]> 
		   /// <"is", ["file1.txt=1.0", "file2.txt=1.0"]>
		   /// <"yellow", ["file1.txt=1.0", "file2.txt=1.0"]>
		   /// <"an", ["file2.txt=1.0"]>
		   /// <"elephant", ["file2.txt=1.0"]>
		   
		   /// # of files in the input folder. This is passed to the reduce function while
		   /// creating job
		   int numberOfFiles = context.getConfiguration().getInt("numberOfFiles", 0);
		   
		   /// IDF calculation
		   double idf = Math.log10(1 + (numberOfFiles / numberOfDocumentsWithWord));
		   
		   /// TFIDF is calculated for each word inside the hashmap
		   for (String v : values.keySet()) {
			   /// Key is set to 'word#####filename"
			   key.set(word.toString() + "#####" + v);
			   
			   /// TFIDF is calculated for word in each file by multiplying the term frequency
			   /// with the IDF value
			   tfidf = values.get(v) * idf;
			   
			   context.write(key, new DoubleWritable(tfidf));
		   }
	   }
   }
}
