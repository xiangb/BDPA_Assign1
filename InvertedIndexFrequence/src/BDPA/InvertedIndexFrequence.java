package BDPA;



import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Collections;

public class InvertedIndexFrequence extends Configured implements Tool {
	



   public static void main(String[] args) throws Exception {
      System.out.println(Arrays.toString(args));
      int res = ToolRunner.run(new Configuration(), new InvertedIndexFrequence(), args);
      
      System.exit(res);
   }

   @Override
   public int run(String[] args) throws Exception {
      System.out.println(Arrays.toString(args));
      Job job = new Job(getConf(), "InvertedIndex");
      job.setJarByClass(InvertedIndexFrequence.class);

      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(Text.class);

      job.setMapperClass(Map.class);
      job.setReducerClass(Reduce.class);

     

      job.setInputFormatClass(TextInputFormat.class);
      job.setOutputFormatClass(TextOutputFormat.class);
      

      FileInputFormat.addInputPath(job, new Path(args[0]));
      FileOutputFormat.setOutputPath(job, new Path(args[1]));

      job.waitForCompletion(true);
      
      return 0;
   }
   
   public static class Map extends Mapper<LongWritable, Text, Text, Text> {
      
      private Text word = new Text();
      private Text document = new Text();

     

      @Override
      public void map(LongWritable key, Text value, Context context)
              throws IOException, InterruptedException {

        /* Initialize a hashset variable, set of strings without duplicates*/
        HashSet<String> stopwords = new HashSet<String>();

        /* Read file of stopwords*/
        BufferedReader Reader = new BufferedReader(
                new FileReader(
                        new File(
                                "/home/cloudera/workspace/Assignment1/stopwords.txt")));

        /* Add each line (word) in the variable stopwords*/
        String pattern;
        while ((pattern = Reader.readLine()) != null) {
            stopwords.add(pattern.toLowerCase());
        }

        /* get the name of the document need 
         * org.apache.hadoop.mapreduce.lib.input.FileSplit */
        String doc = ((FileSplit) context.getInputSplit()).getPath().getName();
        

        document = new Text(doc);

         for (String token: value.toString().replaceAll("[^a-zA-Z ]", "").split("\\s+")) {

          /* If the word is not in the stop words list we write it*/
        	 	
        	if (!stopwords.contains(token.toLowerCase())) {
                word.set(token.toLowerCase());
            }

            /* Associate word with the name of the document */
            context.write(word, document);
                
        	 	
        

         }
      }
   }
   

   
   public static class Reduce extends Reducer<Text, Text, Text, Text> {
      @Override
      public void reduce(Text key, Iterable<Text> values, Context context)
              throws IOException, InterruptedException {


          /* Create a list which contains the values corresponding to the key */

    	  ArrayList <String> list_val = new ArrayList <String>();
          
          for (Text val : values){
        	  
              list_val.add(val.toString());
              
             }
          /* Set of unique document */
          
          HashSet<String> setvalue = new HashSet<String>(list_val);
          
          /* Build a string as a concatenation of the name of the documents in which we find the key (word)*/
         
			StringBuilder reducedvalue = new StringBuilder();
          for (String val : setvalue) {
        	 
             if (reducedvalue.length() !=0){
               reducedvalue.append(",");
             }
             
             /* Count the number of val (document name) in list_val which corresponds to the number
              * of times the key appears in this document*/
             
            reducedvalue.append(val+"#"+Collections.frequency(list_val, val));
            
          }
          

          
          context.write(key, new Text(reducedvalue.toString()));
          
          
       }
    }
 }