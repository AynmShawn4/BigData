package ca.uwaterloo.cs.bigdata2016w.AynmShawn4.assignment1;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;
import java.util.HashSet;
import java.util.HashMap; 
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.lang.Math;
import java.lang.String;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.ParserProperties;

import tl.lin.data.pair.PairOfStrings;
import tl.lin.data.map.HMapStIW;
import tl.lin.data.map.HMapStFW;


public class StripesPMI  extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(StripesPMI.class);

  protected static class MyMapper extends Mapper<LongWritable, Text, Text, HMapStFW> {
    private static final Text TEXT = new Text();

    @Override
    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
      String line = ((Text) value).toString();

      HashMap<String, HMapStFW> stripes = new HashMap<String, HMapStFW>();

      StringTokenizer itr = new StringTokenizer(line);

      int cnt = 0;
      HashSet<String> set = new HashSet<String>();
      while (itr.hasMoreTokens()) {
        cnt++;
        String w = itr.nextToken().toLowerCase().replaceAll("(^[^a-z]+|[^a-z]+$)", "");
        if (w.length() == 0) continue;
        set.add(w);
        if (cnt >= 100) break;
      }

      String[] words = new String[set.size()];
      words = set.toArray(words);


      for (int i = 0; i < set.size(); i++){

        for (int j = 0; j < set.size(); j++){

          if (i == j){
            continue;
          }
          if (stripes.containsKey(words[i])) {
            HMapStFW stripe = stripes.get(words[i]);
            if (stripe.containsKey(words[j])) {
              stripe.put(words[j], stripe.get(words[j])+1.0f);
            } else {
              stripe.put(words[j], 1.0f);
            }
          } else {
            HMapStFW stripe = new HMapStFW();
            stripe.put(words[j], 1.0f);
            stripes.put(words[i], stripe);
          }
 
        }
      }

      for (String t : stripes.keySet()) {
        TEXT.set(t);
        context.write(TEXT, stripes.get(t));
      }

    }
  }

  private static class MyCombiner extends Reducer<Text, HMapStFW, Text, HMapStFW> {
    @Override
    public void reduce(Text key, Iterable<HMapStFW> values, Context context)
        throws IOException, InterruptedException {
      Iterator<HMapStFW> iter = values.iterator();
      HMapStFW map = new HMapStFW();

      while (iter.hasNext()) {
        map.plus(iter.next());
      }

      context.write(key, map);
    }
  }

  private static class MyReducer extends Reducer<Text, HMapStFW, Text, HMapStFW> {

    private static final HashMap<String, Float> hMap = new HashMap<String, Float>();


    @Override
    public void setup (Context context) throws IOException{
          try{
                  FileSystem fs = FileSystem.get(new Configuration());
                  FileStatus[] status = fs.listStatus(new Path("./lineNumber/part-r-00000"));
                  
                  for (int i = 0; i < status.length; i++){
                          BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(status[i].getPath())));
                          String line;
                          line=br.readLine();

                          while (line != null){
                            line = line.replaceAll("\\(", "").replaceAll("\\)","").replaceAll("(\\,)(\\s+)(\\*)"," ");
                            String[] countNum = new String[2];
                            countNum = line.split("\\s+");
                        
                            String left = countNum[0];

                            String right = countNum[1];
                            float f = Float.parseFloat(right);


                            hMap.put(left, f);
                            line = br.readLine();

                          }
                  }
          }catch(Exception e){
                  System.out.println("***********************File not found");
          }
        }

    @Override
    public void reduce(Text key, Iterable<HMapStFW> values, Context context)
        throws IOException, InterruptedException {
      Iterator<HMapStFW> iter = values.iterator();
      HMapStFW map = new HMapStFW();

      while (iter.hasNext()) {
        map.plus(iter.next());
      }

      for (String term : map.keySet()) {
        //map.put(term, map.get(term) / sum);

        float total = hMap.get("*");


        float coprob = map.get(term)/total;

        float leftprob =   hMap.get(key.toString()) /total;


        float rightprob = hMap.get(term ) / total;

        float pmi = (float)Math.log10(coprob / (leftprob * rightprob));

        map.put(term, pmi);



      }

      context.write(key,map);

      
    }
  }

  /**
   * Creates an instance of this tool.
   */
  private StripesPMI() {}

  public static class Args {
    @Option(name = "-input", metaVar = "[path]", required = true, usage = "input path")
    public String input;

    @Option(name = "-output", metaVar = "[path]", required = true, usage = "output path")
    public String output;

    @Option(name = "-reducers", metaVar = "[num]", required = false, usage = "number of reducers")
    public int numReducers = 1;

    @Option(name = "-textOutput", required = false, usage = "use TextOutputFormat (otherwise, SequenceFileOutputFormat)")
    public boolean textOutput = false;
  }

  /**
   * Runs this tool.
   */
  public int run(String[] argv) throws Exception {
    Args args = new Args();
    CmdLineParser parser = new CmdLineParser(args, ParserProperties.defaults().withUsageWidth(100));

    try {
      parser.parseArgument(argv);
    } catch (CmdLineException e) {
      System.err.println(e.getMessage());
      parser.printUsage(System.err);
      return -1;
    }

    LOG.info("Tool name: " + StripesPMI.class.getSimpleName());
    LOG.info(" - input path: " + args.input);
    LOG.info(" - output path: " + args.output);
    LOG.info(" - num reducers: " + args.numReducers);
    LOG.info(" - text output: " + args.textOutput);

    Job job = Job.getInstance(getConf());
    job.setJobName(StripesPMI.class.getSimpleName());
    job.setJarByClass(StripesPMI.class);

    job.setNumReduceTasks(args.numReducers);

    FileInputFormat.setInputPaths(job, new Path(args.input));
    FileOutputFormat.setOutputPath(job, new Path(args.output));

    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(HMapStFW.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(HMapStFW.class);
 //   if (args.textOutput) {
 //     job.setOutputFormatClass(TextOutputFormat.class);
 //   } else {
 //     job.setOutputFormatClass(SequenceFileOutputFormat.class);
  //  }

    job.setMapperClass(MyMapper.class);
    job.setCombinerClass(MyCombiner.class);
    job.setReducerClass(MyReducer.class);

    // Delete the output directory if it exists already.
    Path outputDir = new Path(args.output);
    FileSystem.get(getConf()).delete(outputDir, true);

    long startTime = System.currentTimeMillis();
    job.waitForCompletion(true);
    System.out.println("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");

    return 0;
  }

  /**
   * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
   */
  public static void main(String[] args) throws Exception {
    ToolRunner.run(new StripesPMI(), args);
  }
}
