package ca.uwaterloo.cs.bigdata2016w.AynmShawn4.assignment4;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.ArrayList;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.apache.hadoop.io.Text;


import tl.lin.data.pair.PairOfObjectFloat;
import tl.lin.data.pair.PairOfFloatInt;

import tl.lin.data.array.ArrayListOfFloatsWritable;
import tl.lin.data.queue.TopScoredObjects;

public class ExtractTopPersonalizedPageRankNodes extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(ExtractTopPersonalizedPageRankNodes.class);

  private static class MyMapper extends
      Mapper<IntWritable, PageRankNode, IntWritable, PairOfFloatInt> {
    private ArrayList<TopScoredObjects<Integer> > queue = new ArrayList<TopScoredObjects<Integer> >();
    private static String[] ssource;
    private static int numSource;
    private static int[] sourcelist;
    private static final PairOfFloatInt OUT = new PairOfFloatInt();


    @Override
    public void setup(Context context) throws IOException {
      int k = context.getConfiguration().getInt("n", 100);
      //queue = new TopScoredObjects<Integer>(k);
      Configuration conf = context.getConfiguration();

      ssource = conf.get("SOURCE_FIELD").split(",");
      numSource = ssource.length;
     sourcelist = new int[numSource];


      for (int i =0; i < numSource; i++){
        TopScoredObjects<Integer> q = new TopScoredObjects<Integer>(k);
        queue.add( q );
        sourcelist[i] = Integer.parseInt(ssource[i]);


      }
    }

    @Override
    public void map(IntWritable nid, PageRankNode node, Context context) throws IOException,
        InterruptedException {
      for (int i = 0; i < numSource; i++){
        queue.get(i).add(node.getNodeId(), (float)node.getPageRank(i));
      }
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
      IntWritable key = new IntWritable();
      FloatWritable value = new FloatWritable();

      for (int i = 0; i < numSource; i++){
        for (PairOfObjectFloat<Integer> pair : queue.get(i).extractAll()) {
          key.set(pair.getLeftElement());

          OUT.set(pair.getRightElement(), i);
         // value.set(pair.getRightElement() + i );
          context.write(key, OUT);
        }
      }
    }
  }

  private static class MyReducer extends
      Reducer<IntWritable, PairOfFloatInt, Text, IntWritable> {
    private ArrayList<TopScoredObjects<Integer> > queue = new ArrayList<TopScoredObjects<Integer> >();
    private static String[] ssource ;
    private static final Text output = new Text();
        private static int numSource;
        private static int[] sourcelist;



    @Override
    public void setup(Context context) throws IOException {
      Configuration conf = context.getConfiguration();

      int k = context.getConfiguration().getInt("n", 100);
      ssource = conf.get("SOURCE_FIELD").split(",");

       numSource = ssource.length;

       sourcelist = new int[numSource];


        for (int i =0; i < numSource; i++){
            TopScoredObjects<Integer> q = new TopScoredObjects<Integer>(k);
            queue.add( q );
            sourcelist[i] = Integer.parseInt(ssource[i]);
          }
    }

    @Override
    public void reduce(IntWritable nid, Iterable<PairOfFloatInt> iterable, Context context)
        throws IOException {
      Iterator<PairOfFloatInt> iter = iterable.iterator();

      //PairOfFloatInt[] temp = new PairOfFloatInt[numSource];
      float[] temp = new float[numSource];
      int[] temp1 = new int[numSource];      

      int wow = 0;
       while (iter.hasNext()){
        PairOfFloatInt a = iter.next();
        temp[wow] = a.getLeftElement();
        temp1[wow] = a.getRightElement();       
          wow++;

      }

      for (int i = 0; i < wow; i++){
        queue.get( temp1[i] ).add(nid.get(), temp[i] );
        
      }
  
      // Shouldn't happen. Throw an exception.
      if (iter.hasNext()) {
        throw new RuntimeException();
      }
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
      IntWritable value = new IntWritable();
     
      for (int i = 0; i < numSource; i++){
      System.out.println("Source: " + Integer.parseInt(ssource[i]) );
      output.set("Source:");
      value.set(Integer.parseInt(ssource[i]) );
      context.write(output, value);
      

      for (PairOfObjectFloat<Integer> pair : queue.get(i).extractAll()) {
        int temp = pair.getLeftElement();   //left is nid  right is prob
        //output.set(Integer.toString(pair.getLeftElement()) );
        String ss = String.format("%.5f", (float)StrictMath.exp(pair.getRightElement()) );
        output.set( ss);
        //value.set((float)StrictMath.exp(pair.getRightElement()));
        value.set( (int)pair.getLeftElement());
        context.write(output, value);
        System.out.println(String.format("%.5f %d", (float)StrictMath.exp(pair.getRightElement()), temp));
      }
      output.set(" ");
      context.write(output, null);
      System.out.println(" " );
    }
    }
  }

  public ExtractTopPersonalizedPageRankNodes() {
  }

  private static final String INPUT = "input";
  private static final String OUTPUT = "output";
  private static final String TOP = "top";
  private static final String SOURCE = "sources";

  /**
   * Runs this tool.
   */
  @SuppressWarnings({ "static-access" })
  public int run(String[] args) throws Exception {
    Options options = new Options();

    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("input path").create(INPUT));
    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("output path").create(OUTPUT));
    options.addOption(OptionBuilder.withArgName("num").hasArg()
        .withDescription("top n").create(TOP));
    options.addOption(OptionBuilder.withArgName("src").hasArg()
        .withDescription("source array").create(SOURCE));

    CommandLine cmdline;
    CommandLineParser parser = new GnuParser();

    try {
      cmdline = parser.parse(options, args);
    } catch (ParseException exp) {
      System.err.println("Error parsing command line: " + exp.getMessage());
      return -1;
    }

    if (!cmdline.hasOption(INPUT) || !cmdline.hasOption(OUTPUT) || !cmdline.hasOption(TOP)) {
      System.out.println("args: " + Arrays.toString(args));
      HelpFormatter formatter = new HelpFormatter();
      formatter.setWidth(120);
      formatter.printHelp(this.getClass().getName(), options);
      ToolRunner.printGenericCommandUsage(System.out);
      return -1;
    }

    String inputPath = cmdline.getOptionValue(INPUT);
    String outputPath = cmdline.getOptionValue(OUTPUT);
    int n = Integer.parseInt(cmdline.getOptionValue(TOP));
    String ssource = cmdline.getOptionValue(SOURCE);


    LOG.info("Tool name: " + ExtractTopPersonalizedPageRankNodes.class.getSimpleName());
    LOG.info(" - input: " + inputPath);
    LOG.info(" - output: " + outputPath);
    LOG.info(" - top: " + n);
    LOG.info(" - sources: " + ssource);


    Configuration conf = getConf();
    conf.setInt("mapred.min.split.size", 1024 * 1024 * 1024);
    conf.setInt("n", n);
    conf.set("SOURCE_FIELD", ssource);


    Job job = Job.getInstance(conf);
    job.setJobName(ExtractTopPersonalizedPageRankNodes.class.getName() + ":" + inputPath);
    job.setJarByClass(ExtractTopPersonalizedPageRankNodes.class);

    job.setNumReduceTasks(1);

    FileInputFormat.addInputPath(job, new Path(inputPath));
    FileOutputFormat.setOutputPath(job, new Path(outputPath));

    job.setInputFormatClass(SequenceFileInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);

    job.setMapOutputKeyClass(IntWritable.class);
    job.setMapOutputValueClass(PairOfFloatInt.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);

    job.setMapperClass(MyMapper.class);
    job.setReducerClass(MyReducer.class);

    // Delete the output directory if it exists already.
    FileSystem.get(conf).delete(new Path(outputPath), true);

    job.waitForCompletion(true);

    return 0;
  }

  /**
   * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
   */
  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new ExtractTopPersonalizedPageRankNodes(), args);
    System.exit(res);
  }
}
