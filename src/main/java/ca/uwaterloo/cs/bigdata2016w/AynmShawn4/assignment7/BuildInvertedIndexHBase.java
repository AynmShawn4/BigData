package ca.uwaterloo.cs.bigdata2016w.AynmShawn4.assignment7;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.VIntWritable;

import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MapFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.ParserProperties;

import tl.lin.data.array.ArrayListWritable;
import tl.lin.data.fd.Object2IntFrequencyDistribution;
import tl.lin.data.fd.Object2IntFrequencyDistributionEntry;
import tl.lin.data.pair.PairOfInts;
import tl.lin.data.pair.PairOfObjectInt;
import tl.lin.data.pair.PairOfWritables;
import tl.lin.data.pair.PairOfStringInt;
import tl.lin.data.pair.PairOfLongInt;


public class BuildInvertedIndexCompressed extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(BuildInvertedIndexCompressed.class);

  private static class MyMapper extends Mapper<LongWritable, Text, PairOfStringInt, VIntWritable> {
    private static final Text WORD = new Text();
    private static final VIntWritable VINT = new VIntWritable();
    private static final PairOfStringInt PSL = new PairOfStringInt();

    private static final Object2IntFrequencyDistribution<String> COUNTS =
        new Object2IntFrequencyDistributionEntry<String>();

    @Override
    public void map(LongWritable docno, Text doc, Context context)
        throws IOException, InterruptedException {
      String text = doc.toString();

      // Tokenize line.
      List<String> tokens = new ArrayList<String>();
      StringTokenizer itr = new StringTokenizer(text);
      while (itr.hasMoreTokens()) {
        String w = itr.nextToken().toLowerCase().replaceAll("(^[^a-z]+|[^a-z]+$)", "");
        if (w.length() == 0) continue;
        tokens.add(w);
      }

      // Build a histogram of the terms.
      COUNTS.clear();
      for (String token : tokens) {
        COUNTS.increment(token);
      }

      // Emit postings.
      for (PairOfObjectInt<String> e : COUNTS) {
        VINT.set(e.getRightElement() );
        PSL.set(e.getLeftElement(), (int)docno.get());
        context.write(PSL, VINT );
      }
    }
  }

  private static class MyReducer extends
      Reducer<PairOfStringInt, VIntWritable, Text, BytesWritable > {

        //initialize 

        private static Text PREVIOUSTERM ; 
        private static ArrayListWritable<PairOfInts> POSTING ;
        private static IntWritable previousDocnum;
        private static BytesWritable bwritable ;
        private static ByteArrayOutputStream bytearray ;
        private static DataOutputStream data ;

    @Override
    public void setup(Context context)
      throws IOException, InterruptedException {
        POSTING = new ArrayListWritable<PairOfInts>();
        previousDocnum = new IntWritable();
        previousDocnum.set(0);
        PREVIOUSTERM = new Text(""); 
        bwritable = new BytesWritable();
        bytearray = new ByteArrayOutputStream();
        data = new DataOutputStream(bytearray);

    }

    @Override
    public void reduce(PairOfStringInt key, Iterable<VIntWritable> values, Context context)
        throws IOException, InterruptedException {

      String term = key.getLeftElement();


      if ((!term.equals(PREVIOUSTERM.toString())) && (PREVIOUSTERM.getLength() != 0 )){

        for (PairOfInts pair : POSTING){
          WritableUtils.writeVInt(data, pair.getLeftElement() ); //left doc num
          WritableUtils.writeVInt(data, pair.getRightElement()); //right term frequency
        }

        bwritable.set(bytearray.toByteArray(), 0, bytearray.size());

        context.write(PREVIOUSTERM, bwritable);

        POSTING.clear();
        bwritable.setSize(0);
        bytearray.reset();
        data.flush();
        previousDocnum.set(0);
      }
      
      Iterator<VIntWritable> iter = values.iterator();
      //reduce   append
      int df = 0;
      while (iter.hasNext()) {
        df = df + iter.next().get();
      }
      POSTING.add(new PairOfInts(key.getRightElement() - (int)previousDocnum.get() , df));

      PREVIOUSTERM.set(key.getLeftElement());
      previousDocnum.set(key.getRightElement());

     // DF.set(df);
   //   context.write(key,
   //       new PairOfWritables<IntWritable, ArrayListWritable<PairOfInts>>(DF, postings));
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException{

        for (PairOfInts pair : POSTING){
          WritableUtils.writeVInt(data, pair.getLeftElement()  ); //left doc num
          WritableUtils.writeVInt(data, pair.getRightElement()); //right term frequency

        }

        bwritable.set(bytearray.toByteArray(), 0, bytearray.size());
        int aa = (int)previousDocnum.get();
        if (aa != 0 ){
          context.write(PREVIOUSTERM, bwritable);
        }

        POSTING.clear();
        bytearray.reset();
        data.flush();
        bwritable.setSize(0);
        PREVIOUSTERM.set("");

    }
  }

  protected static class MyPartitioner extends Partitioner<PairOfStringInt, VIntWritable> {
    @Override
    public int getPartition(PairOfStringInt key, VIntWritable value, int numReduceTasks) {
      return (key.getLeftElement().toString().hashCode() & Integer.MAX_VALUE) % numReduceTasks;
    }
  }


  private BuildInvertedIndexCompressed() {}

  public static class Args {
    @Option(name = "-input", metaVar = "[path]", required = true, usage = "input path")
    public String input;

    @Option(name = "-output", metaVar = "[path]", required = true, usage = "output path")
    public String output;

    @Option(name = "-reducers", metaVar = "[num]", required = false, usage = "number of reducers")
    public int numReducers = 1;
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

    LOG.info("Tool: " + BuildInvertedIndexCompressed.class.getSimpleName());
    LOG.info(" - input path: " + args.input);
    LOG.info(" - output path: " + args.output);
    LOG.info(" - num reducers: " + args.numReducers);


    Job job = Job.getInstance(getConf());
    job.setJobName(BuildInvertedIndexCompressed.class.getSimpleName());
    job.setJarByClass(BuildInvertedIndexCompressed.class);

    job.setNumReduceTasks(args.numReducers);

    FileInputFormat.setInputPaths(job, new Path(args.input));
    FileOutputFormat.setOutputPath(job, new Path(args.output));

    job.setMapOutputKeyClass(PairOfStringInt.class);
    job.setMapOutputValueClass(VIntWritable.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(BytesWritable.class);
    job.setOutputFormatClass(MapFileOutputFormat.class);

    job.setMapperClass(MyMapper.class);
    job.setReducerClass(MyReducer.class);
    job.setPartitionerClass(MyPartitioner.class);

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
    ToolRunner.run(new BuildInvertedIndexCompressed(), args);
  }
}
