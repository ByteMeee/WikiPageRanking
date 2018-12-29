package hadoop.pageranking;

import hadoop.pageranking.job1.xmlparse.WikiLinksMapper;
import hadoop.pageranking.job1.xmlparse.XmlInputFormat;
import hadoop.pageranking.job2.calculate.RankCalculatorMapper;
import hadoop.pageranking.job2.calculate.RankCalculatorReducer;
import hadoop.pageranking.job3.result.RankingMapper;
import hadoop.pageranking.job1.xmlparse.WikiLinksReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.text.DecimalFormat;
import java.text.NumberFormat;

public class WikiPageRankingDriver extends Configured implements Tool {

    /**
     * Note: The pagerank will get more accurate, as the pagerank calculation MapReduce job
     * will run multiple times, so you should preserve each page's links record
     */

    private static NumberFormat nf = new DecimalFormat("00");

    public static void main (String[]args) throws Exception {
        if(args.length != 2){
            System.out.println("Usage: [input path] [output path]");
            System.exit(-1);
        }
        System.exit(ToolRunner.run(new Configuration(),new WikiPageRankingDriver(),args));
    }

    public int run(String[] strings) throws Exception {

        String tmpOutputFolder = "tmp_output";
        String job1Output = tmpOutputFolder + "wiki/ranking/iter00";

        String lastResultPath = null;

        boolean isCompleted = this.xmlParsing(strings[0],job1Output);
        if (!isCompleted) return 1;


        for (int runs = 0; runs < 5; runs++){

            String inPath = tmpOutputFolder + "wiki/ranking/iter" + nf.format(runs);
            lastResultPath = tmpOutputFolder + "wiki/ranking/iter" + nf.format(runs + 1);

            isCompleted = rankCalculation(inPath,lastResultPath);
            if(!isCompleted) return 1;
        }



        isCompleted = rankOrdering(lastResultPath,strings[0]);
        if (!isCompleted) return 1;

        return 0;
    }


    private boolean xmlParsing(String inputPath,String outputPath)throws IOException,InterruptedException,ClassNotFoundException{
        Configuration configuration = new Configuration();
        configuration.set(XmlInputFormat.START_TAG_KEY, "<page>");
        configuration.set(XmlInputFormat.END_TAG_KEY, "</page>");

        FileSystem fs = FileSystem.newInstance(configuration);
        Path inputFilePath = new Path(inputPath);
        Path outputFilePath = new Path(outputPath);

        if(!fs.exists(inputFilePath)){
            throw new IOException("The system cannot find the specified input file");
        }

        Job xmlParser = Job.getInstance(configuration,"xmlParser");
        xmlParser.setJarByClass(WikiPageRankingDriver.class);


        //Mapper
        FileInputFormat.addInputPath(xmlParser,inputFilePath);
        xmlParser.setInputFormatClass(XmlInputFormat.class);
        xmlParser.setMapperClass(WikiLinksMapper.class);

        xmlParser.setMapOutputKeyClass(Text.class);
        xmlParser.setMapOutputValueClass(Text.class);

        //Reducer
        FileOutputFormat.setOutputPath(xmlParser,outputFilePath);
        xmlParser.setOutputFormatClass(TextOutputFormat.class);

        xmlParser.setOutputKeyClass(Text.class);
        xmlParser.setOutputValueClass(Text.class);
        xmlParser.setReducerClass(WikiLinksReducer.class);

        // Deletes the output file if it exists
        if (fs.exists(outputFilePath)){
            fs.delete(outputFilePath,true);
        }

        return xmlParser.waitForCompletion(true);
    }

    private boolean rankCalculation(String inputPath, String outputPath)throws IOException, InterruptedException, ClassNotFoundException{
        Configuration configuration = new Configuration();
        Job rankCalculator = Job.getInstance(configuration,"rankCalculator");
        rankCalculator.setJarByClass(WikiPageRankingDriver.class);

        Path inputFilePath = new Path(inputPath);
        Path outputFilePath = new Path(outputPath);

        rankCalculator.setOutputKeyClass(Text.class);
        rankCalculator.setOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(rankCalculator, inputFilePath);
        FileOutputFormat.setOutputPath(rankCalculator, outputFilePath);

        rankCalculator.setMapperClass(RankCalculatorMapper.class);
        rankCalculator.setReducerClass(RankCalculatorReducer.class);

        FileSystem fs = FileSystem.newInstance(configuration);

        // Deletes the output file if it exists
        if (fs.exists(outputFilePath)){
            fs.delete(outputFilePath,true);
        }

        return  rankCalculator.waitForCompletion(true);
    }

    private boolean rankOrdering(String inputPath, String outputPath)throws IOException, InterruptedException, ClassNotFoundException{
        Configuration configuration = new Configuration();
        Job rankOrdering = Job.getInstance(configuration,"rankOrdering");
        rankOrdering.setJarByClass(WikiPageRankingDriver.class);

        Path inputFilePath = new Path(inputPath);
        Path outputFilePath = new Path(outputPath);

        rankOrdering.setOutputKeyClass(FloatWritable.class);
        rankOrdering.setOutputValueClass(Text.class);

        rankOrdering.setMapperClass(RankingMapper.class);

        FileInputFormat.setInputPaths(rankOrdering,inputFilePath);
        FileOutputFormat.setOutputPath(rankOrdering,outputFilePath);

        FileSystem fs = FileSystem.newInstance(configuration);

        // Deletes the output file if it exists
        if (fs.exists(outputFilePath)){
            fs.delete(outputFilePath,true);
        }

        return rankOrdering.waitForCompletion(true);
    }

}
