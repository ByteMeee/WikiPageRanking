package hadoop.pageranking.job2.calculate;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class RankCalculatorMapper extends Mapper <LongWritable, Text, Text, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        // Gets the the page and the oage with it's rank
        int pageIndex = value.find("\t");
        int rankIndex = value.find("\t" , pageIndex+1);

        String page = Text.decode(value.getBytes(),0,pageIndex);
        String pageWithRank = Text.decode(value.getBytes(),0,rankIndex+1);

        //Skip pages with no links.
        if (rankIndex == -1) return;

        // Gets the linked pages and the total number of links
        String links = Text.decode(value.getBytes(),rankIndex+1, value.getLength()-(rankIndex+1));
        String[] otherPages = links.split(",");
        int totalNumberLinks = otherPages.length;

        // For each linked page write [the link] [the linked page + the rank + total number of links ]
        for (String link : otherPages){
            context.write(new Text(link),new Text(pageWithRank+totalNumberLinks));
        }

        //The original links of the page for the reduce output
        context.write(new Text(page), new Text("@"+ links));

    }
}
