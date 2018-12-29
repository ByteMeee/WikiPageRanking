package hadoop.pageranking.job3.result;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class RankingMapper extends Mapper<LongWritable, Text, FloatWritable, Text> {

    /**
     * Note : The output of the Mapper will be sorted by the its keys.
     *
     * @param key
     * @param value
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        // Gets the index of the end of page name and page rank
        int pageNameIndex = value.find("\t");
        int pageRankIndex = value.find("\t",pageNameIndex + 1);

        // Gets the page name
        String page = Text.decode(value.getBytes(),0,pageNameIndex);

        int end ;
        // Calculate the length of the rank
        if(pageRankIndex == -1){
            end = value.getLength() - (pageNameIndex + 1);
        } else {
            end = pageRankIndex - (pageNameIndex + 1);
        }

        // Gets the rank of the page
        String rank = Text.decode(value.getBytes(),pageNameIndex + 1,end);
        float rankFloat = Float.parseFloat(rank);

        FloatWritable pageRank = new FloatWritable(rankFloat);
        Text pageName = new Text(page);

        // Output [rank] [page]
        context.write(pageRank,pageName);
    }
}
