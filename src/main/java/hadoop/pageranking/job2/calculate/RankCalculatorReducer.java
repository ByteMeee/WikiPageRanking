package hadoop.pageranking.job2.calculate;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


public class RankCalculatorReducer extends Reducer <Text, Text, Text, Text>{

    private static final float DAMPINGFACTOR = 0.85F;

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        String[] parts;
        String links = "";
        String pageWithRankAndLinks;
        float sumDistributionLinks = 0;

        //Loop through the values (rank + links)
        for(Text value : values){
            pageWithRankAndLinks = value.toString();

            // Save the links record
            if(pageWithRankAndLinks.startsWith("@")){
                links = "\t"+pageWithRankAndLinks.substring(1);
                continue;
            }

            // If it is a normal record find the pagerank and the number of links for the page
            parts = pageWithRankAndLinks.split("\\t");

            float pageRank = Float.valueOf(parts[1]);
            int countOutLinks = Integer.valueOf(parts[2]);

            sumDistributionLinks += (pageRank/countOutLinks);
        }

        // Calculate pagerank
        float newRank = DAMPINGFACTOR * sumDistributionLinks + (1-DAMPINGFACTOR);

        // Add new pagerank
        context.write(key, new Text(newRank + links));

    }
}
