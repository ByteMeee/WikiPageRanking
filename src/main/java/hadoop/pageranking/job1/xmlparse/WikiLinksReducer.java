package hadoop.pageranking.job1.xmlparse;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


public class WikiLinksReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        String valueOut = "1.0\t";
        boolean first = true;

        for(Text text: values){

            if(!first) valueOut += ",";

            valueOut += text.toString();
            first = false;
        }

        context.write(key,new Text(valueOut));
    }
}
