package hadoop.pageranking.job1.xmlparse;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.nio.charset.CharacterCodingException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class WikiLinksMapper extends Mapper<LongWritable, Text, Text, Text> {

    private static final Pattern wikiLinkPattern = Pattern.compile("\\[.+?\\]");

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        // Gets the title and the text of the page
        String[] titleAndText = parseTitleAndText(value);

        if (notValidPage(titleAndText[0]))
            return;

        Text page = new Text(titleAndText[0].replace(' ','_'));

        // Gets the links
        Matcher matcher = wikiLinkPattern.matcher(titleAndText[1]);


        //Loop through the matched links
        while(matcher.find()){
            String otherPage = matcher.group();

            otherPage = getWikiPageFromLink(otherPage);

            if(otherPage == null || otherPage.isEmpty()){
                continue;
            }

            // Add valide linked page for the reduce
            context.write(page,new Text(otherPage));


        }



    }

    private String[] parseTitleAndText(Text value)throws CharacterCodingException{
        String [] titleAndText = new String[2];

        int start = value.find("<title>");
        int end = value.find("</title>");

        //Ignore page without title
        if (start == -1 || end == -1){
            return new String[]{"",""};
        }
        // Adding the tag length
        start += 7;
        titleAndText[0]=Text.decode(value.getBytes(),start,end-start);

        start = value.find("<text");
        start = value.find(">",start);
        end = value.find("</text>",start);

        //Ignore page without text
        if(start == -1 || end == -1){
            return new String[]{"",""};
        }

        start += 1;
        titleAndText[1] = Text.decode(value.getBytes(),start,end-start);

        return titleAndText;
    }

    private boolean notValidPage(String pageTitle){
        //For titles with a namespace
        return  pageTitle.contains(":");
    }

    private String getWikiPageFromLink(String link){
        if(notValidPage(link)) return null;

        int start = link.startsWith("[[") ? 2 : 1;
        int end = link.indexOf("]");

        int pipePosition = link.indexOf("|");
        if(pipePosition > 0){
            end = pipePosition;
        }

        int theme = link.indexOf("#");
        if (theme > 0){
            end = theme;
        }

        link = link.substring(start,end);
        link = link.replaceAll("\\s","_");
        link = link.replaceAll(",","");

        return link;

    }


    private boolean notWikiLink(String link){
        int start = link.startsWith("[[") ? 2 : 1;
        int maxLength = 100;

        if (link.length() < start+2 || link.length() > maxLength ) return true;
        char firstChar = link.charAt(start);

        if(firstChar == '#') return true;
        if(firstChar == '.') return true;
        if(firstChar == '\'') return true;
        if(firstChar == '-') return true;
        if(firstChar == '{') return true;
        if(firstChar == '&') return true;


        return ( link.contains(":") || link.contains(",") || !(link.substring(link.indexOf('&')).startsWith("&amp;")));

    }
}
