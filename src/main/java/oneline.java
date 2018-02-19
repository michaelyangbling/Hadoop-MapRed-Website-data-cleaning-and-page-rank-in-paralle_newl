/* Pseudocode:
class mapper {
   map(line, text){  //when first element is 0 : dummy key
   emit( [1,pageName], linkName )  //deal with repeating linkName per pageName here
   emit ( [0,dummy 1], pageName)
   ...
   emit ( [0,dummy k], pageName ) //give collection of pageNames to all k reducers
   }
}
class partitioner{
   getPartition(
   if dummy
     partioner to separate k reducers correspondingly
   else
     partition-by-pageName)
}
class reducer{
//e.x. reducer k,  Iterable :([0,dummy k], pageNames ) ([1,pageName], linkNames)...
   reduce( ){
     first recover collection of pageNames from ([0,dummy k], pageNames )
     when adding linkNames to pageName, ignore those not in collection of pageNames,
     and don't add duplicates
     a set data structure is sufficient for this requirement

     emit(pageName, linkNames) //e.x. comma or ~ separated
}
*/

import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.*;
import java.io.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Partitioner;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.net.URLDecoder;
import java.util.LinkedList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.xml.sax.Attributes;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.DefaultHandler;


public class oneline {

    public static int compare(int a,int b){
        if(a<b){ return -1;}
        else if(a==b) {return 0;}
        else{return 1;}
    }
    public static class myMapper
            extends Mapper<Object, Text, isPageName, Text>{
        static private  Pattern namePattern;
        static private  Pattern linkPattern;
        static {
            // Keep only html pages not containing tilde (~).
            namePattern = Pattern.compile("^([^~]+)$");
            // Keep only html filenames ending relative paths and not containing tilde (~).
            linkPattern = Pattern.compile("^\\..*/([^~]+)\\.html$");
        }
        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            try {
                // Configure parser.
                SAXParserFactory spf = SAXParserFactory.newInstance();
                spf.setFeature("http://apache.org/xml/features/nonvalidating/load-external-dtd", false);
                SAXParser saxParser = spf.newSAXParser();
                XMLReader xmlReader = saxParser.getXMLReader();
                // Parser fills this list with linked page names.
                List<String> linkPageNames = new LinkedList<>();
                xmlReader.setContentHandler(new WikiParser(linkPageNames));


                String line=value.toString();
                for(int i=0;i<1;i++){  //exactly one loop since there is only one line
                    // Each line formatted as (Wiki-page-name:Wiki-page-html).
                    int delimLoc = line.indexOf(':');
                    String pageName = line.substring(0, delimLoc);
                    String html = line.substring(delimLoc + 1);
                    Matcher matcher = namePattern.matcher(pageName);
                    if (!matcher.find()) {
                        // Skip this html file, name contains (~).
                        continue;
                    }

                    // Parse page and fill list of linked pages.
                    linkPageNames.clear();
                    try {
                        xmlReader.parse(new InputSource(new StringReader(html)));
                    } catch (Exception e) {
                        for(String val:linkPageNames) {
                            for(int j=0;j<5/*numPartitioners*/;j++){
                                context.write(new isPageName(0,Integer.toString(j)), new Text(pageName));
                            }
                            context.write(new isPageName(1,pageName), new Text(""));
                        }
                        // do not Discard ill-formatted pages. emit instead
                        continue;
                    }

                    // always print the page and its links.
                    for(String val:linkPageNames) {
                        for(int j=0;j<5/*numPartitioners*/;j++){
                          context.write(new isPageName(0,Integer.toString(j)), new Text(pageName));
                        }
                        context.write(new isPageName(1,pageName), new Text(val));
                    }
                }

            } catch (Exception e) {
                e.printStackTrace();
            }

        }

        /** Parses a Wikipage, finding links inside bodyContent div element.*/
        private static class WikiParser extends DefaultHandler {
            /** List of linked pages; filled by parser. */
            private List<String> linkPageNames;
            /** Nesting depth inside bodyContent div element. */
            private int count = 0;

            public WikiParser(List<String> linkPageNames) {
                super();
                this.linkPageNames = linkPageNames;
            }

            @Override
            public void startElement(String uri, String localName, String qName, Attributes attributes) throws SAXException {
                super.startElement(uri, localName, qName, attributes);
                if ("div".equalsIgnoreCase(qName) && "bodyContent".equalsIgnoreCase(attributes.getValue("id")) && count == 0) {
                    // Beginning of bodyContent div element.
                    count = 1;
                } else if (count > 0 && "a".equalsIgnoreCase(qName)) {
                    // Anchor tag inside bodyContent div element.
                    count++;
                    String link = attributes.getValue("href");
                    if (link == null) {
                        return;
                    }
                    try {
                        // Decode escaped characters in URL.
                        link = URLDecoder.decode(link, "UTF-8");
                    } catch (Exception e) {
                        // Wiki-weirdness; use link as is.
                    }
                    // Keep only html filenames ending relative paths and not containing tilde (~).
                    Matcher matcher = linkPattern.matcher(link);
                    if (matcher.find()) {
                        linkPageNames.add(matcher.group(1));
                    }
                } else if (count > 0) {
                    // Other element inside bodyContent div.
                    count++;
                }
            }

            @Override
            public void endElement(String uri, String localName, String qName) throws SAXException {
                super.endElement(uri, localName, qName);
                if (count > 0) {
                    // End of element inside bodyContent div.
                    count--;
                }
            }
        }

    }

   /* public static class myPartitioner
            extends Partitioner<staYear, info> { //partition(hash) by station(string)
        public int getPartition(staYear key, info value, int numPartitions) {
            return Math.abs(key.station.hashCode()) % numPartitions;
        }
    }

    public static class myReducer
            extends Reducer<staYear,info,Text,Text> {
        public void reduce(staYear key, Iterable<info> iterable,
                           Context context
        ) throws IOException, InterruptedException {

        }
    }*/
    public static void main(String[] args) throws Exception {
     /*   Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "myjob");
        job.setJarByClass(sort2nd.class);

        job.setMapperClass(myMapper.class);
        job.setReducerClass(myReducer.class);
        job.setPartitionerClass(myPartitioner.class);
        job.setGroupingComparatorClass(groupComparator.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapOutputKeyClass(staYear.class);
        job.setMapOutputValueClass(info.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);*/
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Mapper_Only_Job");

        job.setJarByClass(oneline.class);
        job.setMapperClass(myMapper.class);
        job.setOutputKeyClass(isPageName.class);
        job.setOutputValueClass(Text.class);
        //job.setInputFormatClass(TextInputFormat.class);
        //job.setOutputFormatClass(TextOutputFormat.class);

        // Sets reducer tasks to 0
        job.setNumReduceTasks(0);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean result = job.waitForCompletion(true);

        System.exit(result ? 0 : 1);
    }




}

