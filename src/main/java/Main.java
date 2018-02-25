/* All Pseudocode:
step 1.data cleaning:
class mapper {  // webstite HTML parser
   map(line, text){  //when first element is 0 : dummy key
   emit( [1,pageName], linkName )  //not deal with repeating linkName per pageName here
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
     a SET data structure is sufficient for this requirement

     emit(pageName, linkNames) //e.x. ~~ separated
}

step 2: a map only job to allocate orignal equal pageRanks
class mapper {
   map(line, text){
     emit(page~~link...~~link~~pageRank)
   }
}

step 3: 10 jobs to compute pageRank
class mapper {
    map(line, text){
     emit([1,page], [adjList(0),link...~~link~~pageRank])// pass graph
     if not dangling node
       emit([1,link],[number(1), "pageRank/lenAdjacentList"]
     else
       emit([0,"0"], [dangle(2), "pageRank"]
   }
class partitioner{
   getPartition(
   if dummy(0)
     partioner to separate k reducers correspondingly
   else
     partition-by-Name)
class reducer{
  reduce( ){
  dangleSum=0;sum=0
  if dummy key:  // must appear on first iter
    loop:
     dangleSum+=somePageRank
  else:
    loop:
      if adjList: recover graph for this node
      else: sum+=someValue  // (pageRank/lenAdjacentList)
    finalRank=alpha/|V| + (1-alpha)*( sum+dangleSum/|V| )
    emit( null,page+ adjList+pageRank)
  }
}
}
step4:  get top-k pages in parallel
this top-k algo basically merges local top-k results from mappers to one reducer
here, I set k to 100, that is to give top 100 pages and their page ranks

map(){
emit(dummy, list of local top-k )
}
reduce(){ //only one reducer and only one reduce call
for top-k pages:
  emit(null, page-rank )
}
*/

import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Partitioner;

import java.io.IOException;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Counter;

public class Main {

    public static int compare(int a,int b){ //compare 2 numbers
        if(a<b){ return -1;}
        else if(a==b) {return 0;}
        else{return 1;}
    }
   public static class pageRankPartitioner extends Partitioner<isPageName, customValue>{
      //Partitioner for pageRank jobs
       public int getPartition(isPageName key, customValue value, int numPartitions) {
           if(key.isPage==1){
               return Math.abs(key.PageName.hashCode()) % numPartitions;
           }
           else
               {return Integer.parseInt(key.PageName);}//deal with dummy key
       }
   }

   public static class dataCleanPartitioner// Partitioner for data-cleaning job
            extends Partitioner<isPageName, Text> { //partition(hash) by station(string)
        public int getPartition(isPageName key, Text value, int numPartitions) {
            if(key.isPage==1){
                return Math.abs(key.PageName.hashCode()) % numPartitions;
            }
            else
                {return Integer.parseInt(key.PageName);}
        }
    }


    public static class dataCleanReducer
            extends Reducer<isPageName,Text,NullWritable,Text> {
        private Set<String> pageSet = new HashSet<String>();
        //private int dummyCounter=0;
        public void reduce(isPageName key, Iterable<Text> iterable,
                           Context context) throws IOException, InterruptedException {
            String currentLink;
            if(key.isPage==0){ //recover collection
                for(Text val:iterable){
                    pageSet.add(val.toString());
                }
                //dummyCounter+=1;
                //System.out.println("dummyCounter:"); //check design correctness
                //System.out.println(dummyCounter);
            }
            else {
                Set<String> linkSet = new HashSet<String>();
                String out;
                out=key.PageName;
                for (Text val : iterable) { //use ~~ to concat outputs
                    currentLink=val.toString();
                    if (!currentLink.equals("~~~")){    //if this is an existing link
                        if(!currentLink.equals(key.PageName)) {// if not the pageName
                            if (pageSet.contains(currentLink)) {//delete pointed links  not in the collection
                                linkSet.add(currentLink);
                            }
                        }
                    }

                }
                for (String link:linkSet){ //get adjacent list
                    out=out+"~~"+link;
                }
                context.write( NullWritable.get(), new Text(out));
                context.getCounter(myCounter.NUM_PAGES).increment(1);
                //counter to calculate total page(node) number
            }

        }
    }
    //
    public static void main(String[] args) throws Exception {
        //data cleaning
        Configuration conf = new Configuration();
        conf.set("numSetReducers", args[2]);//set and pass numReducers
        Job job = Job.getInstance(conf, "myjob");
        job.setNumReduceTasks(Integer.parseInt(args[2]));
        job.setJarByClass(Main.class);
        job.setMapperClass(webPaserMapper.class);
        job.setReducerClass(dataCleanReducer.class);
        job.setPartitionerClass(dataCleanPartitioner.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setMapOutputKeyClass(isPageName.class);
        job.setMapOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]+"/input"));
        FileOutputFormat.setOutputPath(job, new Path(args[0]+"/output"));
        boolean status=job.waitForCompletion(true);
        Counters counters = job.getCounters();
        Counter c1=counters.findCounter(myCounter.NUM_PAGES);
        System.out.println(c1.getValue());

        //map only to give initial page rank
        conf.setLong("numNodes",c1.getValue());
        Job jobInitialRank= Job.getInstance(conf, "jobInitialRank");
        jobInitialRank.setJarByClass(Main.class);
        jobInitialRank.setMapperClass(initialRankMapper.class);
        jobInitialRank.setOutputKeyClass(NullWritable.class);
        jobInitialRank.setOutputValueClass(Text.class);
        jobInitialRank.setMapOutputKeyClass(NullWritable.class);
        jobInitialRank.setMapOutputValueClass(Text.class);
        FileInputFormat.addInputPath(jobInitialRank, new Path(args[0]+"/output"));
        FileOutputFormat.setOutputPath(jobInitialRank,
                new Path(args[0]+"/output0"));
        boolean status2=jobInitialRank.waitForCompletion(true);


        //iterate pageRank Algo for 10 times
        for(int i=1;i<=10;i++) {
            Job jobPageRank = Job.getInstance(conf,
                    "jobPageRank"+Integer.toString(i));
            jobPageRank.setNumReduceTasks(Integer.parseInt(args[2]));
            jobPageRank.setJarByClass(Main.class);
            jobPageRank.setMapperClass(pageRankMapper.class);
            jobPageRank.setReducerClass(pageRankReducer.class);
            jobPageRank.setPartitionerClass(pageRankPartitioner.class);
            jobPageRank.setOutputKeyClass(NullWritable.class);
            jobPageRank.setOutputValueClass(Text.class);
            jobPageRank.setMapOutputKeyClass(isPageName.class);
            jobPageRank.setMapOutputValueClass(customValue.class);
            FileInputFormat.addInputPath(jobPageRank, new Path(args[0] + "/output"
            +Integer.toString(i-1)));
            FileOutputFormat.setOutputPath(jobPageRank, new Path(args[0] + "/output"
            +Integer.toString(i)));
            jobPageRank.waitForCompletion(true);
        }

        //parallel top-k algo
        Job jobPageRank = Job.getInstance(conf, "jobTopK");
        jobPageRank.setNumReduceTasks(1);//only need 1 reducer in parallel top-k algo
        jobPageRank.setJarByClass(Main.class);
        jobPageRank.setMapperClass(Top_k.topKpagesMapper.class);
        jobPageRank.setReducerClass(Top_k.topKpagesReducer.class);
        jobPageRank.setOutputKeyClass(NullWritable.class);
        jobPageRank.setOutputValueClass(Text.class);
        jobPageRank.setMapOutputKeyClass(NullWritable.class);
        jobPageRank.setMapOutputValueClass(Top_k.pageRank.class);
        FileInputFormat.addInputPath(jobPageRank, new Path(args[0]+"/output10"));
        FileOutputFormat.setOutputPath(jobPageRank, new Path(args[0]+"/top100_Pages"));
        jobPageRank.waitForCompletion(true);
        //System.exit(status? 0 : 1);

    }
}