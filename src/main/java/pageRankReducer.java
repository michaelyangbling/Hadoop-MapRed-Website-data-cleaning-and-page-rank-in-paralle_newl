/*
class reducer{
  dangleSum=0
  reduce( ){
  sum=0
  if dummy key:  // must appear on first iter
    loop:
     dangleSum+=somePageRank
  else:
    loop:
      if adjList: recover
      else: sum+=someValue  // (pageRank/lenAdjacentList)
    finalRank=alpha/|V| + (1-alpha)*( sum+dangleSum/|V| )
    emit( page, adjList+pageRank)
  }
}

*/

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

public class pageRankReducer extends Reducer<isPageName,customValue,NullWritable,Text>{
    private double dangleSum=0;
    private long numNodes;
    protected void setup(Context context) throws IOException,
            InterruptedException {
        Configuration conf = context.getConfiguration();
        numNodes=conf.getLong("numNodes",-1);
    }
    public void reduce(isPageName key, Iterable<customValue> iterable,
                       Context context) throws IOException, InterruptedException {
        if(key.isPage==0) {
            for (customValue val : iterable) {
                dangleSum += Double.parseDouble(val.content);
            }
        }
        else{
            double sum=0; String output=""; double previousRank=0;
            for (customValue val : iterable) {
                if(val.kind==0){
                    String[] contents=val.content.split("~~");
                    output=contents[0];
                    previousRank=Double.parseDouble(contents[contents.length-1]);
                    for(int i=1;i<=contents.length-2;i++){
                        output=output+"~~"+contents[i];
                    }
                }
                else{
                    sum+=Double.parseDouble(val.content);
                }
            }
            //finalRank=alpha/|V| + (1-alpha)*( sum+dangleSum/|V| )
            String pageRank=Double.toString(0.15/numNodes+0.85*(sum+dangleSum/numNodes));
            context.write(NullWritable.get(), new Text(output+"~~"+pageRank));
            if (Math.random() < 0.002){
            System.out.println(output+"~~"+pageRank
        +"delta  "  +Double.toString(Double.parseDouble(pageRank)-previousRank)+"   ");}

        }
    }

}


