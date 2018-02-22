/*2: a map only job to allocate orignal equal pageRanks
class mapper {
    map(line, text){
        emit(page~~link...~~link~~pageRank)
    }
}*/
//define the mapper for a map only job to allocate orignal equal pageRanks
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


public class initialRankMapper
            extends Mapper<Object, Text, NullWritable, Text> {
    private String numNodes;
    protected void setup(Context context) throws IOException,
            InterruptedException {
        Configuration conf = context.getConfiguration();
        numNodes=conf.get("numNodes");
    }
    public void map(Object key, Text value, Context context
    ) throws IOException, InterruptedException {
        String adjList=value.toString();
        context.write(NullWritable.get(),
                new Text(adjList+"~~"+numNodes));
    }
}

