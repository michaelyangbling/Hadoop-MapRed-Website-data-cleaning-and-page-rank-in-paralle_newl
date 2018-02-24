/*PseudoCode:
class mapper {
    map(line, text){
        emit([1,page], [adjList(0),page~~link...~~link~~pageRank])// pass graph
        if not dangling node
          for all links:
            emit([1,link],[number(1), "pageRank/lenAdjacentList"]
        else
          for 0,1,...j...,k-1 ( k: number of reducers )
            emit([0,"j"], [dangle(2), "pageRank"]
    }*/
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
public class pageRankMapper
        extends Mapper<Object, Text, isPageName, customValue> {
    private int numSetReducers;
    protected void setup(Context context) throws IOException,
            InterruptedException {
        Configuration conf = context.getConfiguration();
        String a=conf.get("numSetReducers");
        numSetReducers=Integer.parseInt(conf.get("numSetReducers"));
        // and then you can use it
    }
    public void map(Object key, Text value, Context context
    ) throws IOException, InterruptedException {
        if (!value.toString().equals("")){
            String wholeString = value.toString();
        String[] contents = wholeString.split("~~");
        context.write(new isPageName(1, contents[0]),
                new customValue(0, wholeString));
        if (contents.length > 2) {
            Double outRank = Double.parseDouble(contents[contents.length - 1]) / (contents.length - 2);
            for (int i = 1; i <= contents.length - 2; i++) {
                context.write(new isPageName(1, contents[i]),
                        new customValue(1, Double.toString(outRank)));
            }
        } else {//emit([0,"j"], [dangle(2), "pageRank"]
            for (int j = 0; j < numSetReducers; j++) {
                context.write(new isPageName(0, Integer.toString(j)),
                        new customValue(2, contents[contents.length - 1]));
            }
        }
      }
    }
}


