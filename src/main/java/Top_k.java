import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.DataInput;
import java.io.DataOutput;
import java.util.ArrayList;
import java.util.List;
import java.util.Comparator;
import java.util.*;

import java.io.IOException;

public class Top_k {

    public static class pageRank{ // store page and its rank
        String page;
        double rank;
        pageRank(){}
        pageRank(String page, double rank){
            this.page=page;
            this.rank=rank;
        }
        public void write(DataOutput out) throws IOException {
            WritableUtils.writeString(out, page);
            out.writeDouble(rank);
        }
        public void readFields(DataInput in) throws IOException {
            page = WritableUtils.readString(in);
            rank = in.readDouble();

        }

    }
    public static class CustomComparator implements Comparator<pageRank> {
        public int compare(pageRank o1, pageRank o2) {
            //define comparator for sorting in descending order


            if(o1.rank<o2.rank)
            {return 1;}
            else if (o1.rank==o2.rank){
                return 0;
            }
            else
                {return -1;}
        }
    }
    public static class topKpagesMapper//website data parser
            extends Mapper<Object, Text, NullWritable, pageRank> {
        private List<pageRank> pageList=new ArrayList<pageRank>();
        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String[] contents=value.toString().split("~~");
            pageList.add( new pageRank(contents[0],
                    Double.parseDouble(contents[contents.length-1])) );

        }
        protected void cleanup(Context context)
                throws IOException, InterruptedException{
            Collections.sort(pageList,new CustomComparator());
            int howMany=0;
            for(pageRank val:pageList){
                if(howMany>100)
                    break;
                context.write(NullWritable.get(),val);
                howMany+=1;
            }
        }
    }
    public static class topKpagesReducer extends Reducer<NullWritable,pageRank,NullWritable,Text> {
        public void reduce(NullWritable key, Iterable<pageRank> values,
                           Context context) throws IOException, InterruptedException {
            List<pageRank> pageList=new ArrayList<pageRank>();
            for(pageRank val:values){
                pageList.add(new pageRank(val.page, val.rank));
            }
            Collections.sort(pageList,new CustomComparator());
            int howMany=0;
            for(pageRank val2:pageList){
                if(howMany>100)
                    break;
                context.write( NullWritable.get(),
                        new Text(val2.page+"   "+Double.toString(val2.rank)) );
                howMany+=1;
            }
        }
    }
}
