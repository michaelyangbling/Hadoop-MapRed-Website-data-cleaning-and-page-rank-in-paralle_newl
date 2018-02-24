/*customized value for pageRank jobs:
class mapper {
    map(line, text){
        emit([1,page], [adjList(0),link...~~link~~pageRank])// pass graph
        if not dangling node
          emit([1,link],[number(1), "pageRank/lenAdjacentList"]
        else
          emit([0,"0"], [dangle(2), "pageRank"]
    }*/


import org.apache.hadoop.io.*;
import java.io.*;

public class customValue implements Writable {
    int kind;
    String content;

    public customValue() {

    }
    public customValue(int kind, String content) {
      this.kind=kind;
      this.content=content;
    }

    public void write(DataOutput out) throws IOException {
        out.writeInt(kind);
        WritableUtils.writeString(out, content);
    }

    public void readFields(DataInput in) throws IOException {
        kind = in.readInt();
        content = WritableUtils.readString(in);

    }
}
