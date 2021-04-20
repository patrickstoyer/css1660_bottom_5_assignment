
import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;
  
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
  
public class Bottom_5_Wordcount_Reducer extends Reducer<Text,
                     LongWritable, LongWritable, Text> {
  
    private TreeMap<Long, String> tmap;
  
    @Override
    public void setup(Context context) throws IOException,
                                     InterruptedException
    {
        tmap = new TreeMap<Long, String>();
    }
  
    @Override
    public void reduce(Text key, Iterable<LongWritable> values,
      Context context) throws IOException, InterruptedException
    {
  
        // input data from mapper
        // key                values
        // word         		[ count ]
        String word = key.toString();
        long count = 0;
  
        for (LongWritable val : values)
        {
            count += val.get();
        }
  
        // insert data into treeMap,
        // we want bottom 5 word counts
        // so we pass count as key
        tmap.put(count, word);
  
        // we remove the last key-value
        // if it's size increases 10
        if (tmap.size() > 5)
        {
            tmap.remove(tmap.lastKey());
        }
    }
  
    @Override
    public void cleanup(Context context) throws IOException,
                                       InterruptedException
    {
  
        for (Map.Entry<Long, String> entry : tmap.entrySet()) 
        {
  
            long count = entry.getKey();
            String name = entry.getValue();
            context.write(new LongWritable(count), new Text(name));
        }
    }
}