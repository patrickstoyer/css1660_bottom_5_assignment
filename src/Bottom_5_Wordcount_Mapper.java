import java.io.*;
import java.util.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
  
public class Bottom_5_Wordcount_Mapper  extends Mapper<Object,
                            Text, Text, LongWritable> {
  
   // private TreeMap<Long, String> tmap;
    private HashMap<String, Long> hmap;
  
    @Override
    public void setup(Context context) throws IOException,
                                     InterruptedException
    {
    		hmap = new HashMap<String, Long>();
    }
  
    @Override
    public void map(Object key, Text value,
       Context context) throws IOException, 
                      InterruptedException
    {
  
        // input data format => movie_name    
        // no_of_views  (tab seperated)
        // we split the input data
        String[] tokens = value.toString().split(" ");
        
        for (String token : tokens) {
        	if (hmap.putIfAbsent(token, (long)1) != null) {
        		hmap.put(token, hmap.get(token) + 1);
        	}
        }
   
    }
  
    @Override
    public void cleanup(Context context) throws IOException,
                                       InterruptedException
    {
    	
        for (Map.Entry<String, Long> entry : hmap.entrySet()) 
        {
  
            String word = entry.getKey();
            Long count = entry.getValue();
  
            context.write(new Text(word), new LongWritable(count));
        }
    }
}