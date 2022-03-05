// ----------------------------------------------------------------------
// start of the map class
import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

public class MapIt extends Mapper<Object, Text, Text, IntWritable>{
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
    // ------------------------------------------------------------------------
    // --- your code should start here
		Text my_key = new Text("Key");
		IntWritable my_value = new IntWritable(1);



		context.write(my_key,my_value);

    // --- your code should end here
    // ------------------------------------------------------------------------
    }
}
// end of the map class
// ----------------------------------------------------------------------
