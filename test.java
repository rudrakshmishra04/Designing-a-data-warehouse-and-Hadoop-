	// ----------------------------------------------------------------------
// start of the map class
import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import java.text.SimpleDateFormat;
import java.util.Date;

public class MapIt extends Mapper<Object, Text, Text, IntWritable>{
    public void map(Object key, Text value, Context context) throws IOException, 
InterruptedException {
    // ------------------------------------------------------------------------
    // --- your code should start here
	String my_line = value.toString();
        if(my_line.trim().length()==0){
            return;
        }
		
		String[] words = my_line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1);
        if(words.length!=21){
            return;
        }
        System.out.println(words.length);
        String borough = words[0].trim();
        String neighborhood = words[1].trim();
		String building_class_category = words[2].trim();
		String tax_class_present = words[3].trim();
		String building_class_present = words[7].trim();
        String address = words[8].trim().replace(',',' ');
		String zipcode = words[10].trim();
		String land_sq_ft = words[14].trim().replace(',',' ');
		String gross_sq_ft = words[15].trim().replace(',',' ');
		String year_built = words[16].trim();
		String tax_class = words[17].trim(); //tax class at time of sale
        String building_class = words[18].trim(); //building_class at time of sale  
        String sale_price = words[19].trim();
		String sale_date = words[20].trim();


    

      //  try {
       //     Double.parseDouble(words[6]);
       // } catch (NumberFormatException e) {
            //return;
       // }
        //double sale_price = Double.parseDouble(words[6]);
		
        //if(province.length()==0){
          //  return;}

        SimpleDateFormat dateFormatInput = new SimpleDateFormat("MM/d/yyyy");
        SimpleDateFormat dateFormatOutput = new SimpleDateFormat("yyyy-MM-dd");
        try {
            Date orderdate = dateFormatInput.parse(sale_date.trim());
// if orderdate is valid format it for the Hive table
            sale_date = dateFormatOutput.format(orderdate);
        } catch (Exception e) { // data is notin the proper format
            return; // skip this line
        }

        String my_str_key = borough+","+neighborhood+","+building_class_category+","+tax_class_present+","+building_class_present+","+address+","+zipcode+","+land_sq_ft+","+gross_sq_ft+","+year_built+","+tax_class+","+building_class+","+sale_price+","+sale_date;
        int my_int_val = 1;

        System.out.println(borough+","+neighborhood+","+building_class_category+","+tax_class_present+","+building_class_present+","+address+","+zipcode+","+land_sq_ft+","+gross_sq_ft+","+year_built+","+tax_class+","+building_class+","+sale_price+","+sale_date);
		
	Text my_key = new Text(my_str_key);
	IntWritable my_value = new IntWritable(1);
	context.write(my_key,my_value);
    // --- your code should end here
    // ------------------------------------------------------------------------
    }
}
// end of the map class
// ----------------------------------------------------------------------
	
	
	
	