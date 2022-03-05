import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.util.Date;

public class test {

    public void process_file(String file_name) throws Exception{
        FileInputStream fstream = new FileInputStream(file_name);
        BufferedReader br = new BufferedReader(new InputStreamReader(fstream));

        String strLine;

//Read File Line By Line
        while ((strLine = br.readLine()) != null) {
            // Print the content on the console
            //System.out.println(strLine);
            map(strLine);
        }

//Close the input stream
        fstream.close();
    }

    public void map(String my_line) throws Exception{
        //System.out.println("Length of line is "+my_line.length());

        if(my_line.trim().length()==0){
            return;
        }

        String[] words = my_line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1);
        if(words.length!=16){
            return;
        }

        String province = words[10].trim();
        String date_time = words[1];

        try {
            Double.parseDouble(words[6]);
        } catch (NumberFormatException e) {
            return;
        }
        double sale_price = Double.parseDouble(words[6]);
        if(province.length()==0){
            return;
        }

        SimpleDateFormat dateFormatInput = new SimpleDateFormat("MM/d/yyyy HH:mm");
        SimpleDateFormat dateFormatOutput = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        try {
            Date orderdate = dateFormatInput.parse(date_time.trim());
            // if orderdate is valid format it for the Hive table
            date_time = dateFormatOutput.format(orderdate);
        } catch (Exception e) {  // data is notin the proper format
            return; // skip this line
        }

        String my_str_key = sale_price+","+province+","+date_time;
        int my_int_val = 1;

        System.out.println(sale_price+" "+province+" "+date_time);


    }


    public static void main(String[] args)  throws Exception{
        String file_name = "C:\\Temp\\Data\\dataSuperstoreSales2010.csv";
        test my_test = new test();
        my_test.process_file(file_name);
    }
}
