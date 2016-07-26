package myudfs;
import java.io.IOException;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

  public class DateToMonth extends EvalFunc<String>
  {
    public String exec(Tuple input) throws IOException {
        if (input == null || input.size() == 0)
            return null;
        try{
            String str = (String)input.get(0);
	    Pattern p = Pattern.compile("(\\d*)/(\\d*)/(\\d*)\\s*(\\d*):(\\d*)");
            Matcher m = p.matcher(str);
	    String month = getMonth(m.group(1));
            return month;
        }catch(Exception e){
            throw new IOException("Caught exception processing input row ", e);
        }
    }

    public String getMonth(String month) {
            switch (month) {
            case "1" :  return "Jan";
            case "01":  return "Jan";
            case "2" :  return "Feb";
            case "02":  return "Feb";        
            case "3" :  return "Mar";
            case "03":  return "Mar";         
            case "4" :  return "Apr";
            case "04":  return "Apr";      
            case "5" :  return "May";
            case "05":  return "May";       
            case "6" :  return "Jun";
            case "06":  return "Jun";        
            case "7" :  return "Jul";
            case "07":  return "Jul";       
            case "8" :  return "Aug";
            case "08":  return "Aug";        
            case "9" :  return "Sep";
            case "09":  return "Sep";         
            case "10":  return "Oct";
            case "11":  return "Nov";
            case "12":  return "Dec";
            default  : return  "Invalid month";
            }            
        }
	
  }
