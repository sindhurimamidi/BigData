package myudfs;
import java.io.IOException;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

  public class TypeDescription extends EvalFunc<String>
  {
    public String exec(Tuple input) throws IOException {
        if (input == null || input.size() == 0)
            return null;
        try{
            String type = (String)input.get(0);
	    String desc = (String)input.get(1);
	    return type+"_"+desc;
	 }catch(Exception e){
            throw new IOException("Caught exception processing input row ", e);
        }
    }
  }
