/*
 * This reducer class collects the brand pairs based on
 * time stamp  transitions
 */
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class Reducer1 extends Reducer<Text, Text, Text, IntWritable> {
    	
		//For counting purpose an IntWritable is used
    	private final static IntWritable one = new IntWritable(1);
    	//To pass the final output, a Text is created
    	private Text consTime = new Text();
    	
    	/*
    	 * This is a feasible code but, 
    	 * when the string is reversed, 32 can become 23 so
    	 * the values are added into an array list
    	 *
    	 * private List<String> listSorting( Iterable<Text> values)
          {
            for (Text t : values) {
				
				String Str = t.toString();
				
				bList.add(Str);
			}
						
       for(int i = 0; i < bList .size(); i++) {
		   bList.set(i, new StringBuilder(bList.get(i)).reverse().toString());
		}
			Collections.sort(bList);
			for(int j = 0; j < bList .size(); j++) {
		   bList.set(j, new StringBuilder(bList.get(j)).reverse().toString());
		}
			
		}
          }    
          	
    	*/
    	
 //Method for sorting the Values according to timestamp       	
    	private ArrayList<String> listSorting(Text key, Iterable<Text> values)		
        {
    		// Array list for storing the values
    		ArrayList<String> bList = new ArrayList<String>();
          for (Text t : values)
            {
    			String Str = t.toString();
    			bList.add(Str);
    		}					
            // Sort the Brands timestamp wise
    		Collections.sort(bList);
    		int size = bList.size();
    		int pairOne = 0;
    		int pairTwo = pairOne + 1;

    		ArrayList<String> reduceOneList = new ArrayList<String>();
            // get the brand values till the size of the array    		
    		while ((pairOne < size) && (pairTwo < size)) {
    			StringBuilder sb = new StringBuilder();
    			// create the brand value apir
    			sb.append("(" + bList.get(+pairOne).split("->")[1] + "," + bList.get(pairTwo).split("->")[1] + ")");
    			reduceOneList.add(sb.toString());
    			pairOne = pairOne + 1;
    			pairTwo = pairTwo + 1;
    		}
    		return (ArrayList<String>) reduceOneList;
    	}
            public void reduce(Text key, Iterable<Text> values, Context con) throws IOException, InterruptedException {
              	ArrayList<String> treePair = (ArrayList<String>) listSorting(key,values);            	
            	//pass the key as brand pairs one value as IntWritable one
            	for(String str: treePair){
            		consTime.set(str);
            		con.write(consTime, one);
            	}        	
            }
    }
