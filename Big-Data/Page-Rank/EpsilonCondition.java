/**
 * A sequential java program to check for epsilon condition. Considering it to be 0.05
 */
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;


public class EpsilonCondition {

	public static void main(String[] args) throws IOException {
		
		int i=1;
		File folder = new File("/Users/gayathripr31/Desktop/pagerank");
		  File[] listOfFiles = folder.listFiles();
		  while(i<listOfFiles.length)
		  {
			  //System.out.println(n);
			  if(i+1<listOfFiles.length)
			  {
				  
			  String file1= listOfFiles[i].getPath();
			  String file2= listOfFiles[i+1].getPath();
			  File f1 = new File (file1);
		        File f2 = new File (file2);
		        FileInputStream fis = new FileInputStream(f1);
				BufferedReader in = new BufferedReader(new InputStreamReader(fis));
				FileInputStream fis1 = new FileInputStream(f2);
				BufferedReader in1 = new BufferedReader(new InputStreamReader(fis1));
				ArrayList<String> list1 = new ArrayList<String>();
				ArrayList<String> list2 = new ArrayList<String>();
				String aLine = null;		
				while ((aLine = in.readLine())!= null) {
					String r[]= aLine.split("\t");
		            list1.add(r[1]);
		           
				
				}
				in.close();
				String bLine = null;		
				while ((bLine = in1.readLine())!= null) {
					String r[]= bLine.split("\t");
		            
		        	list2.add(r[1]);
		        	
				}
				in1.close();
				
				int flag=0;
				for(int j=0;j<list2.size();j++)
				{
					
					Double d1 = Double.parseDouble(list1.get(j));
					Double d2 = Double.parseDouble(list2.get(j));
					Double d3 = d1-d2;
					
					if(d3<0.05) // 0.05 is the epsilon value we choose
					{
						
						
						flag =1;
						
						
					}
				}
				if(flag==1){
					
					System.out.println("Epsilon condition reached at "+i);
					break;// epsilon condition reached
				}
				}
			  i++;
		  }
		  
			}

		}
		  
		
		
        
