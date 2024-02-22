package com.sid.bigdata.yellowtaxi;


import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

import java.io.File;
import java.io.IOException;

import org.junit.Test;

public class ApplicationTest {
@Test
public void yellowTaxiDataCruncherTest() throws IOException {
		String argString[]={"develop", 
							"E:/Hadoop_Home/bin/hadoop.dll",
							"src/test/resources/sample-data/yellow_taxi/input_data/sample-data.csv",
		            		"src/test/resources/sample-data/yellow_taxi/output_data/"};
		Application.main(argString);
		
		String output_path=argString[3];
		//Average Collection Share for all 3 vendors should always add up to 3 (Since 3 vendor's collections always add up to 100% of the collections)
		//assertEquals(3, sumAverageTaxiVendorAmountsShare(new File(output_path)).intValue());
		assertThat(sumAverageTaxiVendorAmountsShare(new File(output_path)).intValue(), is(3));
	}

private static Double sumAverageTaxiVendorAmountsShare(File outputDir) throws IOException {
	File[] files = outputDir.listFiles();
	Double vendorAmtSum=0.0;
    for (File file : files) {
        if (file.isDirectory()) {
           String dirName=file.getCanonicalPath();
           if (dirName.contains("compared_to_average_total_amount_by_vendor=")){
        	   String vendorArray[] = dirName.split("=");
        	   vendorAmtSum+=Double.valueOf(vendorArray[1]);
           }
        
        }
    }
	return vendorAmtSum;
}

//private static void displayDirectoryContents(File dir) throws IOException {
// 
//        File[] files = dir.listFiles();
//        for (File file : files) {
//            if (file.isDirectory()) {
//                System.out.println("Directory Name==>:" + file.getCanonicalPath());
//               // displayDirectoryContents(file);
//            } else {
//                System.out.println("file Not Acess===>" + file.getCanonicalPath());
//            }
//        }
//  
//}
}
