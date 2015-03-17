package test;

import java.io.File;

public class HeapDumpConfTest {

    public static void main(String[] args) {
	String heapDumpConf = "6,128,490[10]";
	//heapDumpConf = "(0 : 133,200,300)[10]";
	//heapDumpConf = "133,200,300; 1000";
	heapDumpConf = "8407734;8407737;8407740;8407743;8407746;8407749;8407750";
	
	System.out.println(heapDumpConf + " ==>");
	long[] records = parseHeapDumpConfs(heapDumpConf);
	for(int i = 0; i < records.length; i++)
	    System.out.println(records[i]);
	

    }
    
    /* headDumpConfiguration:
     * 1. counter[partition] ==> interval, 2 * interval, ..., end - 1  ----- interval = (counter - 1) / partition
     *		e.g., 133,200,300[10] ==> 13320029, 26640058, 39960087, 53280116, 66600145,
     *				          79920174, 93240203, 106560232, 119880261, 133200299
     * 
     * 2. (begin : end)[partition] ==> begin, begin + interval, begin + 2 * interval, ..., end - 1
     * 		e.g., (1,000,000 : 133,200,300)[10] ==> 1000000, 14220029, 27440058, 40660087, 53880116,
     * 							67100145, 80320174, 93540203, 106760232, 119980261,
     *						        133200298
     *
     * 3. counter{interval} ==> interval, 2 * interval, 3 * interval, ..., n * interval (<= counter - 1)
     * 		e.g., (1,000,000 : 133,200,300)[10] ==> 13320029, 26640058, 39960087, 53280116, 66600145,
     *				                        79920174, 93240203, 106560232, 119880261, 133200299
     */
    
    public static long[] parseHeapDumpConfs(String heapDumpConf) {
	if (heapDumpConf == null)
	    return null;
	// 133,200,300[10] or 133200300[10]
	else if(heapDumpConf.contains("[") && !heapDumpConf.contains(":")) {
	    int loc = heapDumpConf.indexOf('[');
	    long maxValue = Long.parseLong(heapDumpConf.substring(0, loc).replaceAll(",", "").trim()) - 1;
	    int partition = Integer.parseInt(heapDumpConf.substring(loc + 1, 
		   heapDumpConf.lastIndexOf(']')).replaceAll(",", "").trim());
	    long interval = (long) (maxValue / partition);
	    long[] values = new long[partition];
	   
	    for(int i = 0; i < partition - 1; i++) 
	        values[i] = (i + 1) * interval;
	    values[partition - 1] = maxValue;
	   
	    return values;
	   
	}
	
	// (1,000,000 : 133,200,300)[10]
	else if(heapDumpConf.contains(":")) {
	    
	    String minValueStr = heapDumpConf.substring(heapDumpConf.indexOf('(') + 1, heapDumpConf.indexOf(':'))
		    .replaceAll(",", "").trim();
	    String maxValueStr = heapDumpConf.substring(heapDumpConf.indexOf(':') + 1, heapDumpConf.indexOf(')'))
		    .replaceAll(",", "").trim();
	    long minValue = Long.parseLong(minValueStr) + 1;
	    long maxValue = Long.parseLong(maxValueStr) - 1;
	    
	    int partition = Integer.parseInt(heapDumpConf.substring(heapDumpConf.indexOf('[') + 1, 
			   heapDumpConf.lastIndexOf(']')).replaceAll(",", "").trim());
	    long interval = (long) ((maxValue - minValue) / partition);
	    long[] values = new long[partition + 1];
		   
	   
	    for(int i = 0; i < partition; i++) 
		values[i] = i * interval + minValue;
	    values[partition] = maxValue;
		   
	    return values;
		   
	}
	// 133,200,300{10,000,000} or 133200300{10000000}
	else if(heapDumpConf.contains("{")) {
	    int loc = heapDumpConf.indexOf('{');
	    long maxValue = Long.parseLong(heapDumpConf.substring(0, loc).replaceAll(",", "").trim()) - 1;
	    long interval = Long.parseLong(heapDumpConf.substring(loc 
		    + 1, heapDumpConf.lastIndexOf('}')).replaceAll(",", "").trim());
	    int partition = (int) (maxValue / interval);
	    if(interval * partition < maxValue)
		partition++;
	    long[] values = new long[partition];
	    
	    for(int i = 0; i < partition - 1; i++)
		values[i] = (i + 1) * interval;
	    values[partition - 1] = maxValue;
	    
	    return values;
	    
	    
	}
	
	// 133,200,300
	else {
	    String[] limits = heapDumpConf.split("(\\.|-|/|;)");
	    long[] values = new long[limits.length];
	    for (int i = 0; i < limits.length; i++) {
		values[i] = Long.parseLong(limits[i].replaceAll(",", "").trim());
	    }
	    return values;
	}

  }
}


