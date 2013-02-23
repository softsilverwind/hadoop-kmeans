package org.myorg.KMeans;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.DoubleWritable;

// http://stackoverflow.com/questions/4386781/implementation-of-an-arraywritable-for-a-custom-hadoop-type/4390928#4390928
class MyArray extends ArrayWritable
{
	public MyArray()
	{
	    super(DoubleWritable.class);
	}
	
	public MyArray(DoubleWritable[] values)
	{
	    super(DoubleWritable.class, values);
	}
}
