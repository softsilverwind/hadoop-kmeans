package org.myorg.KMeans;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.*;

// Can't be generic type because of type erasure
public class MyTuple implements Writable
{
	public IntWritable first;
	public MyArray second;

	@Override
	public void readFields(DataInput in) throws IOException
	{
		first = new IntWritable();
		second = new MyArray();
		first.readFields(in);
		second.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException
	{
		first.write(out);
		second.write(out);
	}

	public MyTuple(IntWritable f, MyArray s)
	{
		first = f;
		second = s;
	}
	
	public MyTuple()
	{
	}
}
