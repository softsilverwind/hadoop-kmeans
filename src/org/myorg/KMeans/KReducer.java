package org.myorg.KMeans;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class KReducer extends org.apache.hadoop.mapreduce.Reducer
		<IntWritable, MyTuple, IntWritable, MyArray>
		implements Reducer<IntWritable, MyTuple, IntWritable, MyArray>
{
	int k, dim;
	SequenceFile.Writer out;
	public void configure(JobConf conf)
	{
		k = Integer.parseInt(conf.get("k"));
		dim = Integer.parseInt(conf.get("dim"));
		
		try
		{
			out = SequenceFile.createWriter(FileSystem.get(conf),
					conf, new Path(conf.get("newCentroidPath")), IntWritable.class, MyArray.class);
		}
		catch(Exception e)
		{
			throw new RuntimeException(e);
		}
	}
		
	public void reduce(IntWritable key, Iterator<MyTuple> values,
			OutputCollector<IntWritable, MyArray> output, Reporter reporter)
			throws IOException
	{
		double[] centroid = new double[dim];
		int n = 0;
        while (values.hasNext())
        {
        	MyTuple tup = values.next();
        	
        	IntWritable f = tup.first;
        	MyArray s = tup.second;
        	
        	DoubleWritable[] sumWritable = (DoubleWritable[]) s.toArray();
        	n += f.get();
        	for (int i = 0; i < dim; ++i)
        		centroid[i] += sumWritable[i].get();
        }
       
    	for (int i = 0; i < dim; ++i)
    		centroid[i] /= n;
        
        
        DoubleWritable[] dc = new DoubleWritable[dim];
        for (int i = 0; i < dim; ++i)
        	dc[i] = new DoubleWritable(centroid[i]);
        
        out.append(key, new MyArray(dc));
        //output.collect(key, new MyArray(dc));
	}

	public void close() throws IOException {
		out.close();
	}
}