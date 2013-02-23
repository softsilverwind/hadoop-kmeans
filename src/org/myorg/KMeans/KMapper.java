package org.myorg.KMeans;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class KMapper
	extends org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, IntWritable, MyTuple>
	implements Mapper<LongWritable, Text, IntWritable, MyTuple>
{
	double[][] centroids;
	int k;
	int dim;
	
	public void configure(JobConf conf)
	{
		k = Integer.parseInt(conf.get("k"));
		dim = Integer.parseInt(conf.get("dim"));
		
		centroids = ClusterJob.readCentroids(conf.get("centroidPath"), conf);
	}

	private double distance(double[] f, double[] s)
	{
		double dist = 0;
		for(int i = 0; i < f.length; ++i)
			dist += (f[i]-s[i])*(f[i]-s[i]);
		return dist;
	}
	
	public void map(LongWritable key, Text value,
		OutputCollector<IntWritable, MyTuple> collector,
		Reporter reporter) throws IOException
	{
		String line = value.toString();
		
		String[] strings = line.split(" ");
		
		ArrayList<String> list = new ArrayList<String>();
			
		for (int i = 0; i < strings.length; ++i)
			if (strings[i].length() != 0)
				list.add(strings[i]);
		
		int len = list.size();
		
		double[][] points = new double[len / dim][dim];
		
		double[][] sums = new double[k][dim];
		int[] n = new int[k];
				
		for (int i = 0; i < len/dim; ++i)
			for(int j = 0; j < dim; ++j)
				points[i][j] = Double.parseDouble(list.get(i * dim + j));
		
		for (double[] point : points)
		{
			double mindist = Double.MAX_VALUE;
			int mincent = -1;
			for(int i = 0; i < k; ++i)
			{
				double[] centroid = centroids[i];
				double dist = distance(point, centroid);
				
				if (dist < mindist)
				{
					mindist = dist;
					mincent = i;
				}
			}
			
			for (int i = 0; i < dim; ++i)
				sums[mincent][i] += point[i];
			
			++n[mincent];
		}
		
		for (int i = 0; i < k; ++i)
		{
			DoubleWritable[] dw = new DoubleWritable[dim];
			for (int j = 0; j < dim; ++j)
				dw[j] = new DoubleWritable(sums[i][j]);
			
			collector.collect(new IntWritable(i), 
					new MyTuple(new IntWritable(n[i]), new MyArray(dw)));
		}
	}

	public void close() throws IOException
	{
	}

}
