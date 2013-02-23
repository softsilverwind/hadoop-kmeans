package org.myorg.KMeans;

import java.io.*;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class ClusterJob
{
	public static void initData(JobConf conf, Path pointPath, Path centroidPath, int k, int dim)
	{
		try
		{
			FSDataInputStream in = FileSystem.get(conf).open(pointPath);
		    BufferedReader d = new BufferedReader(new InputStreamReader(in));
		    
		    String[] strings = d.readLine().split(" ");
		    double[][] centroids = new double[k][dim];
		    for(int i = 0; i < k; ++i)
		    	for(int j = 0; j < dim; ++j)
		    		centroids[i][j] = Double.parseDouble(strings[i*dim + j]);
		    
		    writeCentroids(centroidPath, conf, centroids);
		}
		catch (Exception e)
		{
			throw new RuntimeException(e);
		}
	}
	
	public static void writeCentroids(Path path, JobConf conf, double[][] centroids)
	{
		int k = Integer.parseInt(conf.get("k"));
		int dim = Integer.parseInt(conf.get("dim"));

		try
		{
			SequenceFile.Writer writer = new SequenceFile.Writer(FileSystem.get(conf), conf, path, IntWritable.class, MyArray.class);
			
			for (int i = 0; i < k; ++i)
			{
				DoubleWritable[] arr = new DoubleWritable[centroids[i].length];
				for (int j = 0; j < dim; ++j)
					arr[j] = new DoubleWritable(centroids[i][j]);
				MyArray value = new MyArray(arr);
				
				writer.append(new IntWritable(i), value);
			}
			
			writer.close();
		}
		catch (Exception e)
		{
			throw new RuntimeException(e);
		}
	}
	
	public static double[][] readCentroids(Path path, JobConf conf)
	{
		int k = Integer.parseInt(conf.get("k"));
		int dim = Integer.parseInt(conf.get("dim"));
		
		double[][] centroids = new double[k][dim];

		SequenceFile.Reader reader = null;
		try
		{
			reader = new SequenceFile.Reader(FileSystem.get(conf), path, conf);
			IntWritable key = new IntWritable();
			MyArray value = new MyArray();
			DoubleWritable[][] arr = new DoubleWritable[k][];
			while (reader.next(key, value))
				arr[key.get()] = (DoubleWritable[]) value.toArray();
			
			for(int i = 0; i < k; ++i)
				for(int j = 0; j < dim; ++j)
					centroids[i][j] = arr[i][j].get();
				
		}
		catch (Exception e)
		{
			throw new RuntimeException(e);
		}
		finally
		{
			try
			{
				reader.close();
			}
			catch (Exception e)
			{
				throw new RuntimeException(e);
			}
		}
		
		return centroids;
	}
	
	public static double[][] readCentroids(String filename, JobConf conf)
	{
		return readCentroids(new Path(filename), conf);
	}
	
	public static boolean checkConvergence(JobConf conf)
	{
		double[][] prevCentroids = readCentroids(conf.get("centroidPath"), conf);
		double[][] newCentroids = readCentroids(conf.get("newCentroidPath"), conf);
		int k = Integer.parseInt(conf.get("k"));
		int dim = Integer.parseInt(conf.get("dim"));
		
		for(int i = 0; i < k; ++i)
			for (int j = 0; j < dim; ++j)
				if (Math.abs(prevCentroids[i][j] - newCentroids[i][j]) > 1E-10)
					return false;
		
		return true;
	}
	
	// args: { Points, k, Dimensions, Number of Reducers, [centroids] }
	public static void main(String[] args) throws Exception
	{
		int iteration = 1;

		String centroidPath = "centroids_0";
		String pointPath = args[0];
		
		JobConf conf = new JobConf(ClusterJob.class);
		
		int k = Integer.parseInt(args[1]);
		int dim = Integer.parseInt(args[2]);
		conf.set("dim", args[2]);
		conf.set("k", args[1]);
		
		Path nulll = new Path("nulll");
		FileSystem fs = FileSystem.get(conf);
		
		if (args.length == 4)
			initData(conf, new Path(pointPath.split(",")[0]), new Path(centroidPath), k, dim);
		else if (args.length == 5)
			initData(conf, new Path(args[4]), new Path(centroidPath), k, dim);
		else
		{
			System.err.println("You forgot the args: Points, k, Dimensions, Number of Reducers, [centroids]");
			System.exit(1);
		}
		
		boolean converged;
		do
		{
			conf.set("centroidPath", centroidPath);
			conf.set("dim", args[2]);
			conf.set("k", args[1]);
			centroidPath = "centroids_" + iteration;
			conf.setJobName("JAVA " + pointPath + "->" + centroidPath);
			conf.setNumReduceTasks(Integer.parseInt(args[3]));
			
			conf.setMapOutputKeyClass(IntWritable.class);
			conf.setMapOutputValueClass(MyTuple.class);
			
			conf.setOutputKeyClass(IntWritable.class);
			conf.setOutputValueClass(MyArray.class);
			
			conf.setMapperClass(KMapper.class);
			conf.setReducerClass(KReducer.class);
			
			conf.setInputFormat(TextInputFormat.class);
			conf.setOutputFormat(TextOutputFormat.class);
			
			for (String path : pointPath.split(","))
				FileInputFormat.addInputPath(conf, new Path(path));
			
			if (fs.exists(nulll))
				fs.delete(nulll, true);
			FileOutputFormat.setOutputPath(conf, nulll);
			
			conf.set("newCentroidPath", centroidPath);
			JobClient.runJob(conf);
			converged = checkConvergence(conf);
			conf = new JobConf(ClusterJob.class);

			iteration++;
		}
		while(!converged);
	}
}
