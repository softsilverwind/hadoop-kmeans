package org.myorg.KMeans;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapred.*;

public class Printer
{
	public static void main(String[] args) throws Exception
	{
		JobConf conf = new JobConf(ClusterJob.class);
		
		int k = Integer.parseInt(args[0]);
		int dim = Integer.parseInt(args[1]);
		int jobs = Integer.parseInt(args[2]);
		
		conf.set("k", args[0]);
		conf.set("dim", args[1]);
		
		for (int i = 0; i < jobs; ++i)
		{
			double[][] centroids = ClusterJob.readCentroids(new Path("centroids_" + i), conf);
			
			for (int kk = 0; kk < k; ++kk)
			{
				for (int dimm = 0; dimm < dim; ++dimm)
				{
					System.out.print(centroids[kk][dimm] + ",");
				}
				System.out.println();
			}
			System.out.println();
			System.out.println();
		}
	}
}