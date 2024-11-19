package cis5550.test;

import cis5550.flame.FlameContext;
import cis5550.flame.FlameRDD;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

public class FlameCollect {
	public static void run(FlameContext ctx, String args[]) throws Exception {
		LinkedList<String> list = new LinkedList<String>();
		for (int i=0; i<args.length; i++)
			list.add(args[i]);
	
		FlameRDD rdd = ctx.parallelize(list);
	
		List<String> out = rdd.collect();
		Collections.sort(out);
	
		String result = "";
		for (String s : out) 
			result = result+(result.equals("") ? "" : ",")+s;

		ctx.output(result);
	}
}