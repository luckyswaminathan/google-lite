package cis5550.test;

import cis5550.flame.FlameContext;
import cis5550.flame.FlamePair;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

public class FlameMapToPair {
	public static void run(FlameContext ctx, String args[]) throws Exception {
		LinkedList<String> list = new LinkedList<String>();
		for (int i=0; i<args.length; i++)
			list.add(args[i]);
	
		List<FlamePair> out = ctx.parallelize(list)
                             .mapToPair(s -> new FlamePair(""+s.charAt(0), s.substring(1)))
                             .collect();
	
		Collections.sort(out);
	
		String result = "";
		for (FlamePair p : out) 
			result = result+(result.equals("") ? "" : ",")+p.toString();

		ctx.output(result);
	}
}