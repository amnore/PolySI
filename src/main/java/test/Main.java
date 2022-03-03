package test;

import gpu.GPUmm;
import util.*;
import util.VeriConstants.LoggerType;
import verifier.MonoSATVerifierOneshot;
import verifier.PolyGraphDumper;
import verifier.SIVerifier;


public class Main {

	private static String config_path = null;
	private static String benchmark_path = null;
	private static String dump_file_path = null;

	private static void printAndExit() {
		System.out.println("Usage: run mono [audit|continue|epoch|ep-remote|dump] <config path> <benchmark path> <dumpgraph path>");
		System.exit(1);
	}

	public static void parseParameters(String[] args) {
		if (args.length < 2) {
			printAndExit();
		}

		if (args.length >= 3) {
			config_path = args[2];
		} else {
			config_path = System.getProperty("user.dir")+"/"+VeriConstants.CONFIG_FILE_NAME;
		}

		if (args.length >= 4) {
			benchmark_path = args[3];
		}

		if (args.length >= 5) {
			dump_file_path = args[4];
		}
	}






	public static void fail() {
		new Exception("TODO").printStackTrace();
		assert false;
		System.exit(1);
	}

	public static void main(String[] args) {
		//ReachabilityMatrix.functionTest();
		//AdjMatrix.functionTest();

		// FIXME: parsing arguments
		parseParameters(args);
		ConfigLoader.loadConfig(config_path);
		if (benchmark_path != null) VeriConstants.LOG_FD_LOG = benchmark_path;

		if (!args[0].equals("mono")) {
			fail();
			printAndExit();
		}

		// short-cut for dump graph
		if (args[1].equals("dump")) {
			// TODO: we only support original polygraph so far
			VeriConstants.BUNDLE_CONSTRAINTS = false;
			VeriConstants.WW_CONSTRAINTS = false;
			VeriConstants.BATCH_TX_VERI_SIZE = Integer.MAX_VALUE; // read all

			PolyGraphDumper dumper = new PolyGraphDumper(VeriConstants.LOG_FD_LOG);
			assert args.length == 5;
			dumper.DumpPolyGraph(dump_file_path);
			System.exit(0); // end here
		}

		// doing some actual work
		if (VeriConstants.GPU_MATRIX) {
			GPUmm.initGPU();
		}

		Profiler prof = Profiler.getInstance();
		prof.startTick("ENTIRE_EXPERIMENT");

		if (args[1].equals("audit")) {
			// Performance.offline_performance(verifier);
			MonoSATVerifierOneshot verifier = new MonoSATVerifierOneshot(VeriConstants.LOG_FD_LOG);
			boolean pass = verifier.audit();
			if (pass) {
				ChengLogger.println("[[[[ ACCEPT ]]]]");
			} else {
				ChengLogger.println(LoggerType.ERROR, "[[[[ REJECT ]]]]");
			}
		} else if (args[1].equals("continue")) {
			throw new UnimplementedError();
		} else if (args[1].equals("epoch")) {
			throw new UnimplementedError();
		} else if (args[1].equals("ep-remote")) {
			throw new UnimplementedError();
		} else {
			printAndExit();
		}

		prof.endTick("ENTIRE_EXPERIMENT");
		ChengLogger.println(">>> Overall runtime = " + prof.getTime("ENTIRE_EXPERIMENT") + "ms");

		if (VeriConstants.GPU_MATRIX) {
			GPUmm.destroy();
		}
		System.exit(0); // for verification-in-rounds, sometimes doesn't really stop
	}

}
