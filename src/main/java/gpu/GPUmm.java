package gpu;

import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.FloatBuffer;

import util.UnimplementedError;

public class GPUmm {
	static {
		// System.loadLibrary("gpumm");
	}
	
	private static native void init();
	// public static native void destroy();
	// public static native void selfmm(float[] fb, int n);
	private static native void power(float[] fb, int n, boolean fresh);
	// public static native void connect(float[] fb, int[] src_list, int[] dst_list, int n);
	
	private static boolean has_init = false;
	
	public static void initGPU() {
		// init();
		// has_init = true;
	}

	public static void destroy() {}

	public static void selfmm(float[] fb, int n) {
		throw new UnimplementedError();
	}

	public static void connect(float[] fb, int[] src_list, int[] dst_list, int n) {
		throw new UnimplementedError();
	}

	public static void matrixPower(float[] fb, int n, boolean fresh) {
		throw new UnimplementedError();
	}
}
