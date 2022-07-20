package gpu;

import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.FloatBuffer;

public class GPUmm {
    static {
        System.loadLibrary("gpumm");
    }

    private static native void power(float[] fb, int n);

    private static native void booleanPower(long[][] matrix, int n);

    public static synchronized void matrixPower(float[] fb, int n) {
        power(fb, n);
    }

    public static synchronized void matrixPower(long[][] mat, int n) {
        booleanPower(mat, n);
    }
}
