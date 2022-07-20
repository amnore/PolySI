// JNI
#include <jni.h>
#include "gpu_GPUmm.h"

// CUDA
#include <cuda_runtime.h>
#include <cublas_v2.h>

#include <iostream>
#include <exception>
#include <utility>
#include <cstdint>

#include <sys/time.h>

// ====== Vars ======
#define THREADS_PER_BLOCK 512
#define REGULATE_BATCH 1000

static const char* get_error(cublasStatus_t status) {
    return cublasGetStatusName(status);
}

static const char *get_error(cudaError_t err) {
    return cudaGetErrorString(err);
}

#define CUDA_ASSERT(expr) \
    do { auto err = (expr); if(err) {std::cerr << __FILE__ ":" << __LINE__ << ":" << get_error(err) << '\n'; std::terminate();} } while(0)

static void
denseSgemm(cublasHandle_t handle, float *gpu_src, float *gpu_dst, int n) {
    float alpha = 1.0, beta = 0.0;
  CUDA_ASSERT(cublasSgemm(
        handle,
        CUBLAS_OP_N, CUBLAS_OP_N,
        n, n, n,
        &alpha,
        gpu_src, n,
        gpu_src, n,
        &beta,
        gpu_dst, n));
  CUDA_ASSERT(cudaDeviceSynchronize());
  std::cerr << "  [GPU] dense gemm\n";
}

static void
denseSgemm(cublasHandle_t handle, __half *gpu_src, __half *gpu_dst, int n) {
    __half alpha = 1.0, beta = 0.0;
  CUDA_ASSERT(cublasHgemm(
        handle,
        CUBLAS_OP_N, CUBLAS_OP_N,
        n, n, n,
        &alpha,
        gpu_src, n,
        gpu_src, n,
        &beta,
        gpu_dst, n));
  CUDA_ASSERT(cudaDeviceSynchronize());
  std::cerr << "  [GPU] dense gemm\n";
}

__global__
static void regulateGPU(float *a, int length) {
  int index = (threadIdx.x + blockIdx.x * blockDim.x) * REGULATE_BATCH;
  //printf("block %d, thread %d, index[%d] => [%f]\n", blockIdx.x, threadIdx.x, index, a[index]);
  for (int i=0; i<REGULATE_BATCH; i++) {
    if (index+i < length) {
      a[index + i] = 2 * (a[index + i] != 0);
    }
  }
}

static void regulate(float *gpu_m, int length, float *cpu_m) {
  int num_blocks = ceil((double)length/THREADS_PER_BLOCK/REGULATE_BATCH);
  regulateGPU<<<num_blocks, THREADS_PER_BLOCK>>>(gpu_m, length);
  CUDA_ASSERT(cudaGetLastError());
  CUDA_ASSERT(cudaDeviceSynchronize());
}

static bool earlyTermination(cublasHandle_t handle, float *gpu_m_1, float *gpu_m_2, int length) {
  float* result_1 = (float*) malloc (sizeof(float));
  CUDA_ASSERT(cublasSasum(handle, length, gpu_m_1, 1 /*?*/, result_1));
  float* result_2 = (float*) malloc (sizeof(float));
  CUDA_ASSERT(cublasSasum(handle, length, gpu_m_2, 1 /*?*/, result_2));
  if (*result_1 == *result_2) {
    printf("EarlyTermination: %.3f == %.3f\n", *result_1, *result_2);
  }
  return *result_1 == *result_2;
}

static int power(float *cpu_m, int n) {
    static float *gpu_m = nullptr, *gpu_m2 = nullptr;
    static cublasHandle_t handle_c = nullptr;
    static int matrix_n = 0;

    if (!handle_c) {
        CUDA_ASSERT(cublasCreate(&handle_c));
    }

    if (matrix_n != n) {
        CUDA_ASSERT(cudaFree(gpu_m));
        CUDA_ASSERT(cudaFree(gpu_m2));
        CUDA_ASSERT(cudaMalloc(&gpu_m, n*n*sizeof(float)));
        CUDA_ASSERT(cudaMalloc(&gpu_m2, n*n*sizeof(float)));
        matrix_n = n;
    }

  std::cerr << "[INFO] n=" << n << "\n";


  // (1) copy the matrix to GPU
  CUDA_ASSERT(cudaMemcpy(gpu_m, cpu_m, n*n*sizeof(float), cudaMemcpyHostToDevice));

  int dense_m = 1;

  // (3.3) dense mm then
  float *gpu_src = gpu_m;
  float *gpu_dst = gpu_m2;

  while(dense_m < n) {
    denseSgemm(handle_c, gpu_src, gpu_dst, n);
    dense_m *= 2;
    regulate(gpu_dst, n*n, cpu_m);
    if(earlyTermination(handle_c, gpu_src, gpu_dst, n*n)) {
      std::cerr << "Early termination, dense_m=" << dense_m << ", n=" << n << "\n";
      break;
    }
    std::swap(gpu_src, gpu_dst);
  }

  // (4) copy the result out
  CUDA_ASSERT(cudaMemcpy(cpu_m, gpu_m, n*n*sizeof(float), cudaMemcpyDeviceToHost));
  std::cerr << "DONE, DM^" << dense_m << "\n";

  return 0;
}

void Java_gpu_GPUmm_power (JNIEnv *env, jclass cls, jfloatArray jarr, jint jn) {
  float *matrix = (float*) env->GetPrimitiveArrayCritical(jarr, 0);
  power(matrix, jn);
  env->ReleasePrimitiveArrayCritical(jarr, matrix, 0);
}

void Java_gpu_GPUmm_booleanPower
  (JNIEnv *env, jclass cls, jobjectArray jmat, jint jn) {
    float *cpu_mat = new float[(size_t) jn * jn];
    jlongArray *jarrs = new jlongArray[jn];
    long **arrs = new long*[jn];

    for (int i = 0; i < jn; i++) {
        jarrs[i] = (jlongArray)env->GetObjectArrayElement(jmat, i);
        arrs[i] = env->GetLongArrayElements(jarrs[i], nullptr);

        for (int j = 0; j < jn; j++) {
            if (arrs[i][j/64] & (1ULL << (j % 64))) {
                cpu_mat[i * jn + j] = 1;
            } else {
                cpu_mat[i * jn + j] = 0;
            }
        }
        cpu_mat[i * jn + i] = 1;
    }

    power(cpu_mat, jn);

    for (int i = 0; i < jn; i++) {
        for (int j = 0; j < jn; j++) {
            if (cpu_mat[i * jn + j] && i != j) {
                arrs[i][j / 64] |= (1ULL << (j % 64));
            }
        }

        env->ReleaseLongArrayElements(jarrs[i], arrs[i], 0);
    }
}
