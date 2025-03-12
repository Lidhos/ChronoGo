package util

import (
	"math"
	"testing"
)

func TestValueEncoder(t *testing.T) {
	tests := []struct {
		name   string
		values []float64
	}{
		{
			name:   "单个值",
			values: []float64{123.456},
		},
		{
			name:   "相同的值",
			values: []float64{123.456, 123.456, 123.456},
		},
		{
			name:   "不同的值",
			values: []float64{123.456, 789.012, 345.678},
		},
		{
			name:   "递增的值",
			values: []float64{1.0, 2.0, 3.0, 4.0, 5.0},
		},
		{
			name:   "递减的值",
			values: []float64{5.0, 4.0, 3.0, 2.0, 1.0},
		},
		{
			name:   "接近的值",
			values: []float64{1.0001, 1.0002, 1.0003, 1.0004},
		},
		{
			name:   "特殊值",
			values: []float64{0.0, math.NaN(), math.Inf(1), math.Inf(-1)},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// 编码
			encoder := NewValueEncoder()
			for _, v := range tt.values {
				encoder.Write(v)
			}
			encoded := encoder.Bytes()

			// 解码
			decoder := NewValueDecoder(encoded)
			for i, expected := range tt.values {
				actual, ok := decoder.Next()
				if !ok {
					t.Fatalf("第 %d 个值解码失败", i)
				}

				// 对于NaN，单独处理
				if math.IsNaN(expected) {
					if !math.IsNaN(actual) {
						t.Errorf("第 %d 个值: 期望 NaN, 实际 %v", i, actual)
					}
					continue
				}

				// 对于Inf，单独处理
				if math.IsInf(expected, 0) {
					if !math.IsInf(actual, int(math.Copysign(1, expected))) {
						t.Errorf("第 %d 个值: 期望 %v, 实际 %v", i, expected, actual)
					}
					continue
				}

				// 普通值，检查是否相等
				if expected != actual {
					t.Errorf("第 %d 个值: 期望 %v, 实际 %v", i, expected, actual)
				}
			}

			// 确保没有多余的值
			_, ok := decoder.Next()
			if ok {
				t.Errorf("解码器返回了多余的值")
			}
		})
	}
}

func TestGorillaDeltaEncoder(t *testing.T) {
	tests := []struct {
		name   string
		values []float64
	}{
		{
			name:   "单个值",
			values: []float64{123.456},
		},
		{
			name:   "相同的值",
			values: []float64{123.456, 123.456, 123.456},
		},
		{
			name:   "不同的值",
			values: []float64{123.456, 789.012, 345.678},
		},
		{
			name:   "递增的值",
			values: []float64{1.0, 2.0, 3.0, 4.0, 5.0},
		},
		{
			name:   "递减的值",
			values: []float64{5.0, 4.0, 3.0, 2.0, 1.0},
		},
		{
			name:   "接近的值",
			values: []float64{1.0001, 1.0002, 1.0003, 1.0004},
		},
		{
			name:   "特殊值",
			values: []float64{0.0, math.NaN(), math.Inf(1), math.Inf(-1)},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// 编码
			encoder := NewGorillaDeltaEncoder()
			for _, v := range tt.values {
				encoder.Write(v)
			}
			encoded := encoder.Bytes()

			// 解码
			decoder := NewGorillaDeltaDecoder(encoded)
			for i, expected := range tt.values {
				actual, ok := decoder.Next()
				if !ok {
					t.Fatalf("第 %d 个值解码失败", i)
				}

				// 对于NaN，单独处理
				if math.IsNaN(expected) {
					if !math.IsNaN(actual) {
						t.Errorf("第 %d 个值: 期望 NaN, 实际 %v", i, actual)
					}
					continue
				}

				// 对于Inf，单独处理
				if math.IsInf(expected, 0) {
					if !math.IsInf(actual, int(math.Copysign(1, expected))) {
						t.Errorf("第 %d 个值: 期望 %v, 实际 %v", i, expected, actual)
					}
					continue
				}

				// 普通值，检查是否相等
				if expected != actual {
					t.Errorf("第 %d 个值: 期望 %v, 实际 %v", i, expected, actual)
				}
			}

			// 确保没有多余的值
			_, ok := decoder.Next()
			if ok {
				t.Errorf("解码器返回了多余的值")
			}
		})
	}
}

func BenchmarkValueEncoder(b *testing.B) {
	// 准备测试数据
	data := make([]float64, 1000)
	for i := range data {
		data[i] = float64(i) * 0.1
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		encoder := NewValueEncoder()
		for _, v := range data {
			encoder.Write(v)
		}
		_ = encoder.Bytes()
	}
}

func BenchmarkValueDecoder(b *testing.B) {
	// 准备测试数据
	data := make([]float64, 1000)
	for i := range data {
		data[i] = float64(i) * 0.1
	}

	// 编码
	encoder := NewValueEncoder()
	for _, v := range data {
		encoder.Write(v)
	}
	encoded := encoder.Bytes()

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		decoder := NewValueDecoder(encoded)
		for {
			_, ok := decoder.Next()
			if !ok {
				break
			}
		}
	}
}

func BenchmarkGorillaDeltaEncoder(b *testing.B) {
	// 准备测试数据
	data := make([]float64, 1000)
	for i := range data {
		data[i] = float64(i) * 0.1
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		encoder := NewGorillaDeltaEncoder()
		for _, v := range data {
			encoder.Write(v)
		}
		_ = encoder.Bytes()
	}
}

func BenchmarkGorillaDeltaDecoder(b *testing.B) {
	// 准备测试数据
	data := make([]float64, 1000)
	for i := range data {
		data[i] = float64(i) * 0.1
	}

	// 编码
	encoder := NewGorillaDeltaEncoder()
	for _, v := range data {
		encoder.Write(v)
	}
	encoded := encoder.Bytes()

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		decoder := NewGorillaDeltaDecoder(encoded)
		for {
			_, ok := decoder.Next()
			if !ok {
				break
			}
		}
	}
}

func BenchmarkCompressionRatio(b *testing.B) {
	testCases := []struct {
		name string
		data []float64
	}{
		{
			name: "递增的值",
			data: func() []float64 {
				result := make([]float64, 1000)
				for i := range result {
					result[i] = float64(i) * 0.1
				}
				return result
			}(),
		},
		{
			name: "随机值",
			data: func() []float64 {
				result := make([]float64, 1000)
				for i := range result {
					result[i] = math.Sin(float64(i) * 0.1)
				}
				return result
			}(),
		},
		{
			name: "相同的值",
			data: func() []float64 {
				result := make([]float64, 1000)
				for i := range result {
					result[i] = 123.456
				}
				return result
			}(),
		},
		{
			name: "接近的值",
			data: func() []float64 {
				result := make([]float64, 1000)
				for i := range result {
					result[i] = 100.0 + float64(i)*0.001
				}
				return result
			}(),
		},
	}

	for _, tc := range testCases {
		b.Run(tc.name+"_ValueEncoder", func(b *testing.B) {
			// 原始大小
			originalSize := len(tc.data) * 8 // 每个float64占8字节

			// 编码
			encoder := NewValueEncoder()
			for _, v := range tc.data {
				encoder.Write(v)
			}
			encoded := encoder.Bytes()

			// 计算压缩比
			compressionRatio := float64(originalSize) / float64(len(encoded))
			b.ReportMetric(compressionRatio, "compression_ratio")
		})

		b.Run(tc.name+"_GorillaDeltaEncoder", func(b *testing.B) {
			// 原始大小
			originalSize := len(tc.data) * 8 // 每个float64占8字节

			// 编码
			encoder := NewGorillaDeltaEncoder()
			for _, v := range tc.data {
				encoder.Write(v)
			}
			encoded := encoder.Bytes()

			// 计算压缩比
			compressionRatio := float64(originalSize) / float64(len(encoded))
			b.ReportMetric(compressionRatio, "compression_ratio")
		})
	}
}
