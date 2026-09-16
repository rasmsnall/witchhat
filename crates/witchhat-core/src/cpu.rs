//! Runtime CPU feature detection.
//!
//! SIMD-accelerated paths must never change output, only speed: a hash or
//! transform computed on an AVX2 machine has to match the scalar fallback
//! bit-for-bit, or results stop being reproducible across a fleet of mixed
//! hardware (build box vs. Databricks worker vs. laptop). Detection here is
//! purely a dispatch hint, never a correctness input.

use std::sync::OnceLock;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct CpuFeatures {
    pub sse42: bool,
    pub avx2: bool,
    pub avx512f: bool,
    pub neon: bool,
}

impl CpuFeatures {
    fn detect() -> Self {
        #[cfg(target_arch = "x86_64")]
        {
            CpuFeatures {
                sse42: is_x86_feature_detected!("sse4.2"),
                avx2: is_x86_feature_detected!("avx2"),
                avx512f: is_x86_feature_detected!("avx512f"),
                neon: false,
            }
        }
        #[cfg(target_arch = "aarch64")]
        {
            CpuFeatures {
                sse42: false,
                avx2: false,
                avx512f: false,
                neon: std::arch::is_aarch64_feature_detected!("neon"),
            }
        }
        #[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
        {
            CpuFeatures::default()
        }
    }
}

/// The detected features of the CPU running this process, computed once.
pub fn features() -> CpuFeatures {
    static FEATURES: OnceLock<CpuFeatures> = OnceLock::new();
    *FEATURES.get_or_init(CpuFeatures::detect)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn detect_is_stable_across_calls() {
        assert_eq!(features(), features());
    }
}
