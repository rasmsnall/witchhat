//! Runtime CPU feature detection.
//!
//! SIMD-accelerated paths must never change output, only speed: a hash or transform
//! computed on an AVX2 machine has to match the scalar fallback bit-for-bit, or results
//! stop being reproducible across a fleet of mixed hardware (build box vs. Databricks
//! worker vs. laptop). Detection here is purely a dispatch hint for future kernels, never
//! a correctness input; nothing in [`crate::hash`] currently branches on it.

use std::sync::OnceLock;

/// CPU features detected on the machine running this process.
///
/// All fields default to `false` on an architecture this crate does not probe.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct CpuFeatures {
    /// SSE4.2, on x86_64.
    pub sse42: bool,
    /// AVX2, on x86_64.
    pub avx2: bool,
    /// AVX-512 Foundation, on x86_64.
    pub avx512f: bool,
    /// NEON, on aarch64. Always present in practice (NEON is mandatory in AArch64), but
    /// still probed rather than assumed, for symmetry with the x86_64 fields.
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

/// Returns the detected features of the CPU running this process.
///
/// Detection runs once per process (cached in a [`OnceLock`]); every call after the first
/// returns the cached value with no syscall. Does not panic, blocks only on first call
/// while the underlying `cpuid`/`getauxval` probe completes, and performs no I/O.
///
/// # Examples
///
/// ```
/// let f = witchhat_core::cpu::features();
/// // calling twice returns the same, cached value
/// assert_eq!(f, witchhat_core::cpu::features());
/// ```
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
