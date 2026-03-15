use sysinfo::System;
use tracing::info;
use std::path::Path;

pub struct ComputeProfile {
    pub has_gpu: bool,
    pub gpu_name: Option<String>,
    pub strategy: String,
}

pub fn discover_hardware() -> ComputeProfile {
    let mut sys = System::new_all();
    sys.refresh_all();
    
    // Simplistic hardware discovery
    let has_nvidia = Path::new("/dev/nvidia0").exists() || Path::new("/usr/local/cuda").exists();
    let has_metal = Path::new("/System/Library/Frameworks/Metal.framework").exists(); // Apple Silicon
    
    let (has_gpu, gpu_name, strategy) = if has_nvidia {
        (true, Some("NVIDIA RTX (CUDA)".to_string()), "VRAM Fast Path (FastEmbed + SLM)".to_string())
    } else if has_metal {
        (true, Some("Apple Silicon (Metal)".to_string()), "Metal Unified Memory".to_string())
    } else {
        (false, None, "CPU Fallback (ONNX Quantized)".to_string())
    };
    
    let profile = ComputeProfile { has_gpu, gpu_name, strategy };
    
    if profile.has_gpu {
        info!("⚡ GPU Detected: {}. Enabling Liquid Compute Fast Path.", profile.gpu_name.as_ref().unwrap());
    } else {
        info!("🐌 No GPU Detected. Falling back to CPU execution for Sanitization.");
    }
    
    profile
}
