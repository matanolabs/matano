//! Project-specific utilities
//!
use log::info;

#[cfg(all(feature = "my-dev-feature", not(feature = "my-prod-feature")))]
pub fn log_enabled_features() {
    info!("Congrats! The 1st feature (my-dev-feature) is enabled.");
}

#[cfg(all(feature = "my-prod-feature", not(feature = "my-dev-feature")))]
pub fn log_enabled_features() {
    info!("Congrats! The 2nd feature (my-prod-feature) is enabled.");
}

#[cfg(not(any(feature = "my-prod-feature", feature = "my-dev-feature")))]
pub fn log_enabled_features() {
    info!("No features are currently enabled.");
}

#[cfg(all(feature = "my-prod-feature", feature = "my-dev-feature"))]
pub fn log_enabled_features() {
    info!("Congrats! All features (my-dev-feature, my-prod-feature) are enabled.");
}
