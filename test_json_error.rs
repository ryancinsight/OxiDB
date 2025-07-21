use oxidb::core::common::errors::OxidbError;

fn main() {
    let json_err_str = "{\"invalid\": json"; // Invalid JSON
    let serde_err = serde_json::from_str::<serde_json::Value>(json_err_str).unwrap_err();
    let oxidb_err: OxidbError = serde_err.into();
    
    match oxidb_err {
        OxidbError::Json(msg) => {
            println!("✅ SUCCESS: serde_json::Error correctly converted to OxidbError::Json");
            println!("Error message: {}", msg);
        }
        OxidbError::Serialization(msg) => {
            println!("❌ FAILED: Still using Serialization variant");
            println!("Error message: {}", msg);
        }
        _ => {
            println!("❌ FAILED: Unexpected error variant");
        }
    }
}
