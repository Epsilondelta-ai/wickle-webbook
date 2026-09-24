use std::sync::Arc;

fn describe(request: &str) {
    println!("request: {request}");
}

fn main() {
    let request = String::from("report");
    describe(&request);
    let shared = Arc::new(request);
    let observer = Arc::clone(&shared);
    println!("shared owners: {}", Arc::strong_count(&shared));
    assert_eq!(observer.as_str(), "report");
}
