use std::{future::Future, pin::Pin, sync::Arc};
use tokio::sync::oneshot;

type Reply<'a> = Pin<Box<dyn Future<Output = String> + Send + 'a>>;

trait Model: Send + Sync {
    fn generate<'a>(&'a self, request: &'a str) -> Reply<'a>;
}

struct ScriptedModel;
impl Model for ScriptedModel {
    fn generate<'a>(&'a self, request: &'a str) -> Reply<'a> {
        Box::pin(async move { format!("answer: {request}") })
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let model: Arc<dyn Model> = Arc::new(ScriptedModel);
    let (sender, receiver) = oneshot::channel();
    let handle = tokio::spawn(async move {
        let request = String::from("report");
        let response = model.generate(&request).await;
        sender.send(response).expect("observer remains present");
    });
    drop(handle);
    let response = receiver.await.expect("worker completed");
    assert_eq!(response, "answer: report");
    println!("{response}");
    println!("dropping the task handle did not abort the worker");
}
