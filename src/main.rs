use std::{time::Duration, collections::HashMap, sync::{Arc, Mutex}};
use tokio::{task::JoinHandle, sync::{oneshot, mpsc}};

type Request = i32;
type Message = Arc<String>;
type RequestMessage = (Request, oneshot::Sender<Message>);

struct Cache {
    sender: mpsc::Sender<RequestMessage>,
}

impl Default for Cache {
    
    fn default() -> Self {
        let (tx, mut rx) = mpsc::channel::<RequestMessage>(10);
        let requests: Arc<Mutex<HashMap<Request, Vec<oneshot::Sender<Message>>>>> = Arc::new(Mutex::new(HashMap::new()));
        
        tokio::spawn(async move {
            while let Some((request, sender)) = rx.recv().await {
                let mut requests_lock =  requests.lock().unwrap();
                
                if let Some(request) = requests_lock.get_mut(&request) {
                    request.push(sender);
                    
                    drop(requests_lock);
                } else {
                    requests_lock.insert(request, vec![sender]);
                    
                    drop(requests_lock);
                    
                    let requests = Arc::clone(&requests);
                    
                    tokio::spawn(async move {
                        async_std::task::sleep(Duration::from_secs(request as u64)).await;
                        
                        let message = Arc::new(request.to_string());
                        
                        if let Some(senders) = requests.lock().unwrap().remove(&request) {
                            for sender in senders {
                                sender.send(Arc::clone(&message)).unwrap();
                            }
                        }
                    });
                }
            }
        });
        
        Self {
            sender: tx,
        }
    }
}

impl Cache {
    
    pub async fn request(
        &self,
        request: Request,
    ) -> Result<Message, oneshot::error::RecvError>
    {
        let (tx, rx) = oneshot::channel::<Message>();
        let _ = self.sender.send((request, tx)).await;
        
        rx.await
    }
}

fn spawn_requester(cache: Arc<Cache>, request: i32,) -> JoinHandle<()> {
    tokio::spawn(async move {
        println!("{}", cache.request(request).await.unwrap());
    })
}

#[tokio::main]

async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cache = Arc::new(Cache::default());
    let handles = vec![
        spawn_requester(Arc::clone(&cache), 1),
        spawn_requester(Arc::clone(&cache), 1),
        spawn_requester(Arc::clone(&cache), 1),
        spawn_requester(Arc::clone(&cache), 1),
        spawn_requester(Arc::clone(&cache), 2),
        spawn_requester(Arc::clone(&cache), 2),
    ];
    
    for handle in handles {
        handle.await?;
    }
    
    Ok(())
}
