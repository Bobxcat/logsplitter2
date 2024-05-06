use std::thread::JoinHandle;

pub struct ThreadPool {
    handles: Vec<JoinHandle<()>>,
}

impl ThreadPool {
    pub fn new() -> Self {
        Self { handles: vec![] }
    }
    pub fn spawn(&mut self, f: impl FnOnce() + Send + 'static) {
        self.handles.push(std::thread::spawn(move || f()))
    }
    pub fn join(&mut self) {
        for h in self.handles.drain(..) {
            h.join().unwrap();
        }
    }
    pub fn active_threads(&self) -> usize {
        self.handles.len()
    }
}
