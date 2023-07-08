
use std::sync::Arc;
use parking_lot::RwLock;

pub enum Continue<T> {
    Next(T),
    Done,
}

pub struct AsyncQueue<In, Out> {
    runner: Arc<RwLock<async_queue::AsyncQueue<In, Out>>>,
}
impl<In: Send + Sync + 'static, Out: Send + Sync + 'static> AsyncQueue<In, Out> {
    pub fn new<P, F, C>(parallel_processes: usize, producer: P, function: F, consumer: C) -> Self 
    where   P: Fn() -> Continue<In> + Send + Sync + 'static,
            F: Fn(In) -> anyhow::Result<Out> + Send + Sync + 'static,
            C: Fn(anyhow::Result<Out>) + Send + Sync + 'static,
    {
        Self {
            runner: Arc::new(RwLock::new(
                async_queue::AsyncQueue::new(
                    parallel_processes, 
                    producer, 
                    function,
                    consumer)
                ))
        }
    }
    #[allow(clippy::await_holding_lock)]
    pub async fn run(&self) -> anyhow::Result<()> {
        self.runner.read().run().await?;
        Ok(())
    }
}

mod async_queue {

    use tokio::spawn;
    use parking_lot::RwLock;
    use tokio::task::yield_now;
    use std::sync::Arc;
    use std::collections::VecDeque;

    use crate::processes;
    use crate::queue::Payload;

    use super::Continue;

    type Producer<In>       = Arc<dyn Fn() -> Continue<In> + Send + Sync>;
    type Function<In, Out>  = Arc<dyn Fn(In) -> anyhow::Result<Out> + Send + Sync>;
    type Consumer<Out>      = Arc<dyn Fn(anyhow::Result<Out>) + Send + Sync>;

    #[derive(Debug)]
    pub enum Index {
        Active(usize),
        InActive,
    }

    pub struct AsyncQueue<In, Out> {
        parallel_processes: usize,

        #[allow(clippy::type_complexity)]
        in_queue: Arc<RwLock<VecDeque<(Index, Continue<In>)>>>,
        out_queue: Arc<RwLock<crate::queue::Queue<Out>>>,

        producer: Producer<In>,
        function: Function<In, Out>,
        consumer: Consumer<Out>,
    }
    impl<In: Send + Sync + 'static, Out: Send + Sync + 'static> AsyncQueue<In, Out> {
        pub fn new<P, F, C>(parallel_processes: usize, producer: P, function: F, consumer: C) -> Self 
        where   P: Fn() -> Continue<In> + Send + Sync + 'static,
                F: Fn(In) -> anyhow::Result<Out> + Send + Sync + 'static,
                C: Fn(anyhow::Result<Out>) + Send + Sync + 'static,
        {
            Self {
                parallel_processes,

                in_queue: Default::default(),
                out_queue: Default::default(),

                producer: Arc::new(producer),
                function: Arc::new(function),
                consumer: Arc::new(consumer),
            }
        }
        pub async fn run(&self) -> anyhow::Result<()> {

            let max_processes = self.parallel_processes;
            let spawned_processes = processes::SpawnedProcessed::default();

            loop {
                yield_now().await;
                // producer
                while spawned_processes.current() < max_processes {
                    match (self.producer)() {
                        Continue::Done => {
                            self.in_queue.write().push_back((Index::InActive, Continue::Done));
                            break;
                        }
                        continuation => {
                            spawned_processes.inc();
                            self.in_queue.write().push_back((Index::Active(self.out_queue.write().current_next()), continuation));
                        },
                    }
                }
                // function
                if spawned_processes.current() <= max_processes {
                    while let Some((index, continuation)) = self.in_queue.write().pop_front() {
                        match index {
                            Index::InActive => break,
                            Index::Active(idx) => {
                                let ff = self.function.clone();
                                let ooq = self.out_queue.clone();
                                spawn(async move {
                                    let out = match continuation {
                                        Continue::Next(data) => Continue::Next(ff(data)),
                                        _ => Continue::Done,
                                    };
                                    ooq.write().push(crate::queue::Payload { idx, out } );
                                });
                            }
                        }
                    }
                }
                // consumer
                {
                    let mut can_read = false;
                    if let Some(Payload{ idx, out: Continue::Next(_), .. }) = self.out_queue.read().peek() {
                        can_read = self.out_queue.read().is_last(*idx);
                    }            
                    if can_read {
                        if let Some(Payload{ out: Continue::Next(out), ..}) = self.out_queue.write().pop() {
                            (self.consumer)(out);
                            spawned_processes.dec();
                        }
                    }
                }

                // terminate
                if spawned_processes.current() == 0 {
                    break;
                }
            }

            Ok(())
        }
    }

}

mod processes {

    use std::sync::{Arc, atomic::AtomicUsize};

    #[derive(Default, Clone)]
    pub struct SpawnedProcessed(Arc<AtomicUsize>);
    impl SpawnedProcessed {
        pub fn inc(&self) {
            self.0.store(self.0.load(std::sync::atomic::Ordering::Acquire) + 1, std::sync::atomic::Ordering::Relaxed)
        }
        pub fn dec(&self) {
            self.0.store(self.0.load(std::sync::atomic::Ordering::Acquire) - 1, std::sync::atomic::Ordering::Relaxed)
        }
        pub fn current(&self) -> usize {
            self.0.load(std::sync::atomic::Ordering::Acquire)
        }
    }

}

mod queue {
    use std::collections::{VecDeque, BinaryHeap};

    pub struct Payload<T> {
        pub idx: usize,
        pub out: crate::Continue<anyhow::Result<T>>,
    }
    impl<T> PartialEq for Payload<T> {
        fn eq(&self, other: &Self) -> bool {
            self.idx == other.idx
        }
    }
    impl<T> PartialOrd for Payload<T> {
        fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
            self.idx.partial_cmp(&other.idx)
        }
    }
    impl<T> Eq for Payload<T> {}
    impl<T> Ord for Payload<T> {
        fn cmp(&self, other: &Self) -> std::cmp::Ordering {
            self.idx.cmp(&other.idx)
        }
    }

    pub struct Queue<T> {
        heap: BinaryHeap<Payload<T>>,
        current: usize,
        last: usize,
    }
    impl<T> Default for Queue<T> {
        fn default() -> Self {
            Self {
                heap: Default::default(),
                current: usize::MAX,
                last: usize::MAX,
            }
        }
    }
    impl<T> Queue<T> {

        pub fn handle_overflow(&mut self) {
            if self.current() > 0 {
                return
            }

            let mut new_heap = VecDeque::new();

            let max = self.heap.iter().fold(0, |acc, payload| payload.idx.max(acc));
            while let Some(payload) = self.heap.pop() { new_heap.push_back(payload) }
            
            new_heap.into_iter()
                .map(|payload| Payload { idx: usize::MAX - max + payload.idx, out: payload.out })
                .for_each(|payload| self.heap.push(payload))
                ;

            self.last = usize::MAX;
            self.current = usize::MAX - max;
        }
        pub fn push(&mut self, payload: Payload<T>) {
            self.heap.push(payload);
        }
        pub fn peek(&self) -> Option<&Payload<T>> {
            self.heap.peek()
        }
        pub fn pop(&mut self) -> Option<Payload<T>> {
            let out = self.heap.pop();
            self.last -= 1;
            out
        }
        pub fn current(&self) -> usize {
            self.current
        }
        pub fn current_next(&mut self) -> usize {
            self.current -= 1;
            self.handle_overflow();
            self.current
        }
        pub fn last(&self) -> usize {
            self.last
        }
        pub fn is_last(&self, idx: usize) -> bool {
            self.last() == idx + 1
        }
    }

}

