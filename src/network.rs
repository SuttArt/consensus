//! Contains code for locally simulating a network with unreliable connections.

use std::{cell::Cell, fmt, fs, io::{self, Write}, path, time::{Instant, Duration}};
use std::sync::{Arc, mpsc};
use std::sync::atomic::{AtomicUsize, Ordering};

/// Constant that causes an artificial delay in the relaying of messages.
const NETWORK_DELAY: Duration = Duration::from_millis(10);

/// Virtual node in a virtual network.
pub struct NetworkNode<T> {
	/// This node's partition number.
	/// Can only communicate with nodes in the same partition.
	partition: Arc<AtomicUsize>,
	
	/// The unreliable channel. Local sends *always* succeed.
	/// MPSC to bypass dealing with multiplexing multiple connections.
	channel: (mpsc::Sender<T>, mpsc::Receiver<T>),
	
	/// Public (unique) address of this node.
	/// Useful to identify this node in messages to other nodes.
	pub address: usize,
	
	/// Logfile to store committed entries in.
	log_file: fs::File,
	
	/// Number of entries in the logfile.
	log_entry_no: Cell<u32>
}

/// Reliable channel to a network node.
/// Can be "upgraded" to a full (unreliable) Connection.
#[derive(Debug)] 
pub struct Channel<T> {
	/// Entry point for sending messages to the node that created this channel.
	port: mpsc::Sender<T>,
	
	/// Unique address of the creator.
	pub address: usize,
	
	/// Pointer to the creators partition number.
	pub part: Arc<AtomicUsize>
}

/// Unreliable one-directional connection between two virtual network nodes.
pub struct Connection<T> {
	/// Entry point for sending messages to the node that created this channel.
	port: mpsc::Sender<T>,
	
	/// Partition number of the receiver.
	/// Note: if this is different from the sender's, no messages are relayed
	src: Arc<AtomicUsize>,
	
	/// Partition number of the sender.
	/// Note: if this is different from the receiver's, no messages are relayed
	dst: Arc<AtomicUsize>
}

impl<T> NetworkNode<T> {
	/// Creates a new network node and stores logfile in the specified path.
	pub fn new<P: AsRef<path::Path>>(address: usize, path: P) -> io::Result<Self> {
		Ok(Self {
			partition: Arc::new(AtomicUsize::new(0)),
			channel: mpsc::channel(),
			address,
			log_file: fs::File::create(path.as_ref().join(address.to_string() + ".log"))?,
			log_entry_no: Cell::new(0)
		})
	}
	
	/// Creates a new (reliable) channel to this network node.
	pub fn channel(&self) -> Channel<T> {
		Channel {
			port: self.channel.0.clone(),
			address: self.address,
			part: self.partition.clone()
		}
	}
	
	/// Upgrades a channel to a fully fledged (unreliable) connection.
	/// Note: The connection can be relied upon, if the channel was created by
	/// the same node.
	pub fn accept(&self, request: Channel<T>) -> Connection<T> {
		Connection {
			port: request.port,
			src: self.partition.clone(),
			dst: request.part
		}
	}
	
	/// Receives a message from the channel with an optional timeout.
	/// This method introduces a fixed network delay of 10ms per received
	/// message if the channel was empty before. This is meant to crudely model
	/// network delays since channels would basically be instantaneous otherwise.
	pub fn decode(&self, timeout: Option<Instant>) -> Result<T, mpsc::RecvTimeoutError> {
		use std::thread::sleep;
		
		// delay if the channel was empty before, since other messages might
		// have arrived while we were waiting for the first one;
		// this is really not optimal though, since we'll also be waiting on
		// local transmissions
		match self.channel.1.try_recv() {
			Ok(msg) => Ok(msg),
			Err(_) => {
				if let Some(timeout) = timeout {
					let now = Instant::now();
					
					if now < timeout {
						let msg = self.channel.1.recv_timeout(timeout - now)?;
						sleep(NETWORK_DELAY);
						Ok(msg)
					} else {
						Err(mpsc::RecvTimeoutError::Timeout)
					}
				} else {
					let msg = self.channel.1.recv()?;
					sleep(NETWORK_DELAY);
					Ok(msg)
				}
			}
		}
	}
	
	/// Appends a message to the logfile.
	/// This can not be undone. Make sure it's consistent with the other nodes.
	pub fn append<E: fmt::Debug>(&self, entry: &E) {
		let log_entry_no = self.log_entry_no.get() + 1;
		
		(&self.log_file).write(
			format!("{:4} {:?}\n", log_entry_no, entry).as_bytes()
		).ok();
		
		self.log_entry_no.set(log_entry_no);
	}
}

impl<T> Channel<T> {
	/// Reliably sends a message through the channel.
	/// Both order and content of the message are preserved.
	pub fn send(&self, t: T) {
		self.port.send(t).unwrap();
	}
}

impl<T> Clone for Channel<T> {
	fn clone(&self) -> Self {
		Self {
			port: self.port.clone(),
			address: self.address,
			part: self.part.clone()
		}
	}
}

impl<T> Connection<T> {
	/// Tries to send a message through the connection.
	/// Order and content of the message are preserved, but sending will fail
	/// if the sender and the receiver are not in the same partition.
	pub fn encode(&self, t: T) -> Result<(), mpsc::SendError<T>> {
		use Ordering::SeqCst as Order;
		
		// prevent a data race here by checking if the operation is local first
		if Arc::ptr_eq(&self.src, &self.dst)
		|| self.src.load(Order) == self.dst.load(Order) {
			self.port.send(t)
		} else {
			Err(mpsc::SendError(t))
		}
	}
}

/// Routine disrupting and restoring the network connections randomly.
pub fn daemon<T>(mut channels: Vec<Channel<T>>, events_per_sec: f32, duration_in_sec: f32) {
	use std::{thread, collections::BinaryHeap};
	use tracing::{trace_span, trace};
	use rand::prelude::*;
	use rand_distr::{Exp, Uniform};
	
	struct Event<T> {
		time: Instant,
		kind: EventType<T>
	}
	
	enum EventType<T> {
		Disrupt,
		Restore(Channel<T>)
	}
	
	impl<'a, T> PartialOrd for Event<T> {
		fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
			Some(self.cmp(other))
		}
	}
	
	impl<'a, T> Ord for Event<T> {
		fn cmp(&self, other: &Self) -> std::cmp::Ordering {
			self.time.cmp(&other.time).reverse()
		}
	}
	
	impl<'a, T> PartialEq for Event<T> {
		fn eq(&self, other: &Self) -> bool {
			self.time.eq(&other.time)
		}
	}
	
	impl<'a, T> Eq for Event<T> {}
	
	// short-circuit no-op daemons
	if events_per_sec == 0.0 || duration_in_sec == 0.0 {
		return;
	}
	
	// assert at least one network node
	assert_ne!(channels.len(), 0);
	
	// get the threads random number generator and the current time
	let mut rng = thread_rng();
	let start = Instant::now();
	
	// create a span for the daemon process
	let _guard = trace_span!("Daemon");
	let _guard = _guard.enter();
	
	// create the neg-exp delay distribution
	// (rationale: assuming independent failure rates)
	let delay = Exp::new(events_per_sec / 1000.0).unwrap();
	
	// create the neg-exp duration distribution
	// (rationale: assuming independent failure causes)
	let duration = Exp::new(1.0 / (duration_in_sec * 1000.0)).unwrap();
	
	// create a uniform partition distribution
	// (rationale: assuming equal chance of being grouped together)
	let partition = Uniform::new(1, channels.len());
	
	// initialize a priority queue for the scheduler
	let mut sched = BinaryHeap::new();
	
	// add the first disruption event to the scheduler
	sched.push(Event {
		time: start + Duration::from_millis(rng.sample(delay) as u64),
		kind: EventType::Disrupt
	});
	
	// never-ending loop (each popped disruption-event causes another one to be pushed)
	while let Some(event) = sched.pop() {
		// suspend the thread until the next event occurs
		let now = Instant::now();
		if event.time > now {
			thread::sleep(event.time.duration_since(now));
		}
		
		// dispatch according to the event type
		match event.kind {
			EventType::Disrupt => {
				// select a random node to go offline
				channels.partial_shuffle(&mut rng, 1);
				
				// remove this node from the pool of disruptible nodes
				if let Some(channel) = channels.pop() {
					let partition = rng.sample(partition);
					channel.part.store(partition, Ordering::SeqCst);
					
					trace!(node = channel.address, "disrupt");
					
					// add the restoration-event to the scheduler
					sched.push(Event {
						time: now + Duration::from_millis(rng.sample(duration) as u64),
						kind: EventType::Restore(channel)
					});
				}
				
				// add another disruption event to the scheduler
				sched.push(Event {
					time: now + Duration::from_millis(rng.sample(delay) as u64),
					kind: EventType::Disrupt
				});
			}
			EventType::Restore(channel) => {
				// restore the previously disrupted network node
				channel.part.store(0, Ordering::SeqCst);
				trace!(node = channel.address, "restore");
				channels.push(channel);
			}
		}
	}
}
