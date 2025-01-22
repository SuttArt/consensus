//! Contains the network-message-types for the consensus protocol and banking application.

use std::cmp::PartialEq;
use std::collections::HashMap;
use std::sync::mpsc::channel;
use std::time::{Duration, Instant};
use rand::Rng;
use tracing::{debug, trace};
use crate::network::{Channel, Connection, NetworkNode};

/// Message-type of the network protocol.
#[derive(Debug)]
pub enum Command {
	/// Open an account with a unique name.
	Open { account: String },
	
	/// Deposit money into an account.
	Deposit { account: String, amount: usize },
	
	/// Withdraw money from an account.
	Withdraw { account: String, amount: usize },
	
	/// Transfer money between accounts.
	Transfer { src: String, dst: String, amount: usize },
	
	/// Accept a new network connection.
	Accept(Channel<Command>),
	
	// TODO: add other useful control messages
	RequestVote { src: usize, term: usize, log_term: usize, log_length: usize },
	AppendEntries { src: usize, term: usize, log_item: Option<Box<LogItem>> },
	Vote {}
}

// TODO: add other useful structures and implementations
pub struct Actor {
	pub node: NetworkNode<Command>,
	state: ActorState,
	current_term: usize,
	timeout: Instant,
	votes: usize,
	vote_log: HashMap<usize, usize>, // term, node.address
	log: Vec<(usize, Command)>,
	// we use not Channel<Command>, but <Connection<Command>. Seems like Channel.send() shouldn't be used for sending
	pub connections: HashMap<usize,Connection<Command>>,
	current_leader: Option<usize>
}

#[derive(Debug, PartialEq)]
enum ActorState {
	Leader,
	Follower,
	Candidate,
}

#[derive(Debug)]
struct LogItem {
	index: usize,
	previous_term: usize,
	command: Option<Command>,
}

impl Actor {
	pub fn new(node: NetworkNode<Command>) -> Self {
		Actor {
			node,
			state: ActorState::Follower, // starts always with Follower
			current_term: 0,
			timeout: Instant::now(),
			votes: 0,
			vote_log: HashMap::new(),
			log: Vec::new(),
			connections: HashMap::new(),
			current_leader: None
		}
	}


	pub fn main_loop(&mut self) {
		const HEARTBEAT: Duration = Duration::from_millis(100);
		// Timeouts typically 100-300ms (from slides)
		let range = rand::thread_rng().gen_range(100..300);
		// println!("address:{} range:{}", self.node.address, range);
		let election_timeout: Duration = Duration::from_millis(range);
		self.timeout = Instant::now() + election_timeout;
		// println!("address:{} timeout:{:?}", self.node.address, self.timeout);

		loop {
			while let Ok(cmd) = self.node.decode(None) {
				match cmd {
					// customer requests
					Command::Open { account } => {
						debug!("request to open an account for {:?}", account);

						// connect to leader
						// write to local log
						// duplicate to another actors
						// commit log item
						// write to log

						// cannot be undone, proof, if everything okay
						self.node.append(&Command::Open { account })
					}
					Command::Deposit { account, amount } => {
						debug!(amount, ?account, "request to deposit");

						// cannot be undone, proof, if everything okay
						self.node.append(&Command::Deposit { account, amount })

					}
					Command::Withdraw { account, amount } => {
						debug!(amount, ?account, "request to withdraw");

						// cannot be undone, proof, if everything okay
						self.node.append(&Command::Withdraw { account, amount })
					}
					Command::Transfer { src, dst, amount } => {
						debug!(amount, ?src, ?dst, "request to transfer");

						// cannot be undone, proof, if everything okay
						self.node.append(&Command::Transfer { src, dst, amount })
					}

					// control messages
					Command::Accept(channel) => {
						trace!(origin = channel.address, "accepted connection");
						self.connections.insert(channel.address, self.node.accept(channel));
					}

					// Start the Vote
					Command::RequestVote { src, term, log_term, log_length} => {
						trace!(src, term, log_term, log_length, "request to start the vote");

						// check if I already voted for this term
						if let Some(voted_for) = self.vote_log.get(&src) {
							trace!(term, voted_for, "Already voted for this term");
							continue;
						}
						// accept term better than mine (if I current_term == term, it's means, I already voted for myself)
						if self.current_term < term {
							// accept log data equal or better than mine
							if self.log.last().map(|&(term, _)| term).unwrap_or(0) <= log_term
								&& self.log.len() <= log_length {
								// Send vote
								if let Some(connection) = self.connections.get(&src) {
									if connection.encode(Command::Vote {}).is_ok() {
										trace!(src, term, "Send vote");
										self.vote_log.insert(term, src);
										self.state = ActorState::Follower;
										self.current_term = term;
										// take some more time for actor to be elected (?) Maybe delete after
										self.timeout = Instant::now() + election_timeout + Duration::from_millis(1000);
									} else {
										trace!(src, term, "Failed to send vote");
									}
								}
							}
						}
					}

					// Send vote
					Command::Vote {} => {
						self.votes += 1;
						if self.votes >= self.connections.len() / 2 {
							debug!(self.node.address, self.current_term, "Elected leader");
							self.state = ActorState::Leader;
							self.votes = 0;
						}
					}

					// heartbeats or add log items
					Command::AppendEntries { src, term, log_item } => {
						debug!(src, term, ?log_item, "request to append entries");

						self.state = ActorState::Follower;
						self.current_term = term;
						self.current_leader = Some(src);

						match log_item {
							Some(item) => {
								// Add log item to log
								println!("Value is: {:?}", item);
							}
							None => {
								println!("Value is: None");
								// Just heartbeat, update election timeout
								self.timeout = Instant::now() + election_timeout;
							}
						}
					}
					_ => {}
				}

				/*
				Election Basics
				Increment current term
				Change to Candidate state
				Vote for self
				Send RequestVote RPCs to all other servers, retry until either:
					1. Receive votes from the majority of servers:
						- Become leader
						- Send AppendEntries heartbeats to all other servers
					2. Receive RPC from valid leader:
						- Return to follower state
					3. No-one wins election (election timeout elapses):
						- Increment term, start new election
				*/
				if self.state != ActorState::Leader {
					if Instant::now() <= self.timeout {
						continue;
					}

					self.timeout = Instant::now() + election_timeout;
					self.current_term += 1;
					self.state = ActorState::Candidate;
					self.votes += 1;
					//voted for myself -> write to log
					self.vote_log.insert(self.current_term, self.node.address);

					for connection in self.connections.values() {
						if connection.encode(Command::RequestVote {
							src: self.node.address,
							term: self.current_term,
							log_term: self.log.last().map(|&(term, _)| term).unwrap_or(0),
							log_length: self.log.len(),
						}).is_ok() {
							trace!(?connection, "Command::RequestVote sent successfully");
						} else {
							trace!(?connection, "Failed to send command Command::RequestVote");
						};
					}
				}

				if self.state == ActorState::Leader {
					if Instant::now() <= self.timeout {
						continue;
					}

					self.timeout = Instant::now() + HEARTBEAT;
					// Send AppendEntries heartbeats (Empty) to all other servers
					for connection in self.connections.values() {
						if connection.encode(Command::AppendEntries {
							src: self.node.address,
							term: self.current_term,
							log_item: None,
						}).is_ok() {
							trace!(?connection, "Command::AppendEntries sent successfully");
						} else {
							trace!(?connection, "Failed to send command Command::AppendEntries");
						};
					}
				}

			}
		}
	}
}

/// Helper macro for defining test-scenarios.
/// 
/// The basic idea is to write test-cases and then observe the behavior of the
/// simulated network through the tracing mechanism for debugging purposes.
/// 
/// The macro defines a mini-language to easily express sequences of commands
/// which are executed concurrently unless you explicitly pass time between them.
/// The script needs some collection of channels to operate over which has to be
/// provided as the first command (see next section).
/// Commands are separated by semicolons and are either requests (open an
/// account, deposit money, withdraw money and transfer money between accounts)
/// or other commands (currently only sleep).
/// 
/// # Examples
/// 
/// The following script creates two accounts (Foo and Bar) in different branch
/// offices, deposits money in the Foo-account, waits a second, transfers it to
/// bar, waits another half second and withdraws the money. The waiting periods
/// are critical, because we need to give the consensus-protocol time to confirm
/// the sequence of transactions before referring to changes made in a different
/// branch office. Within one branch office the timing is not important since
/// the commands are always delivered in sequence.
/// 
/// ```rust
///     let channels: Vec<Channel<_>>;
///     script! {
///         use channels;
///         [0] "Foo" => open(), deposit(10);
///         [1] "Bar" => open();
///         sleep();   // the argument defaults to 1 second
///         [0] "Foo" => transfer("Bar", 10);
///         sleep(0.5);// may also sleep for fractions of a second
///         [1] "Bar" => withdraw(10);
///     }
/// ```
#[macro_export]
macro_rules! script {
	// empty base case
	(@expand $chan_vec:ident .) => {};
	
	// meta-rule for customer requests
	(@expand $chan_vec:ident . [$id:expr] $acc:expr => $($cmd:ident($($arg:expr),*)),+; $($tail:tt)*) => {
		$(
			$chan_vec[$id].send(
				script! { @request $cmd($acc, $($arg),*) }
			);
		)*
		script! { @expand $chan_vec . $($tail)* }
	};
	
	// meta-rule for other commands
	(@expand $chan_vec:ident . $cmd:ident($($arg:expr),*); $($tail:tt)*) => {
		script! { @command $cmd($($arg),*) }
		script! { @expand $chan_vec . $($tail)* }
	};
	
	// customer requests
	(@request open($holder:expr,)) => {
		$crate::protocol::Command::Open {
			account: $holder.into()
		}
	};
	(@request deposit($holder:expr, $amount:expr)) => {
		$crate::protocol::Command::Deposit {
			account: $holder.into(),
			amount: $amount
		}
	};
	(@request withdraw($holder:expr, $amount:expr)) => {
		$crate::protocol::Command::Withdraw {
			account: $holder.into(),
			amount: $amount
		}
	};
	(@request transfer($src:expr, $dst:expr, $amount:expr)) => {
		$crate::protocol::Command::Transfer {
			src: $src.into(),
			dst: $dst.into(),
			amount: $amount
		}
	};
	
	// other commands
	(@command sleep($time:expr)) => {
		std::thread::sleep(std::time::Duration::from_millis(($time as f64 * 1000.0) as u64));
	};
	(@command sleep()) => {
		std::thread::sleep(std::time::Duration::from_millis(1000));
	};
	
	// entry point for the user
	(use $chan_vec:expr; $($tail:tt)*) => {
		let ref channels = $chan_vec;
		script! { @expand channels . $($tail)* }	
	};
	
	// rudimentary error diagnostics
	(@request $cmd:ident $($tail:tt)*) => {
		compile_error!("maybe you mean one of open, deposit, withdraw or transfer?")
	};
	(@command $cmd:ident $($tail:tt)*) => {
		compile_error!("maybe you mean sleep or forgot the branch index?")
	};
	(@expand $($tail:tt)*) => {
		compile_error!("illegal command syntax")
	};
	($($tail:tt)*) => {
		compile_error!("missing initial 'use <channels>;'")
	};
}
