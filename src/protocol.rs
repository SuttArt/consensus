//! Contains the network-message-types for the consensus protocol and banking application.

use std::cmp::PartialEq;
use std::collections::HashMap;
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
	AppendEntries {}
}

// TODO: add other useful structures and implementations
pub struct Actor {
	pub node: NetworkNode<Command>,
	state: ActorState,
	current_term: usize,
	votes: usize,
	log: Vec<(usize, Command)>,
	// we use not Channel<Command>, but <Connection<Command>. Seems like Channel.send() shouldn't be used for sending
	pub connections: Vec<Connection<Command>>,
}

#[derive(Debug, PartialEq)]
enum ActorState {
	Leader,
	Follower,
	Candidate,
}

impl Actor {
	pub fn new(node: NetworkNode<Command>) -> Self {
		Actor {
			node,
			state: ActorState::Follower, // starts always with Follower
			current_term: 0,
			votes: 0,
			log: Vec::new(),
			connections: Vec::new(),
		}
	}


	pub fn main_loop(&mut self) {
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
						self.connections.push(self.node.accept(channel));
					}

					// Start the Vote
					Command::RequestVote { src, term, log_term, log_length} => {
						debug!(src, term, log_term, log_length, "request to start the vote");
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
					self.current_term += 1;
					self.state = ActorState::Candidate;
					self.votes += 1;

					for connection in &self.connections {
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
					// Send AppendEntries heartbeats to all other servers
					for connection in &self.connections {
						if connection.encode(Command::AppendEntries {
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
