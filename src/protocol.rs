//! Contains the network-message-types for the consensus protocol and banking application.

use std::cmp::PartialEq;
use std::collections::HashMap;
use std::time::{Duration, Instant};
use rand::Rng;
use tracing::{debug, trace};
use crate::network::{Channel, Connection, NetworkNode};

/// Message-type of the network protocol.
#[derive(Debug, Clone)]
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
	Vote {},
	AppendCommand (Box<Command>),
	Response { src: usize, term: usize, index: usize, confirmation: bool },
	UpdateCommitIndex { index: usize },
}

// TODO: add other useful structures and implementations
pub struct Actor {
	pub node: NetworkNode<Command>,
	state: ActorState,
	current_term: usize,
	timeout: Instant,
	votes: usize,
	vote_log: HashMap<usize, usize>, // node.address,  term
	log: Vec<(usize, Command)>,
	tmp_log: Vec<Command>,
	// we use not Channel<Command>, but <Connection<Command>. Seems like Channel.send() shouldn't be used for sending
	pub connections: HashMap<usize,Connection<Command>>,
	// For Repairing Follower Logs, Leader keeps nextIndex for each follower:
	connections_index: HashMap<usize,Option<usize>>, // <Address, nextIndex>
	current_leader: Option<usize>,
	// Database struct, to hold Bank state, init in main.rs
	db: DB,
	commit_index: Option<usize>
}

#[derive(Debug, PartialEq)]
enum ActorState {
	Leader,
	Follower,
	Candidate,
}

#[derive(Debug, Clone)]
pub struct LogItem {
	index: usize,
	previous_term: usize,
	command: Command,
}

impl Actor {
	pub fn new(node: NetworkNode<Command>, db: DB) -> Self {
		Actor {
			node,
			state: ActorState::Follower, // starts always with Follower
			current_term: 0,
			timeout: Instant::now(),
			votes: 0,
			vote_log: HashMap::new(),
			log: Vec::new(),
			tmp_log: Vec::new(),
			connections: HashMap::new(),
			connections_index: HashMap::new(),
			current_leader: None,
			db,
			commit_index: None
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
			while let Ok(cmd) = self.node.decode(Some(self.timeout)) {
				match cmd {
					// customer requests
					Command::Open { account } => {
						trace!("request to open an account for {:?}", account);
						self.process_command(Command::Open { account: account.clone() });
					}
					Command::Deposit { account, amount } => {
						trace!(amount, ?account, "request to deposit");
						self.process_command(Command::Deposit { account: account.clone(), amount: amount.clone() });

					}
					Command::Withdraw { account, amount } => {
						trace!(amount, ?account, "request to withdraw");
						self.process_command(Command::Withdraw { account: account.clone(), amount: amount.clone() });
					}
					Command::Transfer { src, dst, amount } => {
						trace!(amount, ?src, ?dst, "request to transfer");
						self.process_command(Command::Transfer { src: src.clone(), dst: dst.clone(), amount: amount.clone() });
					}

					// control messages
					Command::Accept(channel) => {
						trace!(origin = channel.address, "accepted connection");
						self.connections.insert(channel.address, self.node.accept(channel.clone()));
						self.connections_index.insert(channel.address, None);
					}

					// Start the Vote
					Command::RequestVote { src, term, log_term, log_length} => {
						trace!(src, term, log_term, log_length, "request to start the vote");

						// check if I already voted for this term
						if let Some(voted_for) = self.vote_log.get(&term) {
							trace!(term, voted_for, "Already voted for this term");
							continue;
						}
						// accept term better than mine (if I current_term == term, it's means, I already voted for myself)
						if self.current_term < term {
							// accept log data equal or better than mine
							if self.log.last().map(|&(term, _)| term).unwrap_or(0) <= log_term
								&& self.log.len() <= log_length {
								self.vote_log.insert(term, src);
								self.state = ActorState::Follower;
								self.current_term = term;
								self.timeout = Instant::now() + election_timeout;

								// Send vote
								if let Some(connection) = self.connections.get(&src) {
									if connection.encode(Command::Vote {}).is_ok() {
										trace!(src, term, "Send vote");
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
						if self.votes >= self.connections.len() + 1 / 2 {
							debug!(self.node.address, self.current_term, "Elected leader");
							self.state = ActorState::Leader;
							self.votes = 0;
							self.current_leader = None;
							self.timeout = Instant::now(); // send immediately Heartbeat

							// check if you have something in your tmp_log
							if !self.tmp_log.is_empty() {
								let commands: Vec<_> = self.tmp_log.drain(..).collect();
								for cmd in commands {
									self.handle_append_command(cmd);
								}
							}
						}
					}

					// heartbeats or add log items
					Command::AppendEntries { src, term, log_item } => {
						// Sometimes it sends data from previous term
						if self.current_term <= term || self.state == ActorState::Candidate {
							trace!(src, term, ?log_item, "request to append entries");

							self.state = ActorState::Follower;
							self.current_term = term;
							self.current_leader = Some(src);
							// update election timeout
							self.timeout = Instant::now() + election_timeout;
							let follower_index;

							match log_item {
								Some(item) => {
									// Add log item to log
									// AppendEntries Consistency Check
									// Repairing Follower Logs
									let mut confirmation = false;

									if let Some(follower_item) = self.log.last() {
										// Get the index of the last element
										follower_index = Some(self.log.len() - 1);


										// if follower item term in log == term of prev. item in leader log, then ok
										if follower_item.0 == item.previous_term {
											// if follower item index in log == prev. item index in leader log, then ok
											if follower_index.unwrap() == item.index - 1 {
												confirmation = true;
											}
										}
									} else {
										// Log is empty
										follower_index = Some(0);
										if item.index == 0 {
											confirmation = true;
										}
									}


									// If it not the right item - we delete it from log and send Response without confirmation, that leader tries again with next log item
									if !confirmation {self.log.pop();}

									// Send Response
									if let Some(connection) = self.connections.get(&src) {
										if connection.encode(Command::Response {
											src: self.node.address,
											term,
											index: follower_index.unwrap_or(0),
											confirmation,
										}).is_ok() && confirmation {
											self.log.push((term, item.command));
											trace!(?self.log, "Updated Log.");
										} else {
											trace!(src, term, "Failed to send response.");
										}
									}
								}
								None => {
									// Just heartbeat
									// check if tmp_log hase some items -> push to the leader to process them
									if !self.tmp_log.is_empty() {
										let commands: Vec<_> = self.tmp_log.drain(..).collect();
										for cmd in commands {
											self.process_command(cmd);
										}
									}
								}
							}
						} else {
							continue;
						}
					}

					// Check if Clients Commands could be executed, if yes - send to followers. No - reject
					Command::AppendCommand(command) => {
						if self.state == ActorState::Leader {
							self.handle_append_command(*command);
						} else {
							// If somehow not leader -> store it in tmp_log and try later
							self.tmp_log.push(*command);
						}
					}

					// Respond from Followers to leader, after asking append Entries to the log
					Command::Response { src, term, mut index, confirmation } => {
						if self.state == ActorState::Leader && self.current_term == term {

							if confirmation {
								// save log index for follower
								self.connections_index.insert(src, Some(index + 1));

								self.commit(src, index + 1);
							} else {
								if index > 0 {
									index = self.log.len() -1;
								} else {
									index = 0;
								}

								let log_item = Some(Box::new(self.create_log_item(index)));

								if let Some(connection) = self.connections.get(&src) {
									if connection.encode(Command::AppendEntries {
										src: self.node.address,
										term,
										log_item: log_item.clone(),
									}).is_ok() {
										trace!(?connection, "Command::AppendEntries sent successfully");
									} else {
										trace!(?connection, "Failed to send command Command::AppendEntries");
									};
								}
							}
						}
					}

					Command::UpdateCommitIndex { index } => {
						trace!("update commit index for {:?}", index);
						self.commit_index = Option::from(index);
					}
				}
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
				self.vote_log.insert(self.node.address, self.current_term);

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
				// debug!(?self.log);
				self.timeout = Instant::now() + HEARTBEAT;
				// Send AppendEntries heartbeats (Empty) to all other servers
				for connection in self.connections.values() {
					if connection.encode(Command::AppendEntries {
						src: self.node.address,
						term: self.current_term,
						log_item: None,
					}).is_ok() {
						trace!(?connection, "Heartbeat sent successfully");
					} else {
						trace!(?connection, "Failed to send Heartbeat");
					};
				}
			}
		}
	}

	fn process_command(&mut self, command: Command) {
		/*
		Normal Operation

		Client sends command to leader;
		Leader appends command to its log;
		Leader sends AppendEntries RPCs to followers;
		Once new entry committed:
			Leader passes command to its state machine, returns result to client.
			Leader notifies followers of committed entries in subsequent AppendEntries RPCs.
			Followers pass committed commands to their state machines.
		Crashed/slow followers?
			Leader retries RPCs until they succeed.
		Performance is optimal in common case:
			One successful RPC to any majority of servers.
		 */

		match self.current_leader {
			Some(leader) => {
				// Add log item to log
				if let Some(leader) = self.connections.get(&leader) {
					if leader.encode(Command::AppendCommand(Box::new(command.clone()))).is_ok() {
						trace!(?command, ?self.current_leader, "AppendCommand sent successfully");
					} else {
						trace!(?command, ?self.current_leader, "Failed to send AppendCommand, push to tmp log.");
						self.tmp_log.push(command);
					}
				}

			}
			None => {
				// check, maybe you are leader
				if self.state == ActorState::Leader {
					self.handle_append_command(command);
				} else {
					trace!(?command, "No current leader, push to tmp log.");
					// Store to tmp_log and process them later
					self.tmp_log.push(command);
				}
			}
		}
	}

	fn handle_append_command(&mut self, command: Command) {
		trace!(self.node.address, self.current_term, ?command, "Command written into log");
		self.log.push((self.current_term, command));

		// Send to followers
		let log_item = Some(Box::new(self.create_log_item(self.log.len() - 1)));
		for connection in self.connections.values_mut() {
			if connection.encode(Command::AppendEntries {
				src: self.node.address,
				term: self.current_term,
				log_item: log_item.clone(),
			}).is_ok() {
				trace!(?connection, "Command::AppendEntries sent successfully");
			} else {
				trace!(?connection, "Failed to send command Command::AppendEntries");
			};
		}
	}
	fn is_possible_to_do(&self, command: Command) -> bool {
		match command {
			Command::Open { account } => self.db.try_open_account(&account),
			Command::Deposit { account, amount } => self.db.try_deposit_money(&account, amount),
			Command::Withdraw { account, amount } => self.db.try_withdraw_money(&account, amount),
			Command::Transfer { src, dst, amount } => self.db.try_transfer_money(&src, &dst, amount),
			_ => {false}
		}
	}
	fn do_stuff(&mut self, command: Command) {
		match command {
			Command::Open { account } => {
				self.db.open_account(&account);
				self.node.append(&Command::Open { account });
			},
			Command::Deposit { account, amount } => {
				self.db.deposit_money(&account, amount);
				self.node.append(&Command::Deposit { account, amount });
			},
			Command::Withdraw { account, amount } => {
				self.db.withdraw_money(&account, amount);
				self.node.append(&Command::Withdraw { account, amount });
			},
			Command::Transfer { src, dst, amount } => {
				self.db.transfer_money(&src, &dst, amount);
				self.node.append(&Command::Transfer { src, dst, amount })
			},
			_ => {}
		}
	}

	fn create_log_item(&mut self, index: usize) -> LogItem {
		// Get the term of the previous entry, or 0 if out of bounds
		let previous_term = if index < self.log.len() && index > 0 {
			self.log.get(index - 1).map(|&(term, _)| term).unwrap_or(0)
		} else {
			0
		};

		LogItem {
			index,
			previous_term,
			command: self.log[index].1.clone(),
		}
	}

	fn commit(&mut self, src: usize, index: usize) {
		// Count addresses with index >= nextIndex
		let count = self.connections_index
			.iter()
			.filter(|&(_, &next_index)| match (index, next_index) {
				(index, Some(value)) => value >= index,
				_ => false,
			})
			.count();

		if count >= (self.connections.len() + 1) / 2 {
			// Update commit index to the highest confirmed index
			let new_commit_index = self.commit_index.map_or(index, |current| current.max(index));

			// Execute commands between the old and new commit indices
			if let Some(old_commit_index) = self.commit_index {
				for i in (old_commit_index + 1)..=new_commit_index {
					if let Some((_, command)) = self.log.get(i) {
						if self.is_possible_to_do(command.clone()) {
							self.do_stuff(command.clone());
						} else {
							trace!("{:?} {}", command, "Command rejected");
						}
					}
				}
			} else {
				// Handle the first-time commit
				for i in 0..=new_commit_index {
					if let Some((_, command)) = self.log.get(i) {
						if self.is_possible_to_do(command.clone()) {
							self.do_stuff(command.clone());
						} else {
							trace!("{:?} {}", command, "Command rejected");
						}
					}
				}
			}

			// Update the commit index
			self.commit_index = Some(new_commit_index);
			trace!(?self.commit_index, "Updated commit_index");

			// Notify followers of the updated commit index
			if let Some(connection) = self.connections.get(&src) {
				if connection.encode(Command::UpdateCommitIndex {
					index: self.commit_index.unwrap(),
				}).is_ok() {
					trace!(?connection, "UpdateCommitIndex sent successfully");
				} else {
					trace!(?connection, "Failed to send command UpdateCommitIndex");
				}
			}

		} else {
			trace!(?count, ?index, "Not enough followers confirmed for commit");
		}
	}

}


#[derive(Debug, Clone)]
pub struct DB {
	/// Stores account balances keyed by account names.
	account: HashMap<String, usize>,
}

impl DB {
	/// Creates a new, empty database of accounts.
	pub fn new() -> Self {
		DB {
			account: HashMap::new(),
		}
	}

	/// Opens a new account with a unique name.
	fn open_account(&mut self, account: &str) {
		self.account.entry(account.to_string()).or_insert(0);
	}

	/// Attempts to check if an account can be opened, returning `true` if it can, or `false` if the account already exists.
	fn try_open_account(&self, account: &str) -> bool {
		!self.account.contains_key(account)
	}

	/// Deposits money into an account.
	fn deposit_money(&mut self, account: &str, amount: usize) {
		if let Some(balance) = self.account.get_mut(account) {
			*balance += amount;
		}
	}

	/// Attempts to check if money can be deposited into an account, returning `true` if possible, or `false` if the account does not exist.
	fn try_deposit_money(&self, account: &str, _amount: usize) -> bool {
		!self.try_open_account(account)
	}

	/// Withdraws money from an account.
	fn withdraw_money(&mut self, account: &str, amount: usize) {
		if let Some(balance) = self.account.get_mut(account) {
			if *balance >= amount {
				*balance -= amount;
			}
		}
	}

	/// Attempts to check if money can be withdrawn from an account, returning `true` if possible, or `false` if the account does not exist or has insufficient funds.
	fn try_withdraw_money(&self, account: &str, amount: usize) -> bool {
		self.account.get(account).map_or(false, |balance| *balance >= amount)
	}

	/// Transfers money between two accounts.
	fn transfer_money(&mut self, src: &str, dst: &str, amount: usize) {
		if self.try_withdraw_money(src, amount) && self.account.contains_key(dst) {
			let src_new_balance = self.account[src] - amount;
			let dst_new_balance = self.account[dst] + amount;
			self.account.insert(src.to_string(), src_new_balance);
			self.account.insert(dst.to_string(), dst_new_balance);
		}
	}

	/// Attempts to check if money can be transferred between two accounts, returning `true` if possible, or `false` otherwise.
	fn try_transfer_money(&self, src: &str, dst: &str, amount: usize) -> bool {
		self.try_withdraw_money(src, amount) && self.account.contains_key(dst)
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
