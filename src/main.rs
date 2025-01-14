//! Implementation of the Raft Consensus Protocol for a banking application.

use std::{env::args, fs, io, thread};

use rand::prelude::*;
#[allow(unused_imports)]
use tracing::{debug, info, Level, trace, trace_span};

use network::{Channel, daemon, NetworkNode};
use protocol::Command;

pub mod network;
pub mod protocol;

/// Creates and connects a number of branch offices for the bank.
pub fn setup_offices(office_count: usize, log_path: &str) -> io::Result<Vec<Channel<Command>>> {
	let mut channels = Vec::with_capacity(office_count);
	
	// create the log directory if needed
	fs::create_dir_all(log_path)?;
	
	// create various network nodes and start them
	for address in 0..office_count {
		let node = NetworkNode::new(address, &log_path)?;
		channels.push(node.channel());
		
		thread::spawn(move || {
			// configure a span to associate log-entries with this network node
			let _guard = trace_span!("NetworkNode", id = node.address);
			let _guard = _guard.enter();
			
			// dispatching event loop
			while let Ok(cmd) = node.decode(None) {
				match cmd {
					// customer requests
					Command::Open { account } => {
						debug!("request to open an account for {:?}", account);
					}
					Command::Deposit { account, amount } => {
						debug!(amount, ?account, "request to deposit");
					}
					Command::Withdraw { account, amount } => {
						debug!(amount, ?account, "request to withdraw");
					}
					Command::Transfer { src, dst, amount } => {
						debug!(amount, ?src, ?dst, "request to transfer");
					}
					
					// control messages
					Command::Accept(channel) => {
						trace!(origin = channel.address, "accepted connection");
					}
				}
			}
		});
	}
	
	// connect the network nodes in random order
	let mut rng = thread_rng();
	for src in channels.iter() {
		for dst in channels.iter().choose_multiple(&mut rng, office_count) {
			if src.address == dst.address { continue; }
			src.send(Command::Accept(dst.clone()));
		}
	}
	
	Ok(channels)
}

fn main() -> io::Result<()> {
	use tracing_subscriber::{FmtSubscriber, fmt::time::ChronoLocal};
	let log_path = args().nth(1).unwrap_or("logs".to_string());
	
	// initialize the tracer
	FmtSubscriber::builder()
		.with_timer(ChronoLocal::new("[%Mm %Ss]".to_string()))
		.with_max_level(Level::TRACE)
		.init();
	
	// create and connect a number of offices
	let channels = setup_offices(6, &log_path)?;
	let copy = channels.clone();
	
	// activate the thread responsible for the disruption of connections
	thread::spawn(move || daemon(copy, 1.0, 1.0));
	
	// sample script for your convenience
	script! {
		// tell the macro which collection of channels to use
		use channels;
		
		// customer requests start with the branch office index,
		// followed by the source account name and a list of requests
		[0] "Weber"   => open(), deposit( 50);
		[1] "Redlich" => open(), deposit(100);
		sleep();
		[2] "Redlich" => transfer("Weber", 20);
		sleep();
		[3] "Weber"   => withdraw(60);
		sleep(2);
	}
	
	Ok(())
}
