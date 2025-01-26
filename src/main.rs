//! Implementation of the Raft Consensus Protocol for a banking application.

use std::{env::args, fs, io, thread};

use rand::prelude::*;
#[allow(unused_imports)]
use tracing::{debug, info, Level, trace, trace_span, debug_span};
use network::{Channel, daemon, NetworkNode};
use protocol::{Command, Actor, DB};

pub mod network;
pub mod protocol;

/// Creates and connects a number of branch offices for the bank.
pub fn setup_offices(office_count: usize, log_path: &str) -> io::Result<Vec<Channel<Command>>> {
	let mut channels = Vec::with_capacity(office_count);
	
	// create the log directory if needed
	fs::create_dir_all(log_path)?;
	let db = DB::new();
	// create various network nodes and start them
	for address in 0..office_count {
		let node = NetworkNode::new(address, &log_path)?;
		channels.push(node.channel());

		let mut actor = Actor::new(node, db.clone());
		
		thread::spawn(move || {
			// configure a span to associate log-entries with this network node
			let _guard = debug_span!("NetworkNode", id = actor.node.address);
			let _guard = _guard.enter();

			actor.main_loop();
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
		.with_max_level(Level::DEBUG)
		.init();
	
	// create and connect a number of offices
	let channels = setup_offices(6, &log_path)?;
	let copy = channels.clone();
	
	// activate the thread responsible for the disruption of connections
	thread::spawn(move || daemon(copy, 0.0, 1.0)); // TODO: Turn on for deploy
	
	// sample script for your convenience
	script! {
		// tell the macro which collection of channels to use
		use channels;
		
		// customer requests start with the branch office index,
		// followed by the source account name and a list of requests
		[0] "Weber"   => open(), deposit( 50);
		[5] "Weber"   => open(); // wont be logged
		[1] "Redlich" => open(), deposit(100);
		sleep();
		[2] "Redlich" => transfer("Weber", 20);
		sleep();
		[3] "Weber"   => withdraw(60);
		sleep(2);
	}
	
	Ok(())
}
