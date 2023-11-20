use crossbeam::channel::{unbounded, Receiver, Sender};
use pars_libs::{parse_line, Remote, RemoteCommand};
use std::io::Write;
use std::process::Output;
use std::sync::{Arc, Mutex};
use std::{io, process::Command, thread};

// enum that determines which halt argument has been given
enum Termination {
    Never,
    Lazy,
    Eager,
}

// enum that determines what kind of halt or exit method
// will be used when a command fails
#[derive(PartialEq, Clone)]
enum ExitType {
    Ok,
    LazyExit,
    EagerExit,
}

// enum that determines if the program is in
// client mode or server mode
#[derive(PartialEq)]
enum RemoteType {
    Client,
    Server,
}

// struct that holds the information that will be passed
// to the threads
struct ThreadData {
    termination: Termination,
    remote_type: RemoteType,
    connection: Option<Remote>,
    num_threads: u32,
}

// Function that acts as the input thread, sending the commands
// through a sender to be received by a thread
fn input_thread(sender: Sender<Vec<Vec<String>>>) {
    for line in io::stdin().lines() {
        let commands = parse_line(&line.unwrap());

        if let Some(commands) = commands {
            sender.send(commands).expect("Error sending commands");
        }
    }
}

// Function that outputs the command output to stdout
fn output_commands(output_vec: &Vec<Output>) {
    for output in output_vec {
        io::stdout().write_all(&output.stdout).unwrap();
    }
}

// Function that takes in the command, and processes it, also checking if
// an exit has occured in this thread or another thread through the given channels
fn process_commands(
    commands: Vec<Vec<String>>,
    thread_data: &Arc<Mutex<ThreadData>>,
    exit_receiver: Receiver<ExitType>,
    exit_sender: Sender<ExitType>,
) -> ExitType {
    let mut exit_type: ExitType = ExitType::Ok;
    let mut output_vec: Vec<Output> = Vec::new();
    let remote: Option<Remote> = thread_data.lock().unwrap().connection.as_ref().cloned();
    for command in commands {
        // Check if an exit has occurred in another thread
        // and if it is a lazy or eager exit
        if let Ok(exit_code) = exit_receiver.try_recv() {
            if exit_code == ExitType::LazyExit || exit_code == ExitType::EagerExit {
                exit_sender.send(exit_code).expect("failed to send code");
                break;
            }
        }

        // Check if the command is a command that will run on a remote machine
        // or a local machine, and get the output of that command
        let output = if let Some(ref remote) = remote {
            RemoteCommand::remote_output(
                Command::new(command.first().unwrap()).arg(command.last().unwrap()),
                remote,
            )
            .unwrap()
        } else {
            Command::new(command.first().unwrap())
                .arg(command.last().unwrap())
                .output()
                .unwrap()
        };

        let output_clone = output.clone();

        // Check if an error has occurred with the command
        // and set the type of exit that has been provided
        if !output_clone.status.success() {
            match thread_data.lock().unwrap().termination {
                Termination::Never => {
                    break;
                }
                Termination::Lazy => {
                    exit_type = ExitType::LazyExit;
                    break;
                }
                Termination::Eager => {
                    exit_type = ExitType::EagerExit;
                    break;
                }
            }
        } else {
            output_vec.push(output_clone);
        }

        // Check if an eager halt has occurred, and stop execution
        // of the rest of the line
        if let Ok(exit_code) = exit_receiver.try_recv() {
            if exit_code == ExitType::EagerExit {
                exit_sender.send(exit_code).expect("failed to send code");
                break;
            }
        }
    }

    output_commands(&output_vec);
    exit_type
}

// This function acts as the output thread, receiving lines from the input thread, and processes the lines using
// the process_commands function
fn output_thread(
    receiver: Receiver<Vec<Vec<String>>>,
    thread_data: Arc<Mutex<ThreadData>>,
    exit_sender: Sender<ExitType>,
    exit_receiver: Receiver<ExitType>,
) {
    for commands in receiver.iter() {
        let exit_receiver_clone = exit_receiver.clone();
        let exit_sender_clone = exit_sender.clone();
        let exit_type = process_commands(
            commands,
            &thread_data,
            exit_receiver_clone,
            exit_sender_clone,
        );
        exit_sender
            .send(exit_type.clone())
            .expect("Unable to send exit code");
    }
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    // Initialise the thread data
    let mut thread_data = ThreadData {
        termination: Termination::Never,
        remote_type: RemoteType::Client,
        connection: None,
        num_threads: 0,
    };

    // check if the arguments is correct, and set the values
    // accordingly
    if args.len() < 3 {
        panic!("Error, invalid arguments");
    }
    if args.len() > 4 {
        if args[3] == "-e" || args[3] == "--halt" {
            let exit_type: String = args[4].parse().unwrap();
            if exit_type == "eager" {
                thread_data.termination = Termination::Eager;
            }
            if exit_type == "lazy" {
                thread_data.termination = Termination::Lazy;
            }
        } else {
            panic!("Error, invalid arguments");
        }
    }

    // check for -J or -r, and set the thread data
    if args[1] == "-J" {
        match args[2].parse() {
            Ok(num_threads) => {
                thread_data.num_threads = num_threads;
            }
            Err(_) => {
                panic!("Error: number of jobs is not a number");
            }
        }
    }
    if args[1] == "-r" {
        // split the string so that the address, port, number of concurrent jobs
        // is processed correctly
        let connection_string: String = args[2].parse().unwrap();
        if !connection_string.contains(':') || !connection_string.contains('/') {
            panic!("Error, remote address is not valid");
        }
        let connection_string: Vec<&str> = connection_string.split(':').collect();
        let address = connection_string[0].to_string();
        let port_num_jobs: Vec<&str> = connection_string[1].split('/').collect();
        let port = port_num_jobs[0].parse::<u16>().unwrap();
        let num_threads = port_num_jobs[1].parse::<u32>().unwrap();

        // Set the thread data with the relevant information
        thread_data.num_threads = num_threads;
        thread_data.remote_type = RemoteType::Server;
        thread_data.connection = Some(Remote {
            addr: address,
            port,
        });
    }

    // Initialise the threads, as well as the channels, and wrap the thread_data in a mutex
    let mut thread_handles: Vec<_> = Vec::new();
    let num_threads = thread_data.num_threads;
    let (command_sender, command_receiver) = unbounded::<Vec<Vec<String>>>();
    let thread_data_mutex = Arc::new(Mutex::new(thread_data));
    let (exit_sender, exit_receiver) = unbounded::<ExitType>();

    // loop through the number of threads provided, clone the channel and wrap the thread data
    // in a mutex and spawn the threads
    for _ in 0..num_threads {
        let command_receiver = command_receiver.clone();
        let thread_data_mutex = Arc::clone(&thread_data_mutex);
        let exit_sender = exit_sender.clone();
        let exit_receiver = exit_receiver.clone();
        let handle = thread::spawn(move || {
            output_thread(
                command_receiver,
                thread_data_mutex,
                exit_sender,
                exit_receiver,
            )
        });
        thread_handles.push(handle);
    }

    // start the input thread
    let sender_clone = command_sender.clone();
    let handle = thread::spawn(move || input_thread(sender_clone));
    thread_handles.push(handle);
    drop(command_sender);

    for handle in thread_handles {
        handle.join().unwrap();
    }
}
