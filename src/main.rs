use pars_libs::{Remote, RemoteCommand, parse_line};
use std::process::{Output};
use std::{process::Command, thread, io};
use std::io::Write;
use crossbeam::channel::{unbounded, Sender, Receiver};
use std::sync::{Arc, Mutex};


enum Termination {
    Never,
    Lazy,
    Eager,
}

#[derive(PartialEq, Clone)]
enum ExitType {
    Ok,
    LazyExit,
    EagerExit,
}

#[derive(PartialEq)]
enum RemoteType {
    Client,
    Server
}

struct ThreadData {
    termination: Termination,
    remote_type: RemoteType,
    connection: Option<Remote>,
    num_threads: u32, 
}


fn input_thread(sender: Sender<Vec<Vec<String>>>) {

    for line in io::stdin().lines() {  
        let commands = parse_line(&line.unwrap());

        if let Some(commands) = commands {
            sender.send(commands).expect("Error sending commands");
        }

    }


}

fn output_commands(output_vec: &Vec<Output>) {
    for output in output_vec {
        io::stdout().write_all(&output.stdout).unwrap();
    }
}


fn process_commands(commands: Vec<Vec<String>>, thread_data: &Arc<Mutex<ThreadData>>, exit_receiver: Receiver<ExitType>,
                    exit_sender: Sender<ExitType>) -> ExitType {
    let mut exit_type: ExitType = ExitType::Ok;
    let mut output_vec: Vec<Output> = Vec::new();
    let remote: Option<Remote> = if let Some(remote) = &thread_data.lock().unwrap().connection {
        Some(remote.clone())
    } else {
        None
    };
    for command in commands {
        if let Ok(exit_code) = exit_receiver.try_recv() {
            if exit_code == ExitType::LazyExit || exit_code == ExitType::EagerExit {
                exit_sender.send(exit_code).expect("failed to send code");
                break;
            }
        }

        let output = if let Some(ref remote) = remote {
            RemoteCommand::remote_output(Command::new(command.first().as_deref().unwrap())
            .arg(command.last().as_deref().unwrap()), remote).unwrap()
        } else {
            Command::new(command.first().as_deref().unwrap())
            .arg(command.last().as_deref().unwrap())
            .output().unwrap()
        };

        let output_clone = output.clone();
        
        if !output_clone.status.success() {
            match thread_data.lock().unwrap().termination {
                Termination::Never => {
                    break;
                },
                Termination::Lazy => {
                    exit_type = ExitType::LazyExit;
                    break;
                },
                Termination::Eager => {
                    exit_type = ExitType::EagerExit;
                    break;
                } 
            }
        } else {
            output_vec.push(output_clone);

        }

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

fn output_thread(receiver: Receiver<Vec<Vec<String>>>, thread_data: Arc<Mutex<ThreadData>>, exit_sender: Sender<ExitType>, exit_receiver: Receiver<ExitType>) {
    for commands in receiver.iter() {
        let exit_receiver_clone = exit_receiver.clone();
        let exit_sender_clone = exit_sender.clone();
        let exit_type = process_commands(commands, &thread_data, exit_receiver_clone, exit_sender_clone);
        exit_sender.send(exit_type.clone()).expect("Unable to send exit code");
    }
 
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let mut thread_data = ThreadData {
        termination: Termination::Never,
        remote_type: RemoteType::Client,
        connection: None,
        num_threads: 0,
    };

    if args.len() < 3 {
        eprintln!("Error, invalid arguments");
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
            eprintln!("Error, invalid arguments");
        }
    }
    if args[1] == "-J" {
        let num_threads: u32 = args[2].parse().unwrap();
        thread_data.num_threads = num_threads;
    }
    if args[1] == "-r" {
        println!("-r detected");
        let connection_string: String = args[2].parse().unwrap();
        let connection_string: Vec<&str> = connection_string.split(":").collect();
        let address = connection_string[0].to_string();
        let port_num_jobs: Vec<&str> = connection_string[1].split("/").collect();
        let port = port_num_jobs[0].parse::<u16>().unwrap();
        let num_threads = port_num_jobs[1].parse::<u32>().unwrap();
        println!("{:?} {:?} {:?}", address, port, num_threads);
        thread_data.num_threads = num_threads;
        thread_data.remote_type = RemoteType::Server;
        thread_data.connection =  Some(Remote {
            addr: address,
            port: port,
        });
    }

    let mut thread_handles: Vec<_> = Vec::new();
    let num_threads = thread_data.num_threads;
    let (command_sender, command_receiver) = unbounded::<Vec<Vec<String>>>();
    let thread_data_mutex = Arc::new(Mutex::new(thread_data));
    let (exit_sender, exit_receiver) = unbounded::<ExitType>();

    for _ in 0..num_threads {
        let command_receiver= command_receiver.clone();
        let thread_data_mutex = Arc::clone(&thread_data_mutex);
        let exit_sender = exit_sender.clone();
        let exit_receiver = exit_receiver.clone();
        let handle = thread::spawn(move || output_thread(command_receiver, thread_data_mutex, exit_sender, exit_receiver));
        thread_handles.push(handle);
    } 
    
    let sender_clone = command_sender.clone();
    let handle = thread::spawn(move || input_thread(sender_clone));
    thread_handles.push(handle);
    drop(command_sender); 
   
    for handle in thread_handles {
        handle.join().unwrap();
    }

}
