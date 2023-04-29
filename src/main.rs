use std::io::Write;

mod tokenizer;

fn main() {
    loop {
        let expression = prompt("> ");

        match &expression[..] {
            "" => continue,
            "exit" => break,

            _ => println!("Read statement: '{}'", expression),
        }
    }
}

fn prompt(name: &str) -> String {
    let mut line = String::new();
    print!("{}", name);

    std::io::stdout().flush().unwrap();
    std::io::stdin()
        .read_line(&mut line)
        .expect("Error: Could not read a line");

    return line.trim().to_string();
}
