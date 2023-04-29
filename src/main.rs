use std::io::Write;

mod data_type;
mod literal_value;
mod parser;
mod tokenizer;

fn main() {
    loop {
        let expression = prompt("> ");

        match &expression[..] {
            "" => continue,
            "exit" => break,

            _ => {
                let parse_result = parser::parse(&expression);
                println!("parse_result: '{:#?}'", parse_result);
            }
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
