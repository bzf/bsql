pub fn print_table(headers: Vec<&str>, items: Vec<Vec<impl std::fmt::Display>>) {
    // Calculate the maximum width of each column
    let column_widths: Vec<usize> = headers
        .iter()
        .enumerate()
        .map(|(i, header)| {
            let max_item_width = items
                .iter()
                .map(|row| format!("{}", row[i]).len())
                .max()
                .unwrap_or(0);
            std::cmp::max(header.len(), max_item_width)
        })
        .collect();

    // Print the header row
    for (i, header) in headers.iter().enumerate() {
        print!(" {:width$} ", header, width = column_widths[i]);
        print!("| ");
    }
    println!();

    // Print the separator row
    for width in column_widths.iter() {
        print!("{:-<width$}", "", width = width + 2);
        print!("+");
    }
    println!();

    // Print the data rows
    for row in items {
        for (i, item) in row.iter().enumerate() {
            print!(" {:width$} ", item, width = column_widths[i]);
            print!("| ");
        }
        println!();
    }
}
