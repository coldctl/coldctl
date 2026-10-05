pub fn success(message: &str) {
    println!("✓ {message}");
}

pub fn error(message: &str) {
    eprintln!("✗ {message}");
}
// Metadata can contain quoted SQL identifiers, including terminal control characters.
pub fn text(value: &str) -> String {
    value
        .chars()
        .flat_map(|c| {
            if c.is_control() {
                c.escape_default().collect::<Vec<_>>()
            } else {
                vec![c]
            }
        })
        .collect()
}

#[derive(Clone, Copy, clap::ValueEnum)]
pub enum Output {
    Table,
    Json,
}
