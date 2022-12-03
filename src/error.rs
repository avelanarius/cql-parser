#[derive(Debug)]
pub struct ParsingError {
    pub errors: Vec<ParsingErrorMessage>,
    pub notes: Vec<ParsingErrorMessage>,
}

#[derive(Debug)]
pub struct ParsingErrorMessage {
    pub msg: String,

    pub position_start: usize, // inclusive
    pub position_end: usize,   // exclusive, one past the last character
}

impl ParsingError {
    pub fn print(&self, query: &str) {
        for error in &self.errors {
            eprintln!("Error at {}: {}", error.position_start, error.msg);
            eprintln!("Error at {}: {}", error.position_start, query);
            eprintln!(
                "Error at {}: {}{}{}\n",
                error.position_start,
                " ".repeat(error.position_start),
                "^",
                "-".repeat(error.position_end - error.position_start - 1)
            );
        }
        for note in &self.notes {
            eprintln!("Note at {}: {}", note.position_start, note.msg);
            eprintln!("Note at {}: {}", note.position_start, query);
            eprintln!(
                "Note at {}: {}{}{}\n",
                note.position_start,
                " ".repeat(note.position_start),
                "^",
                "-".repeat(note.position_end - note.position_start - 1)
            );
        }
    }
}
