use std::fs;
use std::io::Write;

mod error;
mod lexer;
mod parser;

use crate::lexer::{lex, Token, TokenInfo};
use crate::parser::{parse_select, ParserState};
use rand::seq::SliceRandom; // 0.6.5

use rand::{thread_rng, Rng};
use rand::prelude::*;

/*
 Example bad error message from ANTLR3:

 CREATE KEYSAPCE ks WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1};
    SyntaxException: line 1:0 no viable alternative at input 'CREATE'

  (should be something like: typo in KEYSAPCE, CREATE can only CREATE KEYSPACE/CREATE TABLE etc.)
*/

use std::io::{self, BufRead};

fn main() {
    // print!("piotrg-cql-parser> ");
    // io::stdout().flush().unwrap();

    // let stdin = io::stdin();
    // let query = stdin.lock().lines().next().unwrap().unwrap();

    // let lexed = lex(&query);
    // match lexed {
    //     Ok(lexed) => {
    //         let mut parser_state = ParserState {
    //             tokens: &lexed,
    //             current_position: 0,
    //         };
    //         let parsed = parse_select(&mut parser_state);
    //         match parsed {
    //             Ok(parsed) => {
    //                 println!("Parsing success: \n{:#?}", parsed);
    //             }
    //             Err(err) => {
    //                 err.print(&query);
    //             }
    //         }
    //     },
    //     Err(err) => {
    //         err.print(&query);
    //     }
    // }

    let correct_queries = fs::read_to_string("correct_queries.txt").unwrap();
    let correct_queries = correct_queries.split("\n[QUERIES_SEPARATOR]\n");
    let correct_queries = correct_queries.filter(|q| q.len() < 4000);
    let correct_queries = correct_queries.filter(|q| q.trim().to_lowercase().starts_with("select"));
    let correct_queries = correct_queries.take(50000);

    let mut lex_successes = 0;
    let mut lex_errors = 0;

    let mut parse_successes = 0;
    let mut parse_errors = 0;

    let mut parse_error_examples = Vec::new();
    let mut parse_successes_examples = Vec::new();

    for correct_query in correct_queries {
        // println!("Lexing '{:.100}'...", correct_query);
        let tokens = lex(correct_query);
        match tokens {
            Ok(tokens) => {
                // for token in &tokens {
                //     println!("{}", token)
                // }
                lex_successes += 1;

                // println!("Parsing '{:.100}'...", correct_query);
                let mut parser_state = ParserState {
                    tokens: &tokens,
                    current_position: 0,
                };
                let parsed = parse_select(&mut parser_state);

                while matches!(parser_state.peek_token().info, TokenInfo::Semicolon) {
                    parser_state.eat_token();
                }

                match (parsed, parser_state.finished()) {
                    (Ok(parsed), true) => {
                        // println!("Parsed '{}'\n {}", correct_query, parsed);
                        parse_successes_examples.push(correct_query);
                        parse_successes += 1;
                    }
                    _ => {
                        parse_error_examples.push(correct_query);
                        parse_errors += 1;
                    }
                }
            }
            Err(_) => {
                lex_errors += 1;
            }
        }
        // println!("=============");
    }

    println!(
        "[LEX]   Successes: {}, Errors: {}",
        lex_successes, lex_errors
    );
    println!(
        "[PARSE] Successes: {}, Errors: {}",
        parse_successes, parse_errors
    );

    parse_error_examples.sort_by(|a, b| a.len().partial_cmp(&b.len()).unwrap());
    for example in parse_error_examples
        .iter()
        .filter(|x| !x.to_lowercase().contains("count"))
        .take(40)
    {
        println!("Example parse error: {}", example);
        let lexed = lex(&example);
        let mut ps = ParserState {
            tokens: &lexed.unwrap(),
            current_position: 0,
        };
        let parsed = parse_select(&mut ps);
        match parsed.err() {
            Some(x) => {
                x.print(&example);
            }
            _ => {}
        }
    }

    thread_rng().shuffle(&mut parse_successes_examples);
    // parse_successes_examples.sort_by(|b, a| (a.len() ^ b.len()).partial_cmp(&b.len()).unwrap());
    for example in parse_successes_examples.iter().take(10) {
        println!("Example parse success: {}", example);
    }
}
