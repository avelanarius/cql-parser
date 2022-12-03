use crate::error::{ParsingError, ParsingErrorMessage};
use crate::lexer::{to_column_identifier, Token, TokenInfo};

pub struct ParserState<'a> {
    pub tokens: &'a Vec<Token>,
    pub current_position: usize,
}

impl<'a> ParserState<'a> {
    pub fn peek_token(&self) -> &'a Token {
        self.peek_token_offset(0)
    }

    fn peek_token_offset(&self, offset: usize) -> &'a Token {
        self.tokens
            .get(self.current_position + offset)
            .unwrap_or(self.tokens.last().unwrap())
    }

    fn peek_previous_token(&self) -> &'a Token {
        self.tokens
            .get(self.current_position - 1)
            .unwrap_or(self.tokens.first().unwrap())
    }

    pub fn eat_token(&mut self) -> &'a Token {
        let result = self.peek_token();
        self.current_position += 1;
        result
    }

    pub fn finished(&self) -> bool {
        matches!(self.peek_token().info, TokenInfo::Eof)
    }
}

#[derive(Debug)]
pub struct SelectQuery {
    selectors: Selectors,
    selectors_start_offset: usize,
    selectors_end_offset: usize,

    json: bool,
    distinct: bool,
    table_name: TableName,
    where_restrictions: Vec<Relation>,
    group_by: Option<Vec<String>>,
    order_by: Option<Vec<OrderByClause>>,
    per_partition_limit_value: Option<String>,
    limit_value: Option<String>,
    allow_filtering: bool,
    bypass_cache: bool,
}

#[derive(Debug)]
enum OrderByClause {
    TableOrder(String),
    Desc(String),
    Asc(String),
}

#[derive(Debug)]
enum Relation {
    EqualsRelation(RelationLHS, Term),
    LikeRelation(RelationLHS, Term),
    ContainsRelation(RelationLHS, Term),
    ContainsKeyRelation(RelationLHS, Term),
    InRelation(RelationLHS, Vec<Term>),
}

#[derive(Debug)]
enum RelationLHS {
    Identifier(String),
    Subscript(String, Term),
    TupleOfIdentifiers(Vec<String>),
}

#[derive(Debug)]
enum Term {
    Number(String),
    QuotedIdentifier(String),
    List(Vec<Term>),
    Boolean(bool),
    Null,
}

#[derive(Debug)]
enum Selectors {
    Star,
    Selectors(Vec<Selector>),
}

#[derive(Debug)]
struct Selector {
    selector_kind: SelectorKind,
    selector_start_offset: usize,
    selector_end_offset: usize,
}

#[derive(Debug)]
enum SelectorKind {
    Identifier(String),
    FunctionCall(Option<String>, String, Vec<Selector>), // keyspace.function(selectors)
    Count,                                               // COUNT(*) or COUNT(1)
    FieldAccessor(Box<Selector>, String),                // for example for UDTs: udt_value.field
    Cast(Box<Selector>, String), // CAST(selector AS type), TODO: type shouldn't be a String
}

#[derive(Debug)]
enum TableName {
    Table(String),
    KeyspaceTable(String, String),
}

pub fn parse_select(parser_state: &mut ParserState) -> Result<SelectQuery, ParsingError> {
    let select_token = parser_state.eat_token();
    if !matches!(select_token.info, TokenInfo::Select) {
        return Err(ParsingError {
            errors: vec![ParsingErrorMessage {
                msg: format!("While parsing a SELECT statement, expected 'SELECT' at the start of the statement. Instead got: '{}'.", select_token.info),
                position_start: select_token.position_start,
                position_end: select_token.position_end,
            }],
            notes: Vec::new()
        });
    }

    let mut json = false;
    if let TokenInfo::Json = parser_state.peek_token().info {
        parser_state.eat_token(); // Eat 'JSON'
        json = true;
    }

    let mut distinct = false;
    if let TokenInfo::Distinct = parser_state.peek_token().info {
        parser_state.eat_token(); // Eat 'DISTINCT'
        distinct = true;
    }

    let selectors_start_offset = parser_state.peek_token().position_start;
    let selectors = parse_selectors(parser_state)?;
    let selectors_end_offset = parser_state.peek_previous_token().position_end;

    let from_token = parser_state.eat_token();
    if !matches!(from_token.info, TokenInfo::From) {
        let mut notes = vec![ParsingErrorMessage {
            msg: "This is the entire parsed selector list.".to_string(),
            position_start: selectors_start_offset,
            position_end: selectors_end_offset,
        }];
        if let Selectors::Selectors(selectors) = selectors {
            if let Some(last_selector) = selectors.last() {
                let did_you_intend = if matches!(from_token.info, TokenInfo::From) {
                    "Did you intend to put a ',' after it?"
                } else {
                    ""
                };
                notes.push(ParsingErrorMessage {
                    msg: format!(
                        "This is the last successfully parsed selector. {}",
                        did_you_intend
                    ),
                    position_start: last_selector.selector_start_offset,
                    position_end: last_selector.selector_end_offset,
                });
            }
        }
        return Err(ParsingError {
            errors: vec![ParsingErrorMessage {
                msg: format!("While parsing a SELECT statement, expected 'FROM' after the selector list (for example 'SELECT a, b, c FROM ...'). Instead got: '{}'.", from_token.info),
                position_start: from_token.position_start,
                position_end: from_token.position_end,
            }],
            notes
        });
    }

    let table_name = parse_table_name(parser_state)?;

    let mut where_restrictions = Vec::new();
    if let TokenInfo::Where = parser_state.peek_token().info {
        parser_state.eat_token(); // Eat 'WHERE'

        let relation1 = parse_relation(parser_state)?;
        where_restrictions.push(relation1);

        while let TokenInfo::And = parser_state.peek_token().info {
            parser_state.eat_token(); // Eat 'AND'
            let relation_i = parse_relation(parser_state)?;
            where_restrictions.push(relation_i);
        }
    }

    // GROUP BY
    let mut group_by = None;
    if let (TokenInfo::Group, TokenInfo::By) = (
        &parser_state.peek_token_offset(0).info,
        &parser_state.peek_token_offset(1).info,
    ) {
        parser_state.eat_token(); // Eat 'GROUP'
        parser_state.eat_token(); // Eat 'BY'

        let mut group_by_identifiers = Vec::new();

        let identifier1 = parser_state.eat_token();
        if let TokenInfo::Identifier(identifier1) = &identifier1.info {
            group_by_identifiers.push(identifier1.clone());
        } else {
            return Err(ParsingError {
                errors: vec![ParsingErrorMessage {
                    msg: format!("While parsing a 'GROUP BY' part of a SELECT statement, expected an identifier at the start of 'GROUP BY' list (at position 1 of the list). Instead got: '{}'.", identifier1.info),
                    position_start: identifier1.position_start,
                    position_end: identifier1.position_end,
                }],
                notes: Vec::new()
            });
        }

        let mut i = 1;
        while let TokenInfo::Comma = parser_state.peek_token().info {
            i += 1;

            parser_state.eat_token(); // Eat ','

            let identifier_i = parser_state.eat_token();
            if let TokenInfo::Identifier(identifier_i) = &identifier_i.info {
                group_by_identifiers.push(identifier_i.clone());
            } else {
                return Err(ParsingError {
                    errors: vec![ParsingErrorMessage {
                        msg: format!("While parsing a 'GROUP BY' part of a SELECT statement, expected an identifier in the 'GROUP BY' list (at position {} of the list). Instead got: '{}'.", i, identifier_i.info),
                        position_start: identifier_i.position_start,
                        position_end: identifier_i.position_end,
                    }],
                    // Note: previous identifer was...
                    notes: Vec::new()
                });
            }
        }

        group_by = Some(group_by_identifiers);
    }

    // ORDER BY
    let mut order_by = None;
    if let (TokenInfo::Order, TokenInfo::By) = (
        &parser_state.peek_token_offset(0).info,
        &parser_state.peek_token_offset(1).info,
    ) {
        parser_state.eat_token(); // Eat 'ORDER'
        parser_state.eat_token(); // Eat 'BY'

        let mut order_by_clauses = Vec::new();

        let order_by_clause1 = parse_order_by_clause(parser_state)?;
        order_by_clauses.push(order_by_clause1);

        while let TokenInfo::Comma = parser_state.peek_token().info {
            parser_state.eat_token(); // Eat ','

            let order_by_clause_i = parse_order_by_clause(parser_state)?;
            order_by_clauses.push(order_by_clause_i);
        }

        order_by = Some(order_by_clauses);
    }

    // PER PARTITION LIMIT
    let mut per_partition_limit_value = None;
    if let (TokenInfo::Per, TokenInfo::Partition, TokenInfo::Limit) = (
        &parser_state.peek_token_offset(0).info,
        &parser_state.peek_token_offset(1).info,
        &parser_state.peek_token_offset(2).info,
    ) {
        parser_state.eat_token(); // Eat 'PER'
        parser_state.eat_token(); // Eat 'PARTITION'
        parser_state.eat_token(); // Eat 'LIMIT'

        let per_partition_limit_token = parser_state.eat_token();
        if let TokenInfo::Number(n) = &per_partition_limit_token.info {
            per_partition_limit_value = Some(n.clone());
        } else {
            return Err(ParsingError {
                errors: vec![ParsingErrorMessage {
                    msg: format!("While parsing a 'PER PARTITION LIMIT' part of a SELECT statement, expected a number after 'LIMIT'. Instead got: '{}'.", per_partition_limit_token.info),
                    position_start: per_partition_limit_token.position_start,
                    position_end: per_partition_limit_token.position_end,
                }],
                notes: Vec::new()
            });
        }
    }

    let mut limit_value = None;
    if let TokenInfo::Limit = parser_state.peek_token().info {
        // LIMIT n
        parser_state.eat_token(); // Eat 'LIMIT'

        let limit_value_token = parser_state.eat_token();
        if let TokenInfo::Number(n) = &limit_value_token.info {
            limit_value = Some(n.clone());
        } else {
            return Err(ParsingError {
                errors: vec![ParsingErrorMessage {
                    msg: format!("While parsing a 'LIMIT' part of a SELECT statement, expected a number after 'LIMIT'. Instead got: '{}'.", limit_value_token.info),
                    position_start: limit_value_token.position_start,
                    position_end: limit_value_token.position_end,
                }],
                notes: Vec::new()
            });
        }
    }

    // ALLOW FILTERING
    let mut allow_filtering = false;
    if let (TokenInfo::Allow, TokenInfo::Filtering) = (
        &parser_state.peek_token_offset(0).info,
        &parser_state.peek_token_offset(1).info,
    ) {
        parser_state.eat_token(); // Eat 'ALLOW'
        parser_state.eat_token(); // Eat 'FILTERING'

        allow_filtering = true;
    }

    // BYPASS CACHE
    let mut bypass_cache = false;
    if let (TokenInfo::Bypass, TokenInfo::Cache) = (
        &parser_state.peek_token_offset(0).info,
        &parser_state.peek_token_offset(1).info,
    ) {
        parser_state.eat_token(); // Eat 'BYPASS'
        parser_state.eat_token(); // Eat 'CACHE'

        bypass_cache = true;
    }

    Ok(SelectQuery {
        selectors_start_offset,
        selectors,
        selectors_end_offset,
        json,
        distinct,
        table_name,
        where_restrictions,
        group_by,
        order_by,
        per_partition_limit_value,
        limit_value,
        allow_filtering,
        bypass_cache,
    })
}

fn parse_selectors(parser_state: &mut ParserState) -> Result<Selectors, ParsingError> {
    // selectClause in Cql.g

    // Either '*'...
    let possible_star_token = parser_state.peek_token();
    if matches!(possible_star_token.info, TokenInfo::Star) {
        parser_state.eat_token(); // Eat '*'
        return Ok(Selectors::Star);
    }

    // ...or selector1, selector2, selector3, ...
    let mut selectors = Vec::new();
    let selector1 = parse_selector(parser_state)?;
    selectors.push(selector1);

    while let TokenInfo::Comma = parser_state.peek_token().info {
        parser_state.eat_token(); // Eat ','
        let selector_i = parse_selector(parser_state)?;
        selectors.push(selector_i);
    }

    Ok(Selectors::Selectors(selectors))
}

fn parse_selector(parser_state: &mut ParserState) -> Result<Selector, ParsingError> {
    // unaliasedSelector in Cql.g, with field_selection
    let mut result = parse_selector_inner(parser_state)?;

    while let (TokenInfo::Dot, TokenInfo::Identifier(ident)) = (
        &parser_state.peek_token_offset(0).info,
        &parser_state.peek_token_offset(1).info,
    ) {
        parser_state.eat_token(); // Eat '.'
        parser_state.eat_token(); // Eat identifier

        let previous_selector_start_offset = result.selector_start_offset;
        result = Selector {
            selector_kind: SelectorKind::FieldAccessor(Box::new(result), ident.clone()),
            selector_start_offset: previous_selector_start_offset,
            selector_end_offset: parser_state.peek_previous_token().position_end,
        };
    }

    Ok(result)
}

fn parse_selector_inner(parser_state: &mut ParserState) -> Result<Selector, ParsingError> {
    // unaliasedSelector in Cql.g, without field_selection
    let selector_start_offset = parser_state.peek_token().position_start;

    if let TokenInfo::ParenthesesStart = parser_state.peek_token_offset(1).info {
        // Function call (for example: sum(x))
        parse_selector_function_call(parser_state, None)
    } else if let (
        TokenInfo::Identifier(keyspace_name),
        TokenInfo::Dot,
        TokenInfo::Identifier(function_name),
        TokenInfo::ParenthesesStart,
    ) = (
        &parser_state.peek_token_offset(0).info,
        &parser_state.peek_token_offset(1).info,
        &parser_state.peek_token_offset(2).info,
        &parser_state.peek_token_offset(3).info,
    ) {
        // Function call (for example: keyspace.sum(x))
        parser_state.eat_token(); // Eat 'keyspace'
        parser_state.eat_token(); // Eat '.'

        parse_selector_function_call(parser_state, Some(keyspace_name.clone()))
        // FIXME: keyspace token info (location) not passed below
    } else {
        // Ordinary identifier
        let identifier = parser_state.eat_token();
        if let TokenInfo::Identifier(identifier) = to_column_identifier(&identifier.info) {
            Ok(Selector {
                selector_kind: SelectorKind::Identifier(identifier.to_string()),
                selector_start_offset,
                selector_end_offset: parser_state.peek_previous_token().position_end,
            })
        } else {
            return Err(ParsingError {
                errors: vec![ParsingErrorMessage {
                    msg: format!("While parsing a list of selectors of a SELECT statement, expected a valid selector (for example a column name). Instead got: '{}'.", identifier.info),
                    position_start: identifier.position_start,
                    position_end: identifier.position_end,
                }],
                // TODO: Previous selector? a bit hard?
                notes: Vec::new()
            });
        }
    }
}

fn parse_selector_function_call(
    parser_state: &mut ParserState,
    keyspace_name: Option<String>,
) -> Result<Selector, ParsingError> {
    let function_name = &parser_state.eat_token();
    let selector_start_offset = function_name.position_start;

    match &function_name.info {
        TokenInfo::Count => {
            // COUNT(*) or COUNT(1)
            let paren_start_token = parser_state.eat_token();
            if !matches!(paren_start_token.info, TokenInfo::ParenthesesStart) {
                return Err(ParsingError {
                    errors: vec![ParsingErrorMessage {
                        msg: format!("While parsing the 'COUNT' selector (for example 'COUNT(*)'), expected an opening parenthesis after the 'COUNT' keyword. Instead got: '{}'.", paren_start_token.info),
                        position_start: paren_start_token.position_start,
                        position_end: paren_start_token.position_end,
                    }],
                    notes: Vec::new()
                });
            }

            let count_arg_token = parser_state.eat_token();
            match &count_arg_token.info {
                TokenInfo::Star => {}
                TokenInfo::Number(n) if n == "1" => {}
                _ => {
                    return Err(ParsingError {
                        errors: vec![ParsingErrorMessage {
                            msg: format!("While parsing the 'COUNT' selector (for example 'COUNT(*)'), expected '1' or '*' inside 'COUNT' argument list. Instead got: '{}' as an argument.", count_arg_token.info),
                            position_start: count_arg_token.position_start,
                            position_end: count_arg_token.position_end,
                        }],
                        notes: Vec::new()
                    });
                }
            }

            let paren_end_token = parser_state.eat_token();
            if !matches!(paren_end_token.info, TokenInfo::ParenthesesEnd) {
                return Err(ParsingError {
                    errors: vec![ParsingErrorMessage {
                        msg: format!("While parsing the 'COUNT' selector (for example 'COUNT(*)'), expected an ending parenthesis after the 'COUNT' argument list. Instead got: '{}'.", paren_end_token.info),
                        position_start: paren_end_token.position_start,
                        position_end: paren_end_token.position_end,
                    }],
                    // TODO: where was the opening '('?
                    notes: Vec::new()
                });
            }

            Ok(Selector { selector_kind: SelectorKind::Count, selector_start_offset, selector_end_offset: parser_state.peek_previous_token().position_end })
        }
        TokenInfo::Cast => {
            let paren_start_token = parser_state.eat_token();
            if !matches!(paren_start_token.info, TokenInfo::ParenthesesStart) {
                return Err(ParsingError {
                    errors: vec![ParsingErrorMessage {
                        msg: format!("While parsing a 'CAST' selector (for example 'CAST(a AS int)'), expected an opening parenthesis after the 'CAST' keyword. Instead got: '{}'.", paren_start_token.info),
                        position_start: paren_start_token.position_start,
                        position_end: paren_start_token.position_end,
                    }],
                    // TODO: Note: where was the function name 
                    notes: Vec::new()
                });
            }

            let selector_start_offset = parser_state.peek_token().position_start;
            let selector = parse_selector(parser_state)?;
            let selector_end_offset = parser_state.peek_previous_token().position_end;

            let selector = Box::new(selector);

            let as_token = parser_state.eat_token();
            if !matches!(as_token.info, TokenInfo::As) {
                return Err(ParsingError {
                    errors: vec![ParsingErrorMessage {
                        msg: format!("While parsing a 'CAST' selector (for example 'CAST(a AS int)'), expected 'AS' keyword after the expression that is being cast. Instead got: '{}'.", as_token.info),
                        position_start: as_token.position_start,
                        position_end: as_token.position_end,
                    }],
                    // TODO: Note: what is the selector?
                    notes: vec![ParsingErrorMessage {
                        msg: "The 'CAST' started here.".to_string(),
                        position_start: function_name.position_start,
                        position_end: function_name.position_end,
                    }, ParsingErrorMessage {
                        msg: "This is the expression that is being cast (the 'expression' as in 'CAST(expression AS int)'). Try putting 'AS' after it.".to_string(),
                        position_start: selector_start_offset,
                        position_end: selector_end_offset,
                    }]
                });
            }

            let type_token = parser_state.eat_token();
            if let TokenInfo::Identifier(ident) = &type_token.info {
                let paren_end_token = parser_state.eat_token();
                if !matches!(paren_end_token.info, TokenInfo::ParenthesesEnd) {
                    return Err(ParsingError {
                        errors: vec![ParsingErrorMessage {
                            msg: format!("While parsing a 'CAST' selector (for example 'CAST(a AS int)'), expected a closing parenthesis ')' after the type name. Instead got: '{}'.", paren_end_token.info),
                            position_start: paren_end_token.position_start,
                            position_end: paren_end_token.position_end,
                        }],
                        // TODO: Note: where was the function name 
                        notes: vec![
                            ParsingErrorMessage {
                                msg: "The type name was here. Did you intend to put ')' after it?".to_string(),
                                position_start: type_token.position_start,
                                position_end: type_token.position_end,
                            },
                            ParsingErrorMessage {
                                msg: "The parentheses started here.".to_string(),
                                position_start: paren_start_token.position_start,
                                position_end: paren_start_token.position_end,
                            }
                        ]
                    });
                }

                Ok(Selector {
                    selector_kind: SelectorKind::Cast(selector, ident.clone()),
                    selector_start_offset,
                    selector_end_offset: parser_state.peek_previous_token().position_end,
                })
            } else {
                Err(ParsingError {
                    errors: vec![ParsingErrorMessage {
                        msg: format!("While parsing a 'CAST' selector (for example 'CAST(a AS int)'), expected a valid type (such as 'int') after the 'AS' keyword. Instead got: '{}'.", type_token.info),
                        position_start: type_token.position_start,
                        position_end: type_token.position_end,
                    }],
                    // TODO: Note: what is the selector?
                    notes: Vec::new()
                })
            }
        }
        TokenInfo::Identifier(ident) => {
            let paren_start_token = parser_state.eat_token();
            if !matches!(paren_start_token.info, TokenInfo::ParenthesesStart) {
                return Err(ParsingError {
                    errors: vec![ParsingErrorMessage {
                        msg: format!("While parsing a function selector (for example 'SUM(a)' or 'my_udf(b)'), expected an opening parenthesis after the function name. Instead got: '{}'.", paren_start_token.info),
                        position_start: paren_start_token.position_start,
                        position_end: paren_start_token.position_end,
                    }],
                    // TODO: Note: where was the function name 
                    notes: Vec::new()
                });
            }

            let mut arguments = Vec::new();

            // 3 cases:
            //   1. function_name()
            //   2. function_name(arg1)
            //   3. function_name(arg1, arg2, arg3, ..., argX)

            if let TokenInfo::ParenthesesEnd = parser_state.peek_token().info {
                // Case 1
            } else {
                let argument1 = parse_selector(parser_state)?;
                arguments.push(argument1);

                while matches!(parser_state.peek_token().info, TokenInfo::Comma) {
                    parser_state.eat_token(); // Eat ','

                    let argument_i = parse_selector(parser_state)?;
                    arguments.push(argument_i);
                }
            }

            let paren_end_token = parser_state.eat_token();
            if !matches!(paren_end_token.info, TokenInfo::ParenthesesEnd) {
                return Err(ParsingError {
                    errors: vec![ParsingErrorMessage {
                        msg: format!("While parsing a function selector (for example 'SUM(a)' or 'my_udf(b)'), expected a closing parenthesis after the function argument list. Instead got: '{}'.", paren_end_token.info),
                        position_start: paren_end_token.position_start,
                        position_end: paren_end_token.position_end,
                    }],
                    // TODO: Note: where was the function name, opening '('
                    notes: Vec::new()
                });
            }

            Ok(Selector {
                selector_kind: SelectorKind::FunctionCall(keyspace_name, ident.clone(), arguments),
                selector_start_offset,
                selector_end_offset: parser_state.peek_previous_token().position_end
            })
        }
        _ => Err(ParsingError {
            errors: vec![ParsingErrorMessage {
                msg: format!("While parsing a function selector (for example 'SUM(a)' or 'my_udf(b)'), expected a valid function identifier. Instead got: '{}'.", function_name.info),
                position_start: function_name.position_start,
                position_end: function_name.position_end,
            }],
            notes: Vec::new()
        }),
    }
}

fn parse_table_name(parser_state: &mut ParserState) -> Result<TableName, ParsingError> {
    // Detect if it's 'keyspace.table' or just 'table'
    let probable_dot = parser_state.peek_token_offset(1);
    if matches!(probable_dot.info, TokenInfo::Dot) {
        // 'keyspace.table'
        let keyspace_name = parser_state.eat_token();
        if let TokenInfo::Identifier(keyspace_name) = &keyspace_name.info {
            parser_state.eat_token(); // Eat '.'

            let table_name = parser_state.eat_token();

            if let TokenInfo::Identifier(table_name) = &table_name.info {
                Ok(TableName::KeyspaceTable(
                    keyspace_name.to_string(),
                    table_name.to_string(),
                ))
            } else {
                Err(ParsingError {
                    errors: vec![ParsingErrorMessage {
                        msg: format!("While parsing a table name (of form 'keyspace.table'), expected a table name (identifier) after '.'. Instead got: '{}'.", table_name.info),
                        position_start: table_name.position_start,
                        position_end: table_name.position_end,
                    }],
                    // TODO: Note: print parsed keyspace
                    notes: Vec::new()
                })
            }
        } else {
            Err(ParsingError {
                errors: vec![ParsingErrorMessage {
                    msg: format!("While parsing a table name (of form 'keyspace.table'), expected a keyspace name (identifier) before '.' and table name. Instead got: '{}'.", keyspace_name.info),
                    position_start: keyspace_name.position_start,
                    position_end: keyspace_name.position_end,
                }],
                notes: Vec::new()
            })
        }
    } else {
        let table_name = parser_state.eat_token();

        if let TokenInfo::Identifier(table_name) = &table_name.info {
            Ok(TableName::Table(table_name.to_string()))
        } else {
            Err(ParsingError {
                errors: vec![ParsingErrorMessage {
                    msg: format!("While parsing a table name (of form 'table' without explicit keyspace name), expected a table name (identifier). Instead got: '{}'.", table_name.info),
                    position_start: table_name.position_start,
                    position_end: table_name.position_end,
                }],
                notes: Vec::new()
            })
        }
    }
}

fn parse_relation(parser_state: &mut ParserState) -> Result<Relation, ParsingError> {
    let relation_lhs = parse_relation_lhs(parser_state)?;

    let relation_operator = parser_state.eat_token();
    match relation_operator.info {
        TokenInfo::Equals => {
            let term = parse_term(parser_state)?;
            Ok(Relation::EqualsRelation(relation_lhs, term))
        }
        TokenInfo::Like => {
            let term = parse_term(parser_state)?;
            Ok(Relation::LikeRelation(relation_lhs, term))
        }
        TokenInfo::Contains => {
            if matches!(parser_state.peek_token().info, TokenInfo::Key) {
                // CONTAINS KEY
                parser_state.eat_token(); // Eat 'KEY'

                let term = parse_term(parser_state)?;
                Ok(Relation::ContainsKeyRelation(relation_lhs, term))    
            } else {
                let term = parse_term(parser_state)?;
                Ok(Relation::ContainsRelation(relation_lhs, term))    
            }
        }
        TokenInfo::In => {
            let paren_start_token = parser_state.eat_token();
            if !matches!(paren_start_token.info, TokenInfo::ParenthesesStart) {
                return Err(ParsingError {
                    errors: vec![ParsingErrorMessage {
                        msg: format!("While parsing a 'IN' relation (for example 'a IN (1, 2)'), expected '(' after the 'IN' keyword. Instead got: '{}'.", paren_start_token.info),
                        position_start: paren_start_token.position_start,
                        position_end: paren_start_token.position_end,
                    }],
                    // TODO: note: where in started?
                    notes: Vec::new()
                });
            }

            let mut terms = Vec::new();

            // 3 cases:
            //   1. IN ()
            //   2. IN (term1)
            //   3. IN (term1, term2, term3, ..., termX)

            if let TokenInfo::ParenthesesEnd = parser_state.peek_token().info {
                // Case 1
            } else {
                let term1 = parse_term(parser_state)?;
                terms.push(term1);

                while matches!(parser_state.peek_token().info, TokenInfo::Comma) {
                    parser_state.eat_token(); // Eat ','

                    let term_i = parse_term(parser_state)?;
                    terms.push(term_i);
                }
            }

            let paren_end_token = parser_state.eat_token();
            if !matches!(paren_end_token.info, TokenInfo::ParenthesesEnd) {
                return Err(ParsingError {
                    errors: vec![ParsingErrorMessage {
                        msg: format!("While parsing a 'IN' relation (for example 'a IN (1, 2)'), expected ')' after the 'IN' list. Instead got: '{}'.", paren_end_token.info),
                        position_start: paren_end_token.position_start,
                        position_end: paren_end_token.position_end,
                    }],
                    // TODO: note: where in started?
                    notes: Vec::new()
                });
            }
            Ok(Relation::InRelation(relation_lhs, terms))
        }
        _ => Err(ParsingError {
            errors: vec![ParsingErrorMessage {
                msg: format!("While parsing a relation (such as 'a = 5' or 'a IN (1, 2)'), got an invalid relation operator: '{}'.", relation_operator.info),
                position_start: relation_operator.position_start,
                position_end: relation_operator.position_end,
            }],
            // TODO: note: where in started?
            notes: Vec::new()
        }),
    }
}

fn parse_relation_lhs(parser_state: &mut ParserState) -> Result<RelationLHS, ParsingError> {
    let identifier_token = parser_state.eat_token();
    if let TokenInfo::Identifier(identifier) = to_column_identifier(&identifier_token.info) {
        // Is it a[3]?
        if matches!(
            parser_state.peek_token().info,
            TokenInfo::SquareBracketStart
        ) {
            // a[3]
            parser_state.eat_token(); // Eat '['
            let term = parse_term(parser_state)?;

            let square_bracket_end_token = parser_state.eat_token();
            if !matches!(square_bracket_end_token.info, TokenInfo::SquareBracketEnd) {
                return Err(ParsingError {
                    errors: vec![ParsingErrorMessage {
                        msg: format!("While parsing a relation left-hand side (for example 'a[3]' as in 'a[3] = ?'), expected ']' at the end of the subscript. Instead got: '{}'.", square_bracket_end_token.info),
                        position_start: square_bracket_end_token.position_start,
                        position_end: square_bracket_end_token.position_end,
                    }],
                    // TODO: note: where in started?
                    notes: Vec::new()
                });
            }

            Ok(RelationLHS::Subscript(identifier, term))
        } else {
            // a
            Ok(RelationLHS::Identifier(identifier))
        }
    } else if let TokenInfo::ParenthesesStart = identifier_token.info {
        // Tuple of identifiers (a, b, c)
        let mut identifiers = Vec::new();
        let identifier1 = parser_state.eat_token();
        if let TokenInfo::Identifier(ident) = &identifier1.info {
            identifiers.push(ident.clone());
        } else {
            return Err(ParsingError {
                errors: vec![ParsingErrorMessage {
                    msg: format!("While parsing a relation left-hand side (such as '(a, b)' in '(a, b) = (1, 2)'), expected an identifier after an opening '('. Instead got: '{}'.", identifier1.info),
                    position_start: identifier1.position_start,
                    position_end: identifier1.position_end,
                }],
                notes: Vec::new()
            });
        }

        while matches!(parser_state.peek_token().info, TokenInfo::Comma) {
            parser_state.eat_token(); // Eat ','

            let identifier_i = parser_state.eat_token();
            if let TokenInfo::Identifier(ident) = &identifier_i.info {
                identifiers.push(ident.clone());
            } else {
                return Err(ParsingError {
                    errors: vec![ParsingErrorMessage {
                        // TODO: i-th
                        msg: format!("While parsing a relation left-hand side (such as '(a, b)' in '(a, b) = (1, 2)'), expected an identifier after the comma ','. Instead got: '{}'.", identifier_i.info),
                        position_start: identifier_i.position_start,
                        position_end: identifier_i.position_end,
                    }],
                    // TODO: node: previous one
                    notes: Vec::new()
                });
            }
        }

        let paren_end_token = parser_state.eat_token();
        if !matches!(paren_end_token.info, TokenInfo::ParenthesesEnd) {
            return Err(ParsingError {
                errors: vec![ParsingErrorMessage {
                    msg: format!("While parsing a relation left-hand side (such as '(a, b)' in '(a, b) = (1, 2)'), expected an closing parenthesis ')'. Instead got: '{}'.", paren_end_token.info),
                    position_start: paren_end_token.position_start,
                    position_end: paren_end_token.position_end,
                }],
                // TODO: where was the opening '('?
                notes: Vec::new()
            });
        }

        Ok(RelationLHS::TupleOfIdentifiers(identifiers))
    } else {
        Err(ParsingError {
            errors: vec![ParsingErrorMessage {
                msg: format!("While parsing a relation (such as 'a = 5' or 'a IN (1, 2)'), the relation started with a invalid token: '{}'.", identifier_token.info),
                position_start: identifier_token.position_start,
                position_end: identifier_token.position_end,
            }],
            // TODO: note: previous relation? hard?
            notes: Vec::new()
        })
    }
}

fn parse_term(parser_state: &mut ParserState) -> Result<Term, ParsingError> {
    let token = parser_state.eat_token();
    match &token.info {
        TokenInfo::Number(number) => Ok(Term::Number(number.clone())),
        TokenInfo::QuotedIdentifier(ident) => Ok(Term::QuotedIdentifier(ident.clone())),
        TokenInfo::Null => Ok(Term::Null),
        TokenInfo::True => Ok(Term::Boolean(true)),
        TokenInfo::False => Ok(Term::Boolean(false)),
        TokenInfo::SquareBracketStart => {
            let mut terms = Vec::new();

            let term1 = parse_term(parser_state)?;
            terms.push(term1);

            while matches!(parser_state.peek_token().info, TokenInfo::Comma) {
                parser_state.eat_token(); // Eat ','
                let term_i = parse_term(parser_state)?;
                terms.push(term_i);
            }

            let square_bracket_end_token = parser_state.eat_token();
            if !matches!(square_bracket_end_token.info, TokenInfo::SquareBracketEnd) {
                return Err(ParsingError {
                    errors: vec![ParsingErrorMessage {
                        msg: format!("While parsing a list term (for example '[1, 3]' as in 'a = [1, 3]'), expected ']' after the last element of the list. Instead got: '{}'.", square_bracket_end_token.info),
                        position_start: square_bracket_end_token.position_start,
                        position_end: square_bracket_end_token.position_end,
                    }],
                    // TODO: note: where in started?
                    notes: Vec::new()
                });
            }

            Ok(Term::List(terms))
        }
        _ => Err(ParsingError {
            errors: vec![ParsingErrorMessage {
                msg: format!(
                    "While parsing a term (such as '5' in 'a = 5'), got an invalid term: '{}'.",
                    token.info
                ),
                position_start: token.position_start,
                position_end: token.position_end,
            }],
            notes: Vec::new(),
        }),
    }
}

fn parse_order_by_clause(parser_state: &mut ParserState) -> Result<OrderByClause, ParsingError> {
    let identifer = parser_state.eat_token();
    if let TokenInfo::Identifier(identifier) = &identifer.info {
        if matches!(parser_state.peek_token().info, TokenInfo::Asc) {
            // ORDER BY column ASC
            parser_state.eat_token(); // Eat 'ASC'
            Ok(OrderByClause::Asc(identifier.clone()))
        } else if matches!(parser_state.peek_token().info, TokenInfo::Desc) {
            // ORDER BY column DESC
            parser_state.eat_token(); // Eat 'DESC'
            Ok(OrderByClause::Desc(identifier.clone()))
        } else {
            // ORDER BY column
            Ok(OrderByClause::TableOrder(identifier.clone()))
        }
    } else {
        Err(ParsingError {
            errors: vec![ParsingErrorMessage {
                msg: format!("While parsing a 'ORDER BY' clause (as in 'ORDER BY a, b'), expected a column name. Instead got: '{}'.", identifer.info),
                position_start: identifer.position_start,
                position_end: identifer.position_end,
            }],
            notes: Vec::new()
        })
    }
}
