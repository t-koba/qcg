use serde_json::Value;

use super::bag::ValueBag;
use super::error::{ExprError, MAX_EXPRESSION_DEPTH, MAX_EXPRESSION_NODES};
use super::eval::{as_number, evaluate_binary, evaluate_call, number_value, truthy};
use super::lexer::Token;

#[derive(Debug, Clone)]
pub(crate) enum ExpressionNode {
    Literal(Value),
    Path(String),
    Array(Vec<Self>),
    Unary {
        operator: &'static str,
        value: Box<Self>,
    },
    Binary {
        operator: &'static str,
        left: Box<Self>,
        right: Box<Self>,
    },
    Call {
        name: String,
        arguments: Vec<Self>,
    },
}

impl ExpressionNode {
    pub(crate) fn evaluate(&self, bag: &ValueBag) -> Result<Value, String> {
        match self {
            Self::Literal(value) => Ok(value.clone()),
            Self::Array(items) => items
                .iter()
                .map(|item| item.evaluate(bag))
                .collect::<Result<Vec<_>, _>>()
                .map(Value::Array),
            Self::Path(path) => Ok(bag.get_path(path).cloned().unwrap_or(Value::Null)),
            Self::Unary { operator, value } => {
                let value = value.evaluate(bag)?;
                match *operator {
                    "!" => Ok(Value::Bool(!truthy(&value))),
                    "-" => number_value(-as_number(&value, "unary -")?),
                    _ => Err(format!("unknown unary operator `{operator}`")),
                }
            }
            Self::Binary {
                operator,
                left,
                right,
            } => {
                let left = left.evaluate(bag)?;
                match *operator {
                    "||" if truthy(&left) => return Ok(Value::Bool(true)),
                    "&&" if !truthy(&left) => return Ok(Value::Bool(false)),
                    _ => {}
                }
                let right = right.evaluate(bag)?;
                evaluate_binary(operator, &left, &right)
            }
            Self::Call { name, arguments } => {
                let values = arguments
                    .iter()
                    .map(|argument| argument.evaluate(bag))
                    .collect::<Result<Vec<_>, _>>()?;
                evaluate_call(name, &values)
            }
        }
    }
}

pub(crate) struct Parser {
    tokens: Vec<Token>,
    cursor: usize,
    nodes: usize,
}

impl Parser {
    pub(crate) fn new(tokens: Vec<Token>) -> Self {
        Self {
            tokens,
            cursor: 0,
            nodes: 0,
        }
    }

    pub(crate) fn parse_expression(
        &mut self,
        min_binding_power: u8,
    ) -> Result<ExpressionNode, ExprError> {
        self.parse_expression_at(min_binding_power, 0)
    }

    fn parse_expression_at(
        &mut self,
        min_binding_power: u8,
        depth: usize,
    ) -> Result<ExpressionNode, ExprError> {
        self.ensure_depth(depth)?;
        let mut left = self.parse_prefix(depth)?;
        while let Token::Operator(operator) = self.peek() {
            let Some((left_power, right_power)) = infix_binding_power(operator) else {
                break;
            };
            if left_power < min_binding_power {
                break;
            }
            let operator = *operator;
            self.cursor += 1;
            let right = self.parse_expression_at(right_power, self.next_depth(depth)?)?;
            self.reserve_node()?;
            left = ExpressionNode::Binary {
                operator,
                left: Box::new(left),
                right: Box::new(right),
            };
        }
        Ok(left)
    }

    fn parse_prefix(&mut self, depth: usize) -> Result<ExpressionNode, ExprError> {
        self.ensure_depth(depth)?;
        let token = self.next().clone();
        match token {
            Token::Bool(value) => {
                self.reserve_node()?;
                Ok(ExpressionNode::Literal(Value::Bool(value)))
            }
            Token::Null => {
                self.reserve_node()?;
                Ok(ExpressionNode::Literal(Value::Null))
            }
            Token::String(value) => {
                self.reserve_node()?;
                Ok(ExpressionNode::Literal(Value::String(value)))
            }
            Token::Number(value) => {
                let value = number_value(value).map_err(ExprError::Syntax)?;
                self.reserve_node()?;
                Ok(ExpressionNode::Literal(value))
            }
            Token::Identifier(identifier) => {
                if matches!(self.peek(), Token::LeftParen) {
                    self.cursor += 1;
                    self.parse_call(identifier, self.next_depth(depth)?)
                } else if identifier.starts_with("inputs.")
                    || identifier.starts_with("steps.")
                    || identifier == "item"
                    || identifier.starts_with("item.")
                {
                    if identifier.split('.').count() > MAX_EXPRESSION_DEPTH {
                        return Err(ExprError::TooDeep {
                            limit: MAX_EXPRESSION_DEPTH,
                        });
                    }
                    self.reserve_node()?;
                    Ok(ExpressionNode::Path(identifier))
                } else {
                    Err(ExprError::Syntax(format!(
                        "unsupported literal `{identifier}`"
                    )))
                }
            }
            Token::Operator(operator @ ("!" | "-")) => {
                let value = self.parse_expression_at(13, self.next_depth(depth)?)?;
                self.reserve_node()?;
                Ok(ExpressionNode::Unary {
                    operator,
                    value: Box::new(value),
                })
            }
            Token::LeftParen => {
                let expression = self.parse_expression_at(0, self.next_depth(depth)?)?;
                match self.next() {
                    Token::RightParen => Ok(expression),
                    token => Err(ExprError::Syntax(format!("expected `)`, found {token:?}"))),
                }
            }
            Token::LeftBracket => {
                let mut items = Vec::new();
                if matches!(self.peek(), Token::RightBracket) {
                    self.cursor += 1;
                    self.reserve_node()?;
                    return Ok(ExpressionNode::Array(items));
                }
                loop {
                    items.push(self.parse_expression_at(0, self.next_depth(depth)?)?);
                    match self.next() {
                        Token::Comma => {}
                        Token::RightBracket => break,
                        token => {
                            return Err(ExprError::Syntax(format!(
                                "expected `,` or `]`, found {token:?}"
                            )));
                        }
                    }
                }
                self.reserve_node()?;
                Ok(ExpressionNode::Array(items))
            }
            token => Err(ExprError::Syntax(format!(
                "expected expression, found {token:?}"
            ))),
        }
    }

    fn parse_call(&mut self, name: String, depth: usize) -> Result<ExpressionNode, ExprError> {
        self.ensure_depth(depth)?;
        let mut arguments = Vec::new();
        if matches!(self.peek(), Token::RightParen) {
            self.cursor += 1;
            self.reserve_node()?;
            return Ok(ExpressionNode::Call { name, arguments });
        }
        loop {
            arguments.push(self.parse_expression_at(0, self.next_depth(depth)?)?);
            match self.next() {
                Token::Comma => {}
                Token::RightParen => break,
                token => {
                    return Err(ExprError::Syntax(format!(
                        "expected `,` or `)`, found {token:?}"
                    )));
                }
            }
        }
        self.reserve_node()?;
        Ok(ExpressionNode::Call { name, arguments })
    }

    fn ensure_depth(&self, depth: usize) -> Result<(), ExprError> {
        if depth > MAX_EXPRESSION_DEPTH {
            return Err(ExprError::TooDeep {
                limit: MAX_EXPRESSION_DEPTH,
            });
        }
        Ok(())
    }

    fn next_depth(&self, depth: usize) -> Result<usize, ExprError> {
        depth.checked_add(1).ok_or(ExprError::TooDeep {
            limit: MAX_EXPRESSION_DEPTH,
        })
    }

    fn reserve_node(&mut self) -> Result<(), ExprError> {
        if self.nodes >= MAX_EXPRESSION_NODES {
            return Err(ExprError::TooManyNodes {
                limit: MAX_EXPRESSION_NODES,
            });
        }
        self.nodes += 1;
        Ok(())
    }

    fn peek(&self) -> &Token {
        self.tokens.get(self.cursor).unwrap_or(&Token::End)
    }

    fn next(&mut self) -> &Token {
        let index = self.cursor;
        self.cursor = self.cursor.saturating_add(1);
        self.tokens.get(index).unwrap_or(&Token::End)
    }

    pub(crate) fn expect_end(&self) -> Result<(), ExprError> {
        match self.peek() {
            Token::End => Ok(()),
            token => Err(ExprError::Syntax(format!(
                "unexpected trailing token {token:?}"
            ))),
        }
    }
}

fn infix_binding_power(operator: &str) -> Option<(u8, u8)> {
    Some(match operator {
        "||" => (1, 2),
        "&&" => (3, 4),
        "==" | "!=" => (5, 6),
        ">" | "<" | ">=" | "<=" => (7, 8),
        "+" | "-" => (9, 10),
        "*" | "/" | "%" => (11, 12),
        _ => return None,
    })
}
