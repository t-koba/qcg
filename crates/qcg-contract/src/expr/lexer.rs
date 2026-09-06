use super::bag::ValueBag;
use super::error::{ExprError, MAX_EXPRESSION_BYTES, MAX_EXPRESSION_TOKENS};
use super::eval::truthy;
use super::parser::{ExpressionNode, Parser};

pub(crate) fn eval_expression(src: &str, bag: &ValueBag) -> Result<bool, ExprError> {
    ensure_expression_bytes(src)?;
    if src.trim().is_empty() {
        return Ok(false);
    }
    let mut parser = Parser::new(tokenize(src)?);
    let expression: ExpressionNode = parser.parse_expression(0)?;
    parser.expect_end()?;
    Ok(truthy(
        &expression.evaluate(bag).map_err(ExprError::Evaluation)?,
    ))
}

pub(crate) fn ensure_expression_bytes(source: &str) -> Result<(), ExprError> {
    if source.len() > MAX_EXPRESSION_BYTES {
        return Err(ExprError::InputTooLarge {
            bytes: source.len(),
            limit: MAX_EXPRESSION_BYTES,
        });
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum Token {
    Identifier(String),
    String(String),
    Number(f64),
    Bool(bool),
    Null,
    Operator(&'static str),
    LeftParen,
    RightParen,
    LeftBracket,
    RightBracket,
    Comma,
    End,
}

pub(crate) fn tokenize(source: &str) -> Result<Vec<Token>, ExprError> {
    ensure_expression_bytes(source)?;
    let chars = source.chars().collect::<Vec<_>>();
    let mut tokens = Vec::new();
    let mut index = 0;
    while index < chars.len() {
        let ch = chars[index];
        if ch.is_whitespace() {
            index += 1;
            continue;
        }
        if matches!(ch, '\'' | '"') {
            let quote = ch;
            index += 1;
            let mut value = String::new();
            let mut closed = false;
            while index < chars.len() {
                match chars[index] {
                    current if current == quote => {
                        index += 1;
                        closed = true;
                        break;
                    }
                    '\\' => {
                        index += 1;
                        let escaped = chars.get(index).copied().ok_or_else(|| {
                            ExprError::Syntax("unterminated string escape".into())
                        })?;
                        value.push(match escaped {
                            'n' => '\n',
                            'r' => '\r',
                            't' => '\t',
                            '\\' => '\\',
                            '\'' => '\'',
                            '"' => '"',
                            other => {
                                return Err(ExprError::Syntax(format!(
                                    "unsupported string escape `\\{other}`"
                                )));
                            }
                        });
                        index += 1;
                    }
                    current => {
                        value.push(current);
                        index += 1;
                    }
                }
            }
            if !closed {
                return Err(ExprError::Syntax("unterminated string literal".into()));
            }
            push_token(&mut tokens, Token::String(value))?;
            continue;
        }
        if ch.is_ascii_digit()
            || (ch == '.' && chars.get(index + 1).is_some_and(char::is_ascii_digit))
        {
            let start = index;
            index += 1;
            while index < chars.len()
                && (chars[index].is_ascii_digit()
                    || matches!(chars[index], '.' | 'e' | 'E' | '+' | '-'))
            {
                if matches!(chars[index], '+' | '-')
                    && !chars
                        .get(index.wrapping_sub(1))
                        .is_some_and(|previous| matches!(*previous, 'e' | 'E'))
                {
                    break;
                }
                index += 1;
            }
            let literal = chars[start..index].iter().collect::<String>();
            let value = literal
                .parse::<f64>()
                .map_err(|_| ExprError::Syntax(format!("invalid number literal `{literal}`")))?;
            if !value.is_finite() {
                return Err(ExprError::Syntax("number literal is not finite".into()));
            }
            push_token(&mut tokens, Token::Number(value))?;
            continue;
        }
        if ch.is_ascii_alphabetic() || ch == '_' {
            let start = index;
            index += 1;
            while index < chars.len()
                && (chars[index].is_ascii_alphanumeric() || matches!(chars[index], '_' | '.' | '-'))
            {
                index += 1;
            }
            let identifier = chars[start..index].iter().collect::<String>();
            push_token(
                &mut tokens,
                match identifier.as_str() {
                    "true" => Token::Bool(true),
                    "false" => Token::Bool(false),
                    "null" => Token::Null,
                    _ => Token::Identifier(identifier),
                },
            )?;
            continue;
        }
        let pair = chars
            .get(index + 1)
            .map(|next| [ch, *next].iter().collect::<String>());
        if let Some(operator) = pair.as_deref().and_then(|pair| match pair {
            "||" => Some("||"),
            "&&" => Some("&&"),
            "==" => Some("=="),
            "!=" => Some("!="),
            ">=" => Some(">="),
            "<=" => Some("<="),
            _ => None,
        }) {
            push_token(&mut tokens, Token::Operator(operator))?;
            index += 2;
            continue;
        }
        push_token(
            &mut tokens,
            match ch {
                '!' => Token::Operator("!"),
                '>' => Token::Operator(">"),
                '<' => Token::Operator("<"),
                '+' => Token::Operator("+"),
                '-' => Token::Operator("-"),
                '*' => Token::Operator("*"),
                '/' => Token::Operator("/"),
                '%' => Token::Operator("%"),
                '(' => Token::LeftParen,
                ')' => Token::RightParen,
                '[' => Token::LeftBracket,
                ']' => Token::RightBracket,
                ',' => Token::Comma,
                _ => {
                    return Err(ExprError::Syntax(format!(
                        "unexpected character `{ch}` at position {index}"
                    )));
                }
            },
        )?;
        index += 1;
    }
    push_token(&mut tokens, Token::End)?;
    Ok(tokens)
}

fn push_token(tokens: &mut Vec<Token>, token: Token) -> Result<(), ExprError> {
    if tokens.len() >= MAX_EXPRESSION_TOKENS {
        return Err(ExprError::TooManyTokens {
            limit: MAX_EXPRESSION_TOKENS,
        });
    }
    tokens.push(token);
    Ok(())
}
