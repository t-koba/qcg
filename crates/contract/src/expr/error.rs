pub const MAX_EXPRESSION_BYTES: usize = 64 * 1024;
/// Maximum number of lexer tokens accepted for one expression, including `End`.
pub const MAX_EXPRESSION_TOKENS: usize = 4096;
/// Maximum recursive expression nesting accepted by the parser.
pub const MAX_EXPRESSION_DEPTH: usize = 128;
/// Maximum number of AST nodes accepted for one expression.
pub const MAX_EXPRESSION_NODES: usize = 2048;

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ExprError {
    #[error("expression input is {bytes} bytes, exceeding the {limit}-byte limit")]
    InputTooLarge { bytes: usize, limit: usize },
    #[error("expression token count exceeds the {limit}-token limit")]
    TooManyTokens { limit: usize },
    #[error("expression nesting depth exceeds the {limit}-level limit")]
    TooDeep { limit: usize },
    #[error("expression AST node count exceeds the {limit}-node limit")]
    TooManyNodes { limit: usize },
    #[error("{0}")]
    Syntax(String),
    #[error("{0}")]
    Evaluation(String),
}
