#[derive(thiserror::Error, Debug, Clone, PartialEq)]
pub enum Error {
    #[error("Missing amount for tx: {0}")]
    MissingAmount(u32),
    #[error("Total overflow")]
    TotalOverflow,
    #[error("Transaction not found")]
    TxNotFound,
    #[error("Account locked: {0}")]
    AccountLocked(u16),
    #[error("Transaction not disputed: {0}")]
    TxNotDisputed(u32),
    #[error("Transaction already disputed: {0}")]
    TxAlreadyDisputed(u32),
    #[error("Invalid amount: {0}")]
    InvalidAmount(String),
    #[error("Max available overflow")]
    MaxAvailableOverflow,
    #[error("Max held overflow")]
    MaxHeldOverflow,
    #[error("Min available underflow")]
    MinAvailableUnderflow,
    #[error("Min held underflow")]
    MinHeldUnderflow,
    #[error("Unexpected missing account: {0}")]
    UnexpectedMissingAccount(u16),
    #[error("Invalid dispute")]
    InvalidDispute(u32),
}
