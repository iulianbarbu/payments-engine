use std::{convert::TryInto, sync::Arc};

use crate::error::Error;
use csv_async::Trim;
use futures::StreamExt;
use serde::{Deserialize, Deserializer};
use tokio::{io::AsyncRead, sync::Mutex};
use tracing::error;

use crate::{
    account::Account,
    storage::{AccountsDal, TxsDal},
};

// Transaction type
#[derive(Deserialize, Debug, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum TxType {
    Dispute,
    Resolve,
    Chargeback,
    Deposit,
    Withdrawal,
}

pub trait TxHandle<A: AccountsDal + Send + Sync + Clone, T: TxsDal + Send + Sync + Clone> {
    fn handle(
        &self,
        engine: &mut Engine<A, T>,
    ) -> impl std::future::Future<Output = Result<(), Error>> + Send;
}

fn deserialize_amount<'de, D>(deserializer: D) -> Result<Option<Amount>, D::Error>
where
    D: Deserializer<'de>,
{
    let buf = String::deserialize(deserializer)?;
    let amount_parts = buf.split('.').collect::<Vec<&str>>();

    if amount_parts.len() == 0 {
        return Ok(None);
    }

    if amount_parts.len() == 1 {
        return Ok(Some((amount_parts[0].to_owned(), "0".to_owned())));
    }

    if amount_parts.len() == 2 {
        return Ok(Some((
            amount_parts[0].to_owned(),
            amount_parts[1].to_owned(),
        )));
    }

    Err(serde::de::Error::custom(
        Error::InvalidAmount(format!("unexpected input: {buf}")).to_string(),
    ))
}

type Amount = (String, String);

// Transaction model
#[derive(Deserialize, Debug)]
pub struct Tx {
    r#type: TxType,
    client: u16,
    #[serde(rename(deserialize = "tx"))]
    id: u32,
    #[serde(rename(deserialize = "amount"))]
    #[serde(deserialize_with = "deserialize_amount")]
    amount: Option<Amount>,
    #[serde(skip_deserializing)]
    disputed: bool,
}

impl Tx {
    pub fn mark_disputed(&mut self) {
        self.disputed = true;
    }

    pub fn disputed(&self) -> bool {
        self.disputed
    }

    pub fn storable(&self) -> bool {
        self.r#type == TxType::Withdrawal || self.r#type == TxType::Deposit
    }

    pub fn mark_resolved(&mut self) {
        self.disputed = false;
    }

    pub fn mark_charged_back(&mut self) {
        self.disputed = false;
    }

    pub fn id(&self) -> u32 {
        self.id
    }

    pub fn amount(&self) -> &Option<Amount> {
        &self.amount
    }

    /// Parse the amount and store it in 10**4 denomination.
    pub fn parse_amount(&self) -> std::result::Result<u128, Error> {
        match &self.amount {
            Some(inner) => {
                let whole = inner.0.parse::<u128>().map_err(|err| {
                    Error::InvalidAmount(format!(
                        "Whole part error: {err} when parsing: {}",
                        inner.0
                    ))
                })?;

                let fraction = inner.1.parse::<u128>().map_err(|err| {
                    Error::InvalidAmount(format!(
                        "Fractional part error: {err} when parsing: {}",
                        inner.1
                    ))
                })?;
                let fraction_len: u32 = inner.1.len().try_into().map_err(|err| {
                    Error::InvalidAmount(format!(
                        "Encountered {err} when determining fraction digits count"
                    ))
                })?;

                if fraction_len > 4 {
                    return Err(Error::InvalidAmount(
                        "amount has more than 4 decimal places".to_owned(),
                    ));
                }

                Ok(whole
                    .checked_mul(10000u128)
                    .ok_or(Error::InvalidAmount(format!(
                        "overflow when storing {whole} in 10**4 denomination"
                    )))? // Safe to do unchecked multiplication/subtraction below
                    // given we check the fractional length before.
                    .checked_add(fraction * 10u128.pow(4 - fraction_len))
                    .ok_or(Error::InvalidAmount(format!(
                        "overflow when storing the {whole} + {fraction} in 10**4 denomination"
                    )))?)
            }
            None => Ok(0),
        }
    }
}

impl<A: AccountsDal + Send + Sync + Clone, T: TxsDal + Send + Sync + Clone> TxHandle<A, T> for Tx {
    async fn handle(&self, engine: &mut Engine<A, T>) -> std::result::Result<(), Error> {
        let account = match engine.account(self.client).await {
            Some(inner) => inner,
            None => {
                AccountsDal::insert(engine, Account::new_unlocked(self.client)).await;
                engine
                    .account(self.client)
                    .await
                    .ok_or(Error::UnexpectedMissingAccount(self.client))?
            }
        };

        match self.r#type {
            TxType::Deposit => {
                let inner = &mut account.lock().await;
                if inner.is_locked() {
                    return Err(Error::AccountLocked(inner.client_id()));
                }

                inner.add_available(self.parse_amount()?)?;
            }
            TxType::Withdrawal => {
                let inner = &mut account.lock().await;
                if inner.is_locked() {
                    return Err(Error::AccountLocked(inner.client_id()));
                }

                inner.sub_available(self.parse_amount()?)?;
            }
            TxType::Dispute => match engine.tx(self.id).await {
                None => Err(Error::TxNotFound)?,
                Some(to_be_disputed_tx) => {
                    let inner_tx = &mut to_be_disputed_tx.lock().await;
                    let inner_account = &mut account.lock().await;

                    if inner_account.is_locked() {
                        return Err(Error::AccountLocked(inner_account.client_id()));
                    }

                    if inner_tx.r#type != TxType::Deposit {
                        return Err(Error::InvalidDispute(inner_tx.id));
                    }

                    if inner_tx.disputed() {
                        return Err(Error::TxAlreadyDisputed(inner_tx.id));
                    }

                    let amount = inner_tx.parse_amount()?;
                    inner_account.sub_available(amount)?;
                    inner_tx.mark_disputed();
                    inner_account.add_held(amount)?;
                }
            },
            TxType::Resolve => match engine.tx(self.id).await {
                None => Err(Error::TxNotFound)?,
                Some(disputed_tx) => {
                    let inner_tx = &mut disputed_tx.lock().await;
                    if !inner_tx.disputed() {
                        return Err(Error::TxNotDisputed(inner_tx.id));
                    }

                    let inner_account = &mut account.lock().await;
                    if inner_account.is_locked() {
                        return Err(Error::AccountLocked(inner_account.client_id()));
                    }

                    let amount = inner_tx.parse_amount()?;
                    inner_account.sub_held(amount)?;
                    inner_tx.mark_resolved();
                    inner_account.add_available(amount)?;
                }
            },
            TxType::Chargeback => match engine.tx(self.id).await {
                None => Err(Error::TxNotFound)?,
                Some(disputed_tx) => {
                    let inner_tx = &mut disputed_tx.lock().await;
                    if !inner_tx.disputed() {
                        return Err(Error::TxNotDisputed(inner_tx.id));
                    }

                    let inner_account = &mut account.lock().await;
                    if inner_account.is_locked() {
                        return Err(Error::AccountLocked(inner_account.client_id()));
                    }

                    let amount = inner_tx.parse_amount()?;
                    inner_account.sub_held(amount)?;
                    inner_account.set_locked(true);
                    inner_tx.mark_charged_back();
                }
            },
        }

        Ok(())
    }
}

#[derive(Clone)]
pub struct Engine<A: AccountsDal, T: TxsDal> {
    accounts: A,
    txs: T,
}

impl<
        A: AccountsDal + std::marker::Sync + std::marker::Send,
        T: TxsDal + std::marker::Sync + std::marker::Send,
    > AccountsDal for Engine<A, T>
{
    async fn account(&self, id: u16) -> Option<Arc<Mutex<Account>>> {
        self.accounts.account(id).await
    }

    async fn insert(&mut self, account: Account) {
        self.accounts.insert(account).await
    }

    async fn accounts(
        &self,
    ) -> tokio::sync::RwLockReadGuard<std::collections::HashMap<u16, Arc<Mutex<Account>>>> {
        self.accounts.accounts().await
    }
}

impl<
        A: AccountsDal + std::marker::Sync + std::marker::Send,
        T: TxsDal + std::marker::Sync + std::marker::Send,
    > TxsDal for Engine<A, T>
{
    async fn tx(&self, id: u32) -> Option<Arc<Mutex<Tx>>> {
        self.txs.tx(id).await
    }

    async fn insert(&self, tx: Tx) {
        self.txs.insert(tx).await
    }
}

impl<A: AccountsDal + Send + Sync + Clone, T: TxsDal + Send + Sync + Clone> Engine<A, T> {
    pub fn new(accounts: A, txs: T) -> Self {
        Engine { accounts, txs }
    }

    pub async fn handle_txs(
        &mut self,
        tx_stream: impl AsyncRead + Send + Unpin,
    ) -> anyhow::Result<()> {
        let rdr = csv_async::AsyncReaderBuilder::new()
            .trim(Trim::All)
            .create_deserializer(tx_stream);
        let mut records = rdr.into_deserialize::<Tx>();
        while let Some(record) = records.next().await {
            let tx: Tx = match record {
                Ok(inner) => inner,
                Err(err) => {
                    error!("Errored while processing transaction: {err}");
                    continue;
                }
            };
            let _ = tx.handle(self).await.map_err(|err| {
                error!("TX handling: {err}");
                err
            });
            if tx.storable() {
                TxsDal::insert(self, tx).await;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        error::Error,
        storage::{AccountsDal, InMemoryAccountLedger, InMemoryTxLedger, TxsDal},
    };

    use super::{Engine, Tx, TxHandle, TxType};

    #[test]
    fn parse_amount() {
        let mut tx = Tx {
            r#type: TxType::Deposit,
            client: 0,
            id: 0,
            amount: Some((10.to_string(), 1.to_string())),
            disputed: false,
        };

        // Success
        let val = tx.parse_amount().unwrap();
        assert_eq!(val, 101000);

        // Success leading zero decimal part
        tx.amount = Some((10.to_string(), "01".to_owned()));
        let val = tx.parse_amount().unwrap();
        assert_eq!(val, 100100);

        // Success leading zero whole part
        tx.amount = Some(("001".to_owned(), "01".to_owned()));
        let val = tx.parse_amount().unwrap();
        assert_eq!(val, 10100);

        // Fail overflow whole part
        let mut max_plus_one = u128::MAX.to_string();
        max_plus_one.push('1');
        tx.amount = Some((max_plus_one.clone(), "01".to_owned()));
        let res = tx.parse_amount();
        assert_eq!(
            res,
            Err(Error::InvalidAmount(format!(
                "Whole part error: number too large to fit in target type when parsing: {max_plus_one}",
            )))
        );

        // Fail overflow fractional part
        let mut max_plus_one = u128::MAX.to_string();
        max_plus_one.push('1');
        tx.amount = Some((1.to_string(), max_plus_one.clone()));
        let res = tx.parse_amount();
        assert_eq!(
            res,
            Err(Error::InvalidAmount(format!(
                "Fractional part error: number too large to fit in target type when parsing: {max_plus_one}",
            )))
        );

        // Fail too many decimal places
        tx.amount = Some((1.to_string(), "00001".to_owned()));
        let res = tx.parse_amount();
        assert_eq!(
            res,
            Err(Error::InvalidAmount(format!(
                "amount has more than 4 decimal places",
            )))
        );

        // Fail when adjusting the whole part to 10**4 denomination
        tx.amount = Some((u128::MAX.to_string(), 0.to_string()));
        let res = tx.parse_amount();
        assert_eq!(
            res,
            Err(Error::InvalidAmount(format!(
                "overflow when storing {} in 10**4 denomination",
                u128::MAX
            )))
        );

        // Fail when adding whole + fraction part after adjusting both
        // separately to 10**4 denomination
        let fraction = 9999.to_string();
        let whole = (u128::MAX / 10000u128).to_string();
        tx.amount = Some((whole.clone(), fraction.clone()));
        let res = tx.parse_amount();
        assert_eq!(
            res,
            Err(Error::InvalidAmount(format!(
                "overflow when storing the {whole} + {fraction} in 10**4 denomination",
            )))
        );
    }

    #[tokio::test]
    async fn new_account_deposit_success() {
        let mut engine = Engine::new(
            InMemoryAccountLedger::default(),
            InMemoryTxLedger::default(),
        );
        let tx = Tx {
            r#type: TxType::Deposit,
            client: 0,
            id: 0,
            amount: Some((0.to_string(), 1.to_string())),
            disputed: false,
        };

        tx.handle(&mut engine).await.unwrap();
        let account = engine.account(0).await.unwrap();
        assert_eq!(account.lock().await.available(), tx.parse_amount().unwrap());
    }

    #[tokio::test]
    async fn new_account_deposit_fail_with_available_overflow() {
        let mut engine = Engine::new(
            InMemoryAccountLedger::default(),
            InMemoryTxLedger::default(),
        );
        let tx = Tx {
            r#type: TxType::Deposit,
            client: 0,
            id: 0,
            amount: Some(((u128::MAX / 10000u128).to_string(), 0.to_string())),
            disputed: false,
        };
        tx.handle(&mut engine).await.unwrap();
        let tx = Tx {
            r#type: TxType::Deposit,
            client: 0,
            id: 1,
            amount: Some(((u128::MAX % 10000u128 + 1).to_string(), 0.to_string())),
            disputed: false,
        };
        let res = tx.handle(&mut engine).await;
        assert_eq!(res, Err(Error::MaxAvailableOverflow));
    }

    #[tokio::test]
    async fn withdrawal_success() {
        let mut engine = Engine::new(
            InMemoryAccountLedger::default(),
            InMemoryTxLedger::default(),
        );
        let tx = Tx {
            r#type: TxType::Deposit,
            client: 0,
            id: 0,
            amount: Some((10.to_string(), "01".to_owned())),
            disputed: false,
        };

        tx.handle(&mut engine).await.unwrap();
        let account = engine.account(0).await.unwrap();
        assert_eq!(account.lock().await.available(), tx.parse_amount().unwrap());

        let tx = Tx {
            r#type: TxType::Withdrawal,
            client: 0,
            id: 0,
            amount: Some((10.to_string(), 0.to_string())),
            disputed: false,
        };
        tx.handle(&mut engine).await.unwrap();
        assert_eq!(account.lock().await.available(), 100);
    }

    #[tokio::test]
    async fn withdrawal_fail_with_underflow() {
        let mut engine = Engine::new(
            InMemoryAccountLedger::default(),
            InMemoryTxLedger::default(),
        );
        let tx = Tx {
            r#type: TxType::Deposit,
            client: 0,
            id: 0,
            amount: Some((10.to_string(), 1.to_string())),
            disputed: false,
        };

        tx.handle(&mut engine).await.unwrap();
        let account = engine.account(0).await.unwrap();
        assert_eq!(account.lock().await.available(), tx.parse_amount().unwrap());

        let tx = Tx {
            r#type: TxType::Withdrawal,
            client: 0,
            id: 0,
            amount: Some((10.to_string(), "2".to_owned())),
            disputed: false,
        };
        let res = tx.handle(&mut engine).await;
        assert_eq!(res, Err(Error::MinAvailableUnderflow));
    }

    #[tokio::test]
    async fn dispute_success() {
        let mut engine = Engine::new(
            InMemoryAccountLedger::default(),
            InMemoryTxLedger::default(),
        );

        let tx = Tx {
            r#type: TxType::Deposit,
            client: 0,
            id: 0,
            amount: Some((10.to_string(), 1.to_string())),
            disputed: false,
        };
        tx.handle(&mut engine).await.unwrap();
        TxsDal::insert(&mut engine, tx).await;

        let account = engine.account(0).await.unwrap();
        assert_eq!(account.lock().await.available(), 101000);

        let tx = Tx {
            r#type: TxType::Dispute,
            client: 0,
            id: 0,
            amount: None,
            disputed: false,
        };
        tx.handle(&mut engine).await.unwrap();
        let tx = engine.tx(0).await.unwrap();
        assert_eq!(tx.lock().await.disputed(), true);
        assert_eq!(account.lock().await.available(), 0);
        assert_eq!(account.lock().await.held(), 101000);
    }

    #[tokio::test]
    async fn dispute_fail_with_tx_not_found() {
        let mut engine = Engine::new(
            InMemoryAccountLedger::default(),
            InMemoryTxLedger::default(),
        );

        let tx = Tx {
            r#type: TxType::Dispute,
            client: 0,
            id: 0,
            amount: None,
            disputed: false,
        };
        let res = tx.handle(&mut engine).await;
        assert_eq!(res, Err(Error::TxNotFound));
    }

    #[tokio::test]
    async fn dispute_fail_with_invalid_dispute() {
        let mut engine = Engine::new(
            InMemoryAccountLedger::default(),
            InMemoryTxLedger::default(),
        );

        let tx = Tx {
            r#type: TxType::Withdrawal,
            client: 0,
            id: 0,
            amount: Some((10.to_string(), 1.to_string())),
            disputed: false,
        };
        TxsDal::insert(&mut engine, tx).await;

        let tx = Tx {
            r#type: TxType::Dispute,
            client: 0,
            id: 0,
            amount: None,
            disputed: false,
        };
        let res = tx.handle(&mut engine).await;
        assert_eq!(res, Err(Error::InvalidDispute(0)));
    }

    #[tokio::test]
    async fn dispute_fail_with_tx_already_disputed() {
        let mut engine = Engine::new(
            InMemoryAccountLedger::default(),
            InMemoryTxLedger::default(),
        );

        let tx = Tx {
            r#type: TxType::Deposit,
            client: 0,
            id: 0,
            amount: Some((10.to_string(), 1.to_string())),
            disputed: false,
        };
        tx.handle(&mut engine).await.unwrap();
        TxsDal::insert(&mut engine, tx).await;

        let tx = Tx {
            r#type: TxType::Dispute,
            client: 0,
            id: 0,
            amount: None,
            disputed: false,
        };
        tx.handle(&mut engine).await.unwrap();
        let res = tx.handle(&mut engine).await;
        assert_eq!(res, Err(Error::TxAlreadyDisputed(0)));
    }

    #[tokio::test]
    async fn resolve_success() {
        let mut engine = Engine::new(
            InMemoryAccountLedger::default(),
            InMemoryTxLedger::default(),
        );

        let tx = Tx {
            r#type: TxType::Deposit,
            client: 0,
            id: 0,
            amount: Some((10.to_string(), 1.to_string())),
            disputed: false,
        };
        tx.handle(&mut engine).await.unwrap();
        TxsDal::insert(&mut engine, tx).await;

        let mut tx = Tx {
            r#type: TxType::Dispute,
            client: 0,
            id: 0,
            amount: None,
            disputed: false,
        };
        tx.handle(&mut engine).await.unwrap();
        tx.r#type = TxType::Resolve;
        tx.handle(&mut engine).await.unwrap();
        assert!(!tx.disputed());

        let account = engine.account(0).await.unwrap();
        assert_eq!(account.lock().await.available(), 101000);
        assert_eq!(account.lock().await.held(), 0);
    }

    #[tokio::test]
    async fn resolve_fail_tx_not_disputed() {
        let mut engine = Engine::new(
            InMemoryAccountLedger::default(),
            InMemoryTxLedger::default(),
        );

        let tx = Tx {
            r#type: TxType::Deposit,
            client: 0,
            id: 0,
            amount: Some((10.to_string(), 1.to_string())),
            disputed: false,
        };
        TxsDal::insert(&mut engine, tx).await;

        let tx = Tx {
            r#type: TxType::Resolve,
            client: 0,
            id: 0,
            amount: None,
            disputed: false,
        };
        let res = tx.handle(&mut engine).await;
        assert_eq!(res, Err(Error::TxNotDisputed(0)));
    }

    #[tokio::test]
    async fn resolve_fail_with_tx_not_found() {
        let mut engine = Engine::new(
            InMemoryAccountLedger::default(),
            InMemoryTxLedger::default(),
        );

        let tx = Tx {
            r#type: TxType::Resolve,
            client: 0,
            id: 0,
            amount: None,
            disputed: false,
        };
        let res = tx.handle(&mut engine).await;
        assert_eq!(res, Err(Error::TxNotFound));
    }

    #[tokio::test]
    async fn chargeback_success() {
        let mut engine = Engine::new(
            InMemoryAccountLedger::default(),
            InMemoryTxLedger::default(),
        );

        let tx = Tx {
            r#type: TxType::Deposit,
            client: 0,
            id: 0,
            amount: Some((10.to_string(), 1.to_string())),
            disputed: false,
        };
        tx.handle(&mut engine).await.unwrap();
        TxsDal::insert(&mut engine, tx).await;

        let mut tx = Tx {
            r#type: TxType::Dispute,
            client: 0,
            id: 0,
            amount: None,
            disputed: false,
        };
        tx.handle(&mut engine).await.unwrap();

        tx.r#type = TxType::Chargeback;
        tx.handle(&mut engine).await.unwrap();

        let account = engine.account(0).await.unwrap();
        assert_eq!(account.lock().await.available(), 0);
        assert_eq!(account.lock().await.held(), 0);
        assert!(account.lock().await.is_locked());
    }

    #[tokio::test]
    async fn chargeback_fail_tx_not_disputed() {
        let mut engine = Engine::new(
            InMemoryAccountLedger::default(),
            InMemoryTxLedger::default(),
        );

        let tx = Tx {
            r#type: TxType::Deposit,
            client: 0,
            id: 0,
            amount: Some((10.to_string(), 1.to_string())),
            disputed: false,
        };
        TxsDal::insert(&mut engine, tx).await;

        let tx = Tx {
            r#type: TxType::Chargeback,
            client: 0,
            id: 0,
            amount: None,
            disputed: false,
        };
        let res = tx.handle(&mut engine).await;
        assert_eq!(res, Err(Error::TxNotDisputed(0)));
    }

    #[tokio::test]
    async fn chargeback_fail_with_tx_not_found() {
        let mut engine = Engine::new(
            InMemoryAccountLedger::default(),
            InMemoryTxLedger::default(),
        );

        let tx = Tx {
            r#type: TxType::Chargeback,
            client: 0,
            id: 0,
            amount: None,
            disputed: false,
        };
        let res = tx.handle(&mut engine).await;
        assert_eq!(res, Err(Error::TxNotFound));
    }

    // TODO handle locked
    // TODO check again if dispute should work for withdrawal too
    // TODO test the handling logic, which also reads CSVs (check with whitespaces too)
}
