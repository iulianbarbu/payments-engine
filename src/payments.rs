use std::{str::FromStr, sync::Arc};

use crate::error::Error;
use bigdecimal::BigDecimal;
use csv_async::Trim;
use futures::StreamExt;
use serde::{de, Deserialize};
use tokio::{io::AsyncRead, sync::Mutex};
use tracing::debug;

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

// There is a bug with the `serde` feature of the bigdecimal create, which if used to deserialize
// strings to `BigDecimal` will not work correctly when we'll subtract 0.9999 (the maximum decimals)
// the amounts can have (in case of a withdawal, and possibly for additions with deposits too), so
// I needed to implement a custom deserializer to handle this correctly.
fn deserialize_explicitly<'de, D>(deserializer: D) -> Result<Option<BigDecimal>, D::Error>
where
    D: de::Deserializer<'de>,
{
    let s: Option<&str> = de::Deserialize::deserialize(deserializer).ok();
    if let Some(inner) = s {
        if !inner.is_empty() {
            return Ok(Some(
                BigDecimal::from_str(inner).map_err(de::Error::custom)?,
            ));
        }
    }

    Ok(None)
}

// Transaction model
#[derive(Deserialize, Debug)]
pub struct Tx {
    r#type: TxType,
    client: u16,
    #[serde(rename(deserialize = "tx"))]
    id: u32,
    #[serde(rename(deserialize = "amount"))]
    #[serde(deserialize_with = "deserialize_explicitly")]
    amount: Option<BigDecimal>,
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

    pub fn amount(&self) -> Option<&BigDecimal> {
        self.amount.as_ref()
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

                inner.add_available(self.amount().ok_or(Error::MissingAmount(self.id))?);
            }
            TxType::Withdrawal => {
                let inner = &mut account.lock().await;
                if inner.is_locked() {
                    return Err(Error::AccountLocked(inner.client_id()));
                }

                inner.sub_available(self.amount().ok_or(Error::MissingAmount(self.id))?)?;
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
                    let amount = inner_tx
                        .amount()
                        .ok_or(Error::MissingAmount(inner_tx.id))?
                        .clone();
                    inner_account.sub_available(&amount)?;
                    inner_tx.mark_disputed();
                    inner_account.add_held(&amount);
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

                    let amount = inner_tx
                        .amount()
                        .ok_or(Error::MissingAmount(inner_tx.id()))?
                        .clone();
                    inner_account.sub_held(&amount)?;
                    inner_tx.mark_resolved();
                    inner_account.add_available(&amount);
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

                    let amount = inner_tx
                        .amount()
                        .ok_or(Error::MissingAmount(inner_tx.id()))?;
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
                    debug!("Errored while processing transaction: {err}");
                    continue;
                }
            };
            let _ = tx.handle(self).await.map_err(|err| {
                debug!("TX handling: {err}");
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
    use std::str::FromStr;

    use bigdecimal::BigDecimal;

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
            amount: Some(BigDecimal::from_str("10.1").unwrap()),
            disputed: false,
        };

        // Success
        let val = tx.amount().unwrap();
        assert_eq!(val.to_string(), "10.1");

        // Success leading zero decimal part
        tx.amount = Some(BigDecimal::from_str("10.01").unwrap());
        let val = tx.amount().unwrap();
        assert_eq!(val.to_string(), "10.01");

        // Success leading zero whole part
        tx.amount = Some(BigDecimal::from_str("001.01").unwrap());
        let val = tx.amount().unwrap();
        assert_eq!(val.to_string(), "1.01");
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
            amount: Some(BigDecimal::from_str("10.1").unwrap()),
            disputed: false,
        };

        tx.handle(&mut engine).await.unwrap();
        let account = engine.account(0).await.unwrap();
        assert_eq!(
            account.lock().await.available().to_string(),
            tx.amount().unwrap().to_string()
        );
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
            amount: Some(BigDecimal::from_str("10.01").unwrap()),
            disputed: false,
        };

        tx.handle(&mut engine).await.unwrap();
        let account = engine.account(0).await.unwrap();
        assert_eq!(
            account.lock().await.available().to_string(),
            tx.amount().unwrap().to_string()
        );

        let tx = Tx {
            r#type: TxType::Withdrawal,
            client: 0,
            id: 0,
            amount: Some(BigDecimal::from(10)),
            disputed: false,
        };
        tx.handle(&mut engine).await.unwrap();
        assert_eq!(account.lock().await.available().to_string(), "0.01");
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
            amount: Some(BigDecimal::from_str("10.1").unwrap()),
            disputed: false,
        };

        tx.handle(&mut engine).await.unwrap();
        let account = engine.account(0).await.unwrap();
        assert_eq!(
            account.lock().await.available().to_string(),
            tx.amount().unwrap().to_string()
        );

        let tx = Tx {
            r#type: TxType::Withdrawal,
            client: 0,
            id: 0,
            amount: Some(BigDecimal::from_str("10.2").unwrap()),
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
            amount: Some(BigDecimal::from_str("10.1").unwrap()),
            disputed: false,
        };
        tx.handle(&mut engine).await.unwrap();
        TxsDal::insert(&mut engine, tx).await;

        let account = engine.account(0).await.unwrap();
        assert_eq!(account.lock().await.available().to_string(), "10.1");

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
        assert_eq!(account.lock().await.available().to_string(), "0.0");
        assert_eq!(account.lock().await.held().to_string(), "10.1");
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
            amount: Some(BigDecimal::from_str("10.1").unwrap()),
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
            amount: Some(BigDecimal::from_str("10.1").unwrap()),
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
            amount: Some(BigDecimal::from_str("10.1").unwrap()),
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
        assert_eq!(account.lock().await.available().to_string(), "10.1");
        assert_eq!(account.lock().await.held().to_string(), "0.0");
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
            amount: Some(BigDecimal::from_str("10.1").unwrap()),
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
            amount: Some(BigDecimal::from_str("10.1").unwrap()),
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
        assert_eq!(account.lock().await.available().to_string(), "0.0");
        assert_eq!(account.lock().await.held().to_string(), "0.0");
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
            amount: Some(BigDecimal::from_str("10.1").unwrap()),
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

    #[tokio::test]
    async fn handle_txs() {
        let mut engine = Engine::new(
            InMemoryAccountLedger::default(),
            InMemoryTxLedger::default(),
        );

        let txs = r#"type , client,tx ,amount
        deposit, 1, 1, 1.0
        deposit, 2, 2, 2.0
        deposit, 1, 3, 2.0
        withdrawal, 1, 4, 1.5
        withdrawal,2, 5, 3.0
        dispute,1,1,"#;
        engine
            .handle_txs(tokio::io::BufReader::new(txs.as_bytes()))
            .await
            .unwrap();
        assert_eq!(2, engine.accounts().await.len());
        assert_eq!(
            engine
                .account(1)
                .await
                .unwrap()
                .lock()
                .await
                .available()
                .to_string(),
            "0.5"
        );
        assert_eq!(
            engine
                .account(2)
                .await
                .unwrap()
                .lock()
                .await
                .available()
                .to_string(),
            "2"
        );
    }
}
