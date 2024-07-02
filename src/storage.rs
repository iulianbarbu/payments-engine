use std::{collections::HashMap, sync::Arc};

use tokio::sync::{Mutex, RwLock};

use crate::{account::Account, payments::Tx};

// Abstraction over storage for access to accounts
pub trait AccountsDal {
    fn account(
        &self,
        id: u16,
    ) -> impl std::future::Future<Output = Option<Arc<Mutex<Account>>>> + std::marker::Send;
    fn insert(
        &mut self,
        account: Account,
    ) -> impl std::future::Future<Output = ()> + std::marker::Send;
    fn accounts(&self) ->  impl std::future::Future<Output = tokio::sync::RwLockReadGuard<HashMap<u16, Arc<Mutex<Account>>>>> + Send; 
}

#[derive(Default, Clone)]
pub struct InMemoryAccountLedger(Arc<RwLock<HashMap<u16, Arc<Mutex<Account>>>>>);

impl AccountsDal for InMemoryAccountLedger {
    async fn account(&self, id: u16) -> Option<Arc<Mutex<Account>>> {
        self.0.read().await.get(&id).map(|inner| inner.clone())
    }

    async fn insert(&mut self, account: Account) {
        self.0
            .write()
            .await
            .insert(account.client_id(), Arc::new(Mutex::new(account)));
    }
    
    async fn accounts(&self) ->  tokio::sync::RwLockReadGuard<'_, HashMap<u16,Arc<Mutex<Account>>>> {
        self.0.read().await
    }
}

pub trait TxsDal {
    fn tx(
        &self,
        id: u32,
    ) -> impl std::future::Future<Output = Option<Arc<Mutex<Tx>>>> + std::marker::Send;
    fn insert(&self, tx: Tx) -> impl std::future::Future<Output = ()> + Send;
}

#[derive(Default, Clone)]
pub struct InMemoryTxLedger(Arc<RwLock<HashMap<u32, Arc<Mutex<Tx>>>>>);

impl TxsDal for InMemoryTxLedger {
    async fn tx(&self, id: u32) -> Option<Arc<Mutex<Tx>>> {
        self.0.read().await.get(&id).map(|inner| inner.clone())
    }

    async fn insert(&self, tx: Tx) {
        self.0
            .write()
            .await
            .insert(tx.id(), Arc::new(Mutex::new(tx)));
    }
}