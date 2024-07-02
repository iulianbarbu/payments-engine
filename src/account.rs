use bigdecimal::{BigDecimal, Zero};

use crate::error::Error;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Clone)]
pub struct Account {
    client_id: u16,
    available: BigDecimal,
    held: BigDecimal,
    locked: bool,
}

impl Account {
    pub fn new(client_id: u16, available: BigDecimal, held: BigDecimal, locked: bool) -> Self {
        Account {
            client_id,
            available,
            held,
            locked,
        }
    }

    pub fn new_unlocked(client_id: u16) -> Self {
        Account {
            client_id,
            available: BigDecimal::zero(),
            held: BigDecimal::zero(),
            locked: false,
        }
    }

    pub fn client_id(&self) -> u16 {
        self.client_id
    }

    pub fn available(&self) -> BigDecimal {
        self.available.clone()
    }

    pub fn held(&self) -> BigDecimal {
        self.held.clone()
    }

    pub fn total(&self) -> BigDecimal {
        &self.available + &self.held
    }

    pub fn add_available(&mut self, amount: &BigDecimal) {
        self.available += amount;
    }

    pub fn sub_available(&mut self, amount: &BigDecimal) -> Result<()> {
        if &self.available < amount {
            return Err(Error::MinAvailableUnderflow);
        }

        self.available -= amount;
        Ok(())
    }

    pub fn add_held(&mut self, amount: &BigDecimal) {
        self.held += amount;
    }

    pub fn sub_held(&mut self, amount: &BigDecimal) -> Result<()> {
        if amount > &self.held {
            return Err(Error::MinHeldUnderflow);
        }

        self.held -= amount;
        Ok(())
    }

    pub fn is_locked(&self) -> bool {
        self.locked
    }

    pub fn set_locked(&mut self, locked: bool) {
        self.locked = locked;
    }
}

#[cfg(test)]

mod tests {
    use bigdecimal::{BigDecimal, One, Zero};

    use crate::error::Error;

    use super::Account;

    #[test]
    fn add_available_success() {
        let mut account = Account::new_unlocked(0);
        account.add_available(&BigDecimal::from(10));
        assert_eq!(account.available(), BigDecimal::from(10));
    }

    #[test]
    fn sub_available_success() {
        let mut account = Account::new(0, BigDecimal::from(11), BigDecimal::from(0), false);
        account.sub_available(&BigDecimal::from(10)).unwrap();
        assert_eq!(account.available(), BigDecimal::from(1));
    }

    #[test]
    fn sub_available_underflow() {
        let mut account = Account::new_unlocked(0);
        let res = account.sub_available(&BigDecimal::from(1));
        assert_eq!(res, Err(Error::MinAvailableUnderflow));
    }

    #[test]
    fn add_held_success() {
        let mut account = Account::new_unlocked(0);
        account.add_held(&BigDecimal::from(10));
        assert_eq!(account.held(), BigDecimal::from(10));
    }

    #[test]
    fn sub_held_success() {
        let mut account = Account::new(0, BigDecimal::zero(), BigDecimal::from(11), false);
        account.sub_held(&BigDecimal::from(10)).unwrap();
        assert_eq!(account.held(), BigDecimal::one());
    }

    #[test]
    fn sub_held_underflow() {
        let mut account = Account::new_unlocked(0);
        let res = account.sub_held(&BigDecimal::one());
        assert_eq!(res, Err(Error::MinHeldUnderflow));
    }

    #[test]
    fn set_locked() {
        let mut account = Account::new_unlocked(0);
        assert_eq!(account.is_locked(), false);
        account.set_locked(true);
        assert_eq!(account.is_locked(), true);
    }
}
