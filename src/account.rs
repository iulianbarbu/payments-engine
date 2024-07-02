use crate::error::Error;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Clone)]
pub struct Account {
    client_id: u16,
    available: u128,
    held: u128,
    locked: bool,
}

impl Account {
    pub fn new(client_id: u16, available: u128, held: u128, locked: bool) -> Self {
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
            available: 0,
            held: 0,
            locked: false,
        }
    }

    pub fn client_id(&self) -> u16 {
        self.client_id
    }

    pub fn available(&self) -> u128 {
        self.available
    }

    pub fn held(&self) -> u128 {
        self.held
    }

    pub fn total(&self) -> u128 {
        self.available + self.held
    }

    pub fn add_available(&mut self, amount: u128) -> Result<()> {
        self.available = self
            .available
            .checked_add(amount)
            .ok_or(Error::MaxAvailableOverflow)?;
        Ok(())
    }

    pub fn sub_available(&mut self, amount: u128) -> Result<()> {
        self.available = self
            .available
            .checked_sub(amount)
            .ok_or(Error::MinAvailableUnderflow)?;
        Ok(())
    }

    pub fn add_held(&mut self, amount: u128) -> Result<()> {
        self.held = self
            .held
            .checked_add(amount)
            .ok_or(Error::MaxHeldOverflow)?;
        Ok(())
    }

    pub fn sub_held(&mut self, amount: u128) -> Result<()> {
        self.held = self
            .held
            .checked_sub(amount)
            .ok_or(Error::MinHeldUnderflow)?;
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
    use crate::error::Error;

    use super::Account;

    #[test]
    fn add_available_success() {
        let mut account = Account::new_unlocked(0);
        account.add_available(10).unwrap();
        assert_eq!(account.available(), 10);
    }

    #[test]
    fn add_available_overflow() {
        let mut account = Account::new(0, u128::MAX, 0, false);
        let res = account.add_available(10);
        assert_eq!(res, Err(Error::MaxAvailableOverflow));
    }

    #[test]
    fn sub_available_success() {
        let mut account = Account::new(0, 11, 0, false);
        account.sub_available(10).unwrap();
        assert_eq!(account.available(), 1);
    }

    #[test]
    fn sub_available_underflow() {
        let mut account = Account::new_unlocked(0);
        let res = account.sub_available(1);
        assert_eq!(res, Err(Error::MinAvailableUnderflow));
    }

    #[test]
    fn add_held_success() {
        let mut account = Account::new_unlocked(0);
        account.add_held(10).unwrap();
        assert_eq!(account.held(), 10);
    }

    #[test]
    fn add_held_overflow() {
        let mut account = Account::new(0, 0, u128::MAX, false);
        let res = account.add_held(10);
        assert_eq!(res, Err(Error::MaxHeldOverflow));
    }

    #[test]
    fn sub_held_success() {
        let mut account = Account::new(0, 0, 11, false);
        account.sub_held(10).unwrap();
        assert_eq!(account.held(), 1);
    }

    #[test]
    fn sub_held_underflow() {
        let mut account = Account::new_unlocked(0);
        let res = account.sub_held(1);
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
