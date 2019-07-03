use std::borrow::Borrow;

pub enum Bytes<'a> {
    Borrowed(&'a [u8]),
    Owned(Vec<u8>),
}
impl<'a> From<&'a [u8]> for Bytes<'a> {
    fn from(slice: &'a [u8]) -> Self {
        Bytes::Borrowed(slice)
    }
}
impl<'a, T> From<&'a T> for Bytes<'a>
where
    T: Borrow<[u8]>,
{
    fn from(slice: &'a T) -> Self {
        Bytes::Borrowed(slice.borrow())
    }
}
impl<'a> From<Vec<u8>> for Bytes<'a> {
    fn from(vec: Vec<u8>) -> Self {
        Bytes::Owned(vec)
    }
}
impl<'a> std::ops::Deref for Bytes<'a> {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        match self {
            Bytes::Borrowed(a) => a,
            Bytes::Owned(ref a) => a,
        }
    }
}
impl<'a> std::ops::DerefMut for Bytes<'a> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        match self {
            Bytes::Borrowed(a) => {
                *self = Bytes::Owned(a.to_vec());
                self
            }
            Bytes::Owned(ref mut a) => a,
        }
    }
}
impl db_key::Key for Bytes<'_> {
    fn from_u8(key: &[u8]) -> Self {
        Bytes::Owned(key.to_vec())
    }

    fn as_slice<T, F: Fn(&[u8]) -> T>(&self, f: F) -> T {
        f(&self)
    }
}