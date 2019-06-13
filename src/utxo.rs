
use crate::Rewind;
use failure::Error;
use leveldb_rs::DB;

#[derive(Clone, Deserialize, Serialize, Hash, PartialEq, Eq)]
pub struct UTXOID {
    pub txid: [u8; 32],
    pub vout: u32,
}

pub struct UTXO<'a> {
    address: Option<[u8; 21]>,
    txid: &'a [u8; 32],
    vout: u32,
    value: u64,
}

#[derive(Clone, Deserialize, Serialize)]
pub struct UTXOData {
    address: Option<[u8; 21]>,
    value: u64,
}
impl<'a> From<(&'a UTXOID, UTXOData)> for UTXO<'a> {
    fn from((id, data): (&'a UTXOID, UTXOData)) -> Self {
        UTXO {
            address: data.address,
            txid: &id.txid,
            vout: id.vout,
            value: data.value,
        }
    }
}
impl<'a> From<UTXO<'a>> for (UTXOID, UTXOData) {
    fn from(utxo: UTXO) -> Self {
        (
            UTXOID {
                txid: utxo.txid.clone(),
                vout: utxo.vout,
            },
            UTXOData {
                address: utxo.address,
                value: utxo.value,
            },
        )
    }
}

impl<'a> UTXO<'a> {
    pub fn add(self, db: &mut DB) -> Result<(), Error> {
        if let Some(address) = self.address {
            let mut addr_key = Vec::with_capacity(26);
            addr_key.push(1_u8);
            addr_key.extend(address.as_ref());
            let len = ldb_try!(db.get(&addr_key)).unwrap_or([0_u8; 4].to_vec());
            let mut buf = [0_u8; 4];
            if len.len() == 4 {
                buf.clone_from_slice(&len);
            }
            ldb_try!(db.put(&addr_key, &(u32::from_ne_bytes(buf) + 1).to_ne_bytes()));
            addr_key.extend(&len);

            let mut utxoid_key = Vec::with_capacity(37);
            utxoid_key.push(2_u8);
            utxoid_key.extend(self.txid);
            utxoid_key.extend(&self.vout.to_ne_bytes());
            ldb_try!(db.put(&utxoid_key, &addr_key));

            let mut addr_value = Vec::with_capacity(44);
            addr_value.extend(self.txid);
            addr_value.extend(&self.vout.to_ne_bytes());
            addr_value.extend(&self.value.to_ne_bytes());
            ldb_try!(db.put(&addr_key, &addr_value));
        }
        Ok(())
    }

    pub fn from_txout(txid: &'a [u8; 32], out: &'a bitcoin::TxOut, vout: u32) -> Self {
        UTXO {
            txid,
            vout,
            value: out.value,
            address: {
                if out.script_pubkey.is_p2pkh() {
                    let addr = out
                        .script_pubkey
                        .iter(true)
                        .filter_map(|i| match i {
                            bitcoin::blockdata::script::Instruction::PushBytes(b) => b.get(0..20),
                            _ => None,
                        })
                        .next();
                    let mut buf = [crate::P2PKH; 21];
                    addr.map(|a| {
                        buf[1..].clone_from_slice(a);
                        buf
                    })
                } else if out.script_pubkey.is_p2sh() {
                    let addr = out
                        .script_pubkey
                        .iter(true)
                        .filter_map(|i| match i {
                            bitcoin::blockdata::script::Instruction::PushBytes(b) => b.get(0..20),
                            _ => None,
                        })
                        .next();
                    let mut buf = [crate::P2SH; 21];
                    addr.map(|a| {
                        buf[1..].clone_from_slice(a);
                        buf
                    })
                } else {
                    None
                }
            },
        }
    }

    pub fn from_kv(addr_key: &[u8], addr_value: &[u8]) -> Result<(UTXOID, UTXOData), Error> {
        let mut address = [0_u8; 21];
        address.clone_from_slice(
            &addr_key
                .get(1..22)
                .ok_or(format_err!("unexpected end of input"))?,
        );
        let mut txid = [0_u8; 32];
        txid.clone_from_slice(
            &addr_value
                .get(0..32)
                .ok_or(format_err!("unexpected end of input"))?,
        );
        let mut vout = [0_u8; 4];
        vout.clone_from_slice(
            &addr_value
                .get(32..36)
                .ok_or(format_err!("unexpected end of input"))?,
        );
        let mut value = [0_u8; 8];
        value.clone_from_slice(
            &addr_value
                .get(36..44)
                .ok_or(format_err!("unexpected end of input"))?,
        );
        Ok((
            UTXOID {
                txid,
                vout: u32::from_ne_bytes(vout),
            },
            UTXOData {
                address: Some(address),
                value: u64::from_ne_bytes(value),
            },
        ))
    }
}

impl UTXOID {
    pub fn rem(self, db: &mut DB, idx: u32, rewind: &mut Rewind) -> Result<(), Error> {
        let mut utxoid_key = Vec::with_capacity(37);
        utxoid_key.push(2_u8);
        utxoid_key.extend(&self.txid);
        utxoid_key.extend(&self.vout.to_ne_bytes());
        let addr_key = match ldb_try!(db.get(&utxoid_key)) {
            Some(a) => a,
            None => return Ok(()),
        };
        let len = ldb_try!(db.get(&addr_key[0..22])).ok_or(format_err!("missing addr length"))?;
        let mut buf = [0_u8; 4];
        if len.len() == 4 {
            buf.clone_from_slice(&len);
        } else {
            bail!("invalid addr length")
        }
        let replacement_idx = u32::from_ne_bytes(buf) - 1;
        let mut replacement_addr_key = Vec::with_capacity(26);
        replacement_addr_key.extend(&addr_key[0..22]);
        replacement_addr_key.extend(&replacement_idx.to_ne_bytes());
        if &replacement_idx.to_ne_bytes() != &addr_key[22..] {
            let replacement_addr_value = ldb_try!(db.get(&replacement_addr_key))
                .ok_or(format_err!("missing replacement addr data"))?;
            let kv: (UTXOID, UTXOData) = UTXO::from_kv(
                &addr_key,
                &ldb_try!(db.get(&addr_key))
                    .ok_or(format_err!("missing key to delete"))
                    .map_err(|e| {
                        println!(
                            "{}",
                            bitcoin::util::base58::check_encode_slice(&addr_key[1..22])
                        );
                        e
                    })?,
            )?;
            rewind[idx as usize % crate::CONFIRMATIONS].insert(kv.0, kv.1);
            ldb_try!(db.put(&addr_key, &replacement_addr_value));
        }
        ldb_try!(db.delete(&replacement_addr_key));
        ldb_try!(db.delete(&utxoid_key));
        ldb_try!(db.put(&addr_key[0..22], &replacement_idx.to_ne_bytes()));

        Ok(())
    }
}
impl<'a> From<&'a bitcoin::TxIn> for UTXOID {
    fn from(txin: &'a bitcoin::TxIn) -> Self {
        UTXOID {
            txid: {
                let mut buf = [0u8; 32];
                buf.clone_from_slice(&txin.previous_output.txid[..]);
                buf.reverse();
                buf
            },
            vout: txin.previous_output.vout,
        }
    }
}
