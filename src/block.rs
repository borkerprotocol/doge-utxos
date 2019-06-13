
use crate::utxo::*;
use crate::Rewind;
use bitcoin::consensus::Decodable;
use failure::Error;
use leveldb_rs::DB;
use std::collections::HashMap;

pub struct Block<'a> {
    pub header: bitcoin::BlockHeader,
    pub tx_count: u64,
    pub pos: u64,
    pub cur: std::io::Cursor<&'a [u8]>,
}

impl<'a> Block<'a> {
    pub fn from_slice(raw: &'a [u8]) -> Result<Self, Error> {
        let mut cur = std::io::Cursor::new(raw);
        let header: bitcoin::BlockHeader = Decodable::consensus_decode(&mut cur)?;
        if header.version & 1 << 8 != 0 {
            let _: bitcoin::Transaction = Decodable::consensus_decode(&mut cur)?;
            cur.set_position(cur.position() + 32);
            let len: bitcoin::VarInt = Decodable::consensus_decode(&mut cur)?;
            cur.set_position(cur.position() + 32 * len.0 + 4);
            let len: bitcoin::VarInt = Decodable::consensus_decode(&mut cur)?;
            cur.set_position(cur.position() + 32 * len.0 + 84);
        };
        let tx_count: bitcoin::VarInt = Decodable::consensus_decode(&mut cur)?;
        Ok(Block {
            header,
            tx_count: tx_count.0,
            pos: 0,
            cur,
        })
    }

    pub fn exec(self, db: &mut DB, idx: u32, rewind: &mut Rewind) -> Result<(), Error> {
        rewind[idx as usize % crate::CONFIRMATIONS] = HashMap::new();
        println!("scanning {} txs", self.tx_count);
        for tx in self {
            let tx = tx?;
            let mut txid = [0u8; 32];
            txid.clone_from_slice(&tx.txid()[..]);
            txid.reverse();
            for i in tx.input {
                UTXOID::from(&i).rem(db, idx, rewind)?;
            }
            for (i, o) in tx.output.into_iter().enumerate() {
                UTXO::from_txout(&txid, &o, i as u32).add(db)?;
            }
        }

        Ok(())
    }

    pub fn undo(self, db: &mut DB, idx: u32, rewind: &mut Rewind) -> Result<(), Error> {
        for (id, data) in rewind[idx as usize % crate::CONFIRMATIONS].iter() {
            UTXO::from((id, data.clone())).add(db)?;
        }
        rewind[idx as usize % crate::CONFIRMATIONS] = HashMap::new();
        for tx in self {
            let tx = tx?;
            let mut txid = [0u8; 32];
            txid.clone_from_slice(&tx.txid()[..]);
            txid.reverse();
            for (i, _) in tx.output.into_iter().enumerate() {
                UTXOID {
                    txid: txid.clone(),
                    vout: i as u32,
                }
                .rem(db, idx, rewind)?;
            }
        }

        Ok(())
    }
}

impl<'a> Iterator for Block<'a> {
    type Item = Result<bitcoin::Transaction, Error>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.pos < self.tx_count {
            let tx: Self::Item = parse_tx(&mut self.cur);
            pos += 1;
            Some(tx)
        } else {
            None
        }
    }
}

fn parse_tx<'a>(cur: &mut std::io::Cursor<&'a [u8]>) -> Result<bitcoin::Transaction, Error> {
    use bitcoin::consensus::Decoder;

    let version = cur.read_u32()?;
    let vin_count: bitcoin::VarInt = Decodable::consensus_decode(cur)?;
    let vins: Vec<bitcoin::TxIn> = (0..vin_count.0)
        .map(|_| Decodable::consensus_decode(cur))
        .collect::<Result<Vec<_>, _>>()?;
    let vout_count: bitcoin::VarInt = Decodable::consensus_decode(cur)?;
    let vouts: Vec<bitcoin::TxOut> = (0..vout_count.0)
        .map(|_| Decodable::consensus_decode(cur))
        .collect::<Result<Vec<_>, _>>()?;
    let lock_time = cur.read_u32()?;

    Ok(bitcoin::Transaction {
        version,
        input: vins,
        output: vouts,
        lock_time,
    })
}
