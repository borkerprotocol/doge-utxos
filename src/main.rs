#[macro_use]
extern crate failure;
#[macro_use]
extern crate serde;

macro_rules! ldb_try {
    ($x:expr) => {
        $x.map_err(|e| format_err!("{:?}: {}", e, e))?
    };
}

mod block;
mod utxo;

use crate::block::Block;
use failure::Error;
use leveldb_rs::DB;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use throttled_bitcoin_rpc::BitcoinRpcClient;

pub const P2PKH: u8 = 30;
pub const P2SH: u8 = 22;
pub const CONFIRMATIONS: usize = 10;

pub type Rewind = Vec<HashMap<utxo::UTXOID, utxo::UTXOData>>;

#[derive(Deserialize)]
struct Config {
    node_uri: String,
    node_user: Option<String>,
    node_password: Option<String>,
}

fn main() -> Result<(), Error> {
    let conf: Config = serde_yaml::from_reader(std::fs::File::open("config.yaml")?)?;
    let client = BitcoinRpcClient::new(conf.node_uri, conf.node_user, conf.node_password, 0, 0, 0);
    let path = std::path::Path::new("utxos.db");
    let db = Arc::new(Mutex::new(ldb_try!(
        DB::open(path).or_else(|_| DB::create(path))
    )));
    let t = std::thread::spawn(move || loop {
        let mut rewind: Rewind = std::fs::File::open("rewind.cbor")
            .map_err(Error::from)
            .and_then(|f| serde_cbor::from_reader(f).map_err(Error::from))
            .unwrap_or_else(|_| {
                std::iter::repeat_with(|| HashMap::new())
                    .take(CONFIRMATIONS)
                    .collect()
            });
        match try_process_block(&client, &mut db.lock().unwrap(), &mut rewind) {
            Ok(Some(i)) => println!("scanned {}", i),
            Ok(None) => {
                match std::fs::File::create("rewind.cbor")
                    .map_err(Error::from)
                    .and_then(|mut f| serde_cbor::to_writer(&mut f, &rewind).map_err(Error::from))
                {
                    Ok(_) => (),
                    Err(e) => eprintln!("ERROR SAVING REWIND: {}", e),
                }
            }
            Err(e) => eprintln!("ERROR: {}", e),
        };
    });

    t.join().unwrap();

    Ok(())
}

fn try_process_block(
    client: &BitcoinRpcClient,
    db: &mut DB,
    rewind: &mut Rewind,
) -> Result<Option<u32>, Error> {
    let idx = match ldb_try!(db.get(&[0_u8])) {
        Some(b) => {
            let mut buf = [0_u8; 4];
            if b.len() == 4 {
                buf.clone_from_slice(&b);
            } else {
                bail!("invalid size for u32");
            }
            u32::from_ne_bytes(buf)
        }
        None => 1,
    };
    let mut bkey = Vec::with_capacity(9);
    bkey.push(3_u8);
    bkey.extend(&idx.to_ne_bytes());
    let bhash_str = match client.getblockhash(idx as isize) {
        Ok(a) => a,
        _ => return Ok(None),
    };
    let bhash = hex::decode(&bhash_str)?;
    ldb_try!(db.put(&bkey, &bhash));
    let block_raw = match client.getblock(bhash_str, false)? {
        throttled_bitcoin_rpc::reply::getblock::False(a) => hex::decode(a)?,
        _ => bail!("unexpected response"),
    };
    let block = Block::from_slice(&block_raw)?;
    handle_rewind(
        client,
        db,
        std::borrow::Borrow::<[u8]>::borrow(&block.header.prev_blockhash),
        idx - 1,
        rewind,
    )?;
    block.exec(db, idx, rewind)?;

    Ok(Some(idx))
}

fn handle_rewind(
    client: &BitcoinRpcClient,
    db: &mut DB,
    hash: &[u8],
    idx: u32,
    rewind: &mut Rewind,
) -> Result<(), Error> {
    if idx <= 1 {
        return Ok(());
    }

    let mut block_key = Vec::with_capacity(5);
    block_key.push(3_u8);
    block_key.extend(&idx.to_ne_bytes());
    let old_hash = ldb_try!(db.get(&block_key)).ok_or(format_err!("missing block_hash"))?;
    if old_hash == hash {
        return Ok(());
    }
    let block_raw = match client.getblock(hex::encode(old_hash), false)? {
        throttled_bitcoin_rpc::reply::getblock::False(a) => hex::decode(a)?,
        _ => bail!("unexpected response"),
    };
    let block = Block::from_slice(&block_raw)?;
    block.undo(db, idx, rewind)?;
    let block_raw = match client.getblock(hex::encode(hash), false)? {
        throttled_bitcoin_rpc::reply::getblock::False(a) => hex::decode(a)?,
        _ => bail!("unexpected response"),
    };
    let block = Block::from_slice(&block_raw)?;
    handle_rewind(
        client,
        db,
        std::borrow::Borrow::<[u8]>::borrow(&block.header.prev_blockhash),
        idx - 1,
        rewind,
    )?;
    block.exec(db, idx, rewind)?;
    ldb_try!(db.put(&block_key, hash));

    Ok(())
}