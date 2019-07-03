#![feature(duration_float)]

#[macro_use]
extern crate failure;
#[macro_use]
extern crate serde;

macro_rules! ldb_try {
    ($x:expr) => {
        $x.map_err(|e| format_err!("{:?}: {}", e, e))?
    };
}

mod api;
mod block;
mod utxo;

use crate::block::Block;
use failure::Error;
use futures::future::*;
use hyper::rt::Future;
use hyper::rt::Stream;
use hyper::service::service_fn;
use hyper::{Body, Request, Response, Server};
use leveldb_rs::DB;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;
use throttled_bitcoin_rpc::BitcoinRpcClient;

pub const P2PKH: u8 = 30;
pub const P2SH: u8 = 22;
pub const CONFIRMATIONS: usize = 10;

pub type Rewind = Vec<HashMap<utxo::UTXOID, (Option<utxo::UTXOData>, Option<Vec<u8>>)>>;

#[derive(Deserialize)]
struct Config {
    node_uri: String,
    node_user: Option<String>,
    node_password: Option<String>,
}

#[derive(Deserialize)]
struct RpcMethod {
    method: String,
}

#[derive(Deserialize)]
#[serde(untagged)]
enum RpcQuery {
    Single(RpcMethod),
    Multi(Vec<RpcMethod>),
}

fn main() -> Result<(), Error> {
    let conf: Config = serde_yaml::from_reader(std::fs::File::open("config.yaml")?)?;
    let client_arc = BitcoinRpcClient::new(
        conf.node_uri.clone(),
        conf.node_user.clone(),
        conf.node_password.clone(),
        0,
        0,
        0,
    );
    let path = std::path::Path::new("utxos.db");
    let db_arc = Arc::new(RwLock::new(ldb_try!(
        DB::open(path).or_else(|_| DB::create(path))
    )));
    let (send, recv) = crossbeam_channel::bounded(100);
    let db = db_arc.clone();
    let client = client_arc.clone();
    let b = std::thread::spawn(move || {
        let mut idx = match db.read().get(&[0_u8]) {
            Ok(Some(b)) => {
                let mut buf = [0_u8; 4];
                if b.len() == 4 {
                    buf.clone_from_slice(&b);
                } else {
                    panic!("invalid size for u32");
                }
                u32::from_ne_bytes(buf)
            }
            Ok(None) => 1,
            Err(e) => {
                panic!("{}", e);
            }
        };
        'main: loop {
            let count = match client.getblockcount() {
                Ok(a) => a,
                Err(e) => {
                    eprintln!("{}: {}", line!(), e);
                    continue 'main;
                }
            };
            use throttled_bitcoin_rpc::BatchRequest;
            let mut batcher = client.batcher::<String>();
            let idxs = (idx + 1)..std::cmp::min(idx + 21, count + 1);
            let time = std::time::Instant::now();
            for i in idxs.clone() {
                match batcher.getblockhash(i) {
                    Ok(_) => (),
                    Err(e) => {
                        eprintln!("{}: {}", line!(), e);
                        continue 'main;
                    }
                }
            }
            let hashes = match batcher.send() {
                Ok(a) => a,
                Err(e) => {
                    eprintln!("{}: {}", line!(), e);
                    continue 'main;
                }
            };
            for hash in hashes.iter() {
                match batcher.getblock(hash, false) {
                    Ok(_) => (),
                    Err(e) => {
                        eprintln!("{}: {}", line!(), e);
                        continue 'main;
                    }
                }
            }
            let blocks = match batcher.send() {
                Ok(a) => a,
                Err(e) => {
                    eprintln!("{}: {}", line!(), e);
                    continue 'main;
                }
            };
            println!("{:.2} fetches/second", 1.0 / time.elapsed().as_secs_f64());
            for ((i, hash), block) in idxs
                .into_iter()
                .zip(hashes.into_iter())
                .into_iter()
                .zip(blocks.into_iter())
            {
                let hash = match hex::decode(hash) {
                    Ok(a) => a,
                    Err(e) => {
                        eprintln!("{}: {}", line!(), e);
                        continue 'main;
                    }
                };
                let block = match hex::decode(block) {
                    Ok(a) => a,
                    Err(e) => {
                        eprintln!("{}: {}", line!(), e);
                        continue 'main;
                    }
                };
                match send.send((i, hash, block)) {
                    Ok(_) => (),
                    Err(e) => {
                        eprintln!("{}: {}", line!(), e);
                        continue 'main;
                    }
                };
                idx = i;
            }
        }
    });
    let db = db_arc.clone();
    let client = client_arc.clone();
    let t = std::thread::spawn(move || {
        let mut time = std::time::Instant::now();
        let mut tpb = std::time::Duration::from_secs(0);
        let mut periods = 0;
        loop {
            let mut rewind: Rewind = std::fs::File::open("rewind.cbor")
                .map_err(Error::from)
                .and_then(|f| serde_cbor::from_reader(f).map_err(Error::from))
                .unwrap_or_else(|_| {
                    std::iter::repeat_with(|| HashMap::new())
                        .take(CONFIRMATIONS)
                        .collect()
                });
            match try_process_block(&client, &recv, &db, &mut rewind) {
                Ok(Some(i)) => {
                    println!("scanned {}", i);
                    if i % 100 == 0 {
                        let inst_tpb = time.elapsed() / 100;
                        println!("{:.2} blocks/second", 1.0 / inst_tpb.as_secs_f64());
                        tpb = ((tpb * periods) + inst_tpb) / (periods + 1);
                        periods += 1;
                        time = std::time::Instant::now();
                    }
                    if i % 500 == 0 {
                        match client.getblockcount().ok() {
                            Some(count) if i < count as u32 => {
                                println!("average {} blocks/second", 1.0 / tpb.as_secs_f64());
                                let remaining = tpb * (count - i);
                                println!("{} remaining", humantime::format_duration(remaining));
                            }
                            _ => (),
                        }
                    }
                }
                Ok(None) => {
                    match std::fs::File::create("rewind.cbor")
                        .map_err(Error::from)
                        .and_then(|mut f| {
                            serde_cbor::to_writer(&mut f, &rewind).map_err(Error::from)
                        }) {
                        Ok(_) => (),
                        Err(e) => eprintln!("ERROR SAVING REWIND: {}", e),
                    }
                }
                Err(e) => eprintln!("ERROR: {}{}", e, e.backtrace()),
            };
        }
    });

    let addr_http = ([0, 0, 0, 0], 11021).into();
    // let addr_https = ([0, 0, 0, 0], 11022).into();

    let db = db_arc.clone();
    let rpc_arc = Arc::new(hyper::Uri::from_str(&conf.node_uri)?);
    let make_service = move || {
        let (uname, pwd) = (conf.node_user.clone(), conf.node_password.clone());
        let client = hyper::Client::new();
        let rpc = (&*rpc_arc).clone();
        let db = db.clone();
        service_fn(move |req: Request<Body>| {
            match req.uri().path_and_query() {
                Some(p_and_q) if p_and_q.path() == "/" => {
                    let client = client.clone();
                    let mut r = Request::builder();
                    r.uri(rpc.clone());
                    r.method(req.method());
                    r.headers_mut().map(|h| *h = req.headers().clone());
                    match uname {
                        Some(ref u) => {
                            r.header(
                                "Authorization",
                                format!(
                                    "Basic {}",
                                    base64::encode(&format!(
                                        "{}:{}",
                                        u,
                                        pwd.as_ref().unwrap_or(&"".to_owned())
                                    ))
                                ),
                            );
                        }
                        _ => (),
                    }
                    let bstream = req.into_body();
                    let body = bstream.concat2().map_err(Error::from);
                    let m_b = body.and_then(|b| {
                        serde_json::from_slice(&b)
                            .map(|m: RpcQuery| (m, b))
                            .map_err(Error::from)
                    });
                    let b = m_b.and_then(|(m, b)| match m {
                        RpcQuery::Single(ref m) if m.method == "stop" => {
                            bail!("unauthorized method")
                        }
                        RpcQuery::Multi(ref m)
                            if m.iter().filter(|m| m.method == "stop").count() > 0 =>
                        {
                            bail!("unauthorized method")
                        }
                        _ => Ok(b),
                    });
                    let req = b.and_then(move |b| r.body(Body::from(b)).map_err(Error::from));
                    Either::B(Either::A(
                        req.and_then(move |r| client.request(r).map_err(Error::from)),
                    ))
                }
                None => {
                    let client = client.clone();
                    let mut r = Request::builder();
                    r.uri(rpc.clone());
                    r.method(req.method());
                    r.headers_mut().map(|h| *h = req.headers().clone());
                    match uname {
                        Some(ref u) => {
                            r.header(
                                "Authorization",
                                base64::encode(&format!(
                                    "{}:{}",
                                    u,
                                    pwd.as_ref().unwrap_or(&"".to_owned())
                                )),
                            );
                        }
                        _ => (),
                    }
                    let bstream = req.into_body();
                    let body = bstream.concat2().map_err(Error::from);
                    let m_b = body.and_then(|b| {
                        serde_json::from_slice(&b)
                            .map(|m: RpcQuery| (m, b))
                            .map_err(Error::from)
                    });
                    let b = m_b.and_then(|(m, b)| match m {
                        RpcQuery::Single(ref m) if m.method == "stop" => {
                            bail!("unauthorized method")
                        }
                        RpcQuery::Multi(ref m)
                            if m.iter().filter(|m| m.method == "stop").count() > 0 =>
                        {
                            bail!("unauthorized method")
                        }
                        _ => Ok(b),
                    });
                    let req = b.and_then(move |b| r.body(Body::from(b)).map_err(Error::from));
                    Either::B(Either::B(
                        req.and_then(move |r| client.request(r).map_err(Error::from)),
                    ))
                    // TODO: Don't duplicate
                }
                Some(path_and_query) => Either::A(match req.headers().get("Content-Type") {
                    Some(a) if a.as_bytes().starts_with(b"application/json") => result(
                        api::handle_request(&db, path_and_query)
                            .and_then(|res| res.to_json())
                            .map(|res| Response::new(Body::from(res))),
                    ),
                    Some(a) if a.as_bytes().starts_with(b"application/cbor") => result(
                        api::handle_request(&db, path_and_query)
                            .and_then(|res| serde_cbor::to_vec(&res).map_err(Error::from))
                            .map(|res| Response::new(Body::from(res))),
                    ),
                    Some(a) if a.as_bytes().starts_with(b"application/x-yaml") => result(
                        api::handle_request(&db, path_and_query)
                            .and_then(|res| serde_yaml::to_string(&res).map_err(Error::from))
                            .map(|res| Response::new(Body::from(res))),
                    ),
                    Some(a) if a.as_bytes().starts_with(b"application/octet-stream") => result(
                        api::handle_request(&db, path_and_query)
                            .map(|res| res.to_bytes())
                            .map(|res| Response::new(Body::from(res))),
                    ),
                    _ => err(format_err!("invalid content type")),
                }),
            }
            .or_else(|e| {
                eprintln!("{}\n{}", e, e.backtrace());
                result(Response::builder().status(500).body(Body::from(format!(
                    "{}{}",
                    e,
                    e.backtrace()
                ))))
            })
        })
    };

    let server_http = Server::bind(&addr_http).serve(make_service);
    // let server_https = Server::bind(&addr_https).serve(make_service);

    hyper::rt::run(server_http.map_err(|e| {
        eprintln!("server error: {}", e);
    }));

    t.join().unwrap();
    b.join().unwrap();

    Ok(())
}

fn try_process_block(
    client: &BitcoinRpcClient,
    recv: &crossbeam_channel::Receiver<(u32, Vec<u8>, Vec<u8>)>,
    db: &RwLock<DB>,
    rewind: &mut Rewind,
) -> Result<Option<u32>, Error> {
    let (idx, bhash, block_raw) = match recv.try_recv() {
        Ok(a) => a,
        Err(crossbeam_channel::TryRecvError::Empty) => return Ok(None),
        Err(e) => return Err(Error::from(e)),
    };
    let mut bkey = Vec::with_capacity(9);
    bkey.push(3_u8);
    bkey.extend(&idx.to_ne_bytes());
    ldb_try!(db.write().put(&bkey, &bhash));
    let block = Block::from_slice(&block_raw)?;
    handle_rewind(
        client,
        db,
        &block.header.prev_blockhash[..],
        idx - 1,
        rewind,
    )?;
    block.exec(db, idx, rewind)?;
    ldb_try!(db.write().put(&[0_u8], &(idx + 1).to_ne_bytes()));

    Ok(Some(idx))
}

fn handle_rewind(
    client: &BitcoinRpcClient,
    db: &RwLock<DB>,
    hash: &[u8],
    idx: u32,
    rewind: &mut Rewind,
) -> Result<(), Error> {
    if idx <= 1 {
        return Ok(());
    }

    let mut cow = std::borrow::Cow::Borrowed(hash);
    let hash = cow.to_mut();
    hash.reverse();
    let mut block_key = Vec::with_capacity(5);
    block_key.push(3_u8);
    block_key.extend(&idx.to_ne_bytes());
    let old_hash = ldb_try!(db.read().get(&block_key)).ok_or(format_err!("missing block_hash"))?;
    if old_hash.as_slice() == AsRef::<[u8]>::as_ref(hash) {
        return Ok(());
    }
    println!("reverting {}", hex::encode(old_hash.as_slice()));
    let block_raw = match client.getblock(&hex::encode(old_hash), false)? {
        throttled_bitcoin_rpc::reply::getblock::False(a) => hex::decode(a)?,
        _ => bail!("unexpected response"),
    };
    let block = Block::from_slice(&block_raw)?;
    block.undo(client, db, idx, rewind)?;
    let block_raw = match client.getblock(&hex::encode(&hash), false)? {
        throttled_bitcoin_rpc::reply::getblock::False(a) => hex::decode(a)?,
        _ => bail!("unexpected response"),
    };
    let block = Block::from_slice(&block_raw)?;
    handle_rewind(
        client,
        db,
        &block.header.prev_blockhash[..],
        idx - 1,
        rewind,
    )?;
    block.exec(db, idx, rewind)?;
    ldb_try!(db.write().put(&block_key, hash));

    Ok(())
}
