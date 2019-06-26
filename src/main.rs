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
use hyper::rt::Future;
use hyper::rt::Stream;
use hyper::service::service_fn;
use hyper::{Body, Request, Response, Server};
use leveldb_rs::DB;
use std::collections::HashMap;
use std::str::FromStr;
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

#[derive(Deserialize)]
struct RpcMethod {
    method: String,
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
    let db_arc = Arc::new(Mutex::new(ldb_try!(
        DB::open(path).or_else(|_| DB::create(path))
    )));
    let db = db_arc.clone();
    let client = client_arc.clone();
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
            Err(e) => eprintln!("ERROR: {}{}", e, e.backtrace()),
        };
    });

    let addr_http = ([0, 0, 0, 0], 11021).into();
    // let addr_https = ([0, 0, 0, 0], 11022).into();

    let db = db_arc.clone();
    let rpc_client = client_arc.clone();
    let rpc_arc = Arc::new(hyper::Uri::from_str(&conf.node_uri)?);
    let make_service = move || {
        let client = hyper::Client::new();
        let rpc = (&*rpc_arc).clone();
        let db = db.clone();
        let rpc_client = rpc_client.clone();
        service_fn(
            move |mut req: Request<Body>| match req.uri().path_and_query() {
                Some(p_and_q) if p_and_q.path() == "/" => {
                    let client = client.clone();
                    let mut r = Request::builder();
                    r.uri(rpc.clone());
                    r.method(req.method());
                    r.headers_mut().map(|h| *h = req.headers().clone());
                    let bstream = req.into_body();
                    let body = bstream.concat2().wait().map_err(Error::from);
                    let m_b: Result<(RpcMethod, _), _> = body.and_then(|b| {
                        serde_json::from_slice(&b)
                            .map(|m| (m, b))
                            .map_err(Error::from)
                    });
                    let b = m_b.and_then(|(m, b)| {
                        if m.method == "stop" {
                            bail!("unauthorized method")
                        } else {
                            Ok(b)
                        }
                    });
                    let req = b.and_then(|b| r.body(Body::from(b)).map_err(Error::from));
                    futures::future::Either::B(futures::future::Either::A(
                        futures::future::result(req)
                            .and_then(move |r| client.request(r).map_err(Error::from)),
                    ))
                }
                None => {
                    let client = client.clone();
                    let mut r = Request::builder();
                    r.uri(rpc.clone());
                    r.method(req.method());
                    r.headers_mut().map(|h| *h = req.headers().clone());
                    let bstream = req.into_body();
                    let body = bstream.concat2().wait().map_err(Error::from);
                    let m_b: Result<(RpcMethod, _), _> = body.and_then(|b| {
                        serde_json::from_slice(&b)
                            .map(|m| (m, b))
                            .map_err(Error::from)
                    });
                    let b = m_b.and_then(|(m, b)| {
                        if m.method == "stop" {
                            bail!("unauthorized method")
                        } else {
                            Ok(b)
                        }
                    });
                    let req = b.and_then(|b| r.body(Body::from(b)).map_err(Error::from));
                    futures::future::Either::B(futures::future::Either::B(
                        futures::future::result(req)
                            .and_then(move |r| client.request(r).map_err(Error::from)),
                    ))
                    // TODO: don't duplicate
                }
                Some(path_and_query) => {
                    futures::future::Either::A(match req.headers().get("Content-Type") {
                        Some(a) if a.as_bytes().starts_with(b"application/json") => {
                            futures::future::result(
                                api::handle_request(
                                    &db.lock().unwrap(),
                                    &rpc_client,
                                    path_and_query,
                                )
                                .and_then(|res| Ok(Response::new(Body::from(res.to_json()?)))),
                            )
                        }
                        Some(a) if a.as_bytes().starts_with(b"application/cbor") => {
                            futures::future::result(
                                api::handle_request(
                                    &db.lock().unwrap(),
                                    &rpc_client,
                                    path_and_query,
                                )
                                .and_then(|res| {
                                    Ok(Response::new(Body::from(serde_cbor::to_vec(&res)?)))
                                }),
                            )
                        }
                        Some(a) if a.as_bytes().starts_with(b"application/x-yaml") => {
                            futures::future::result(
                                api::handle_request(
                                    &db.lock().unwrap(),
                                    &rpc_client,
                                    path_and_query,
                                )
                                .and_then(|res| {
                                    Ok(Response::new(Body::from(serde_yaml::to_string(&res)?)))
                                }),
                            )
                        }
                        Some(a) if a.as_bytes().starts_with(b"application/octet-stream") => {
                            futures::future::result(
                                api::handle_request(
                                    &db.lock().unwrap(),
                                    &rpc_client,
                                    path_and_query,
                                )
                                .and_then(|res| Ok(Response::new(Body::from(res.to_bytes())))),
                            )
                        }
                        _ => futures::future::err(format_err!("Invalid content type!")),
                    })
                }
            },

        )
    };

    let server_http = Server::bind(&addr_http).serve(make_service);
    // let server_https = Server::bind(&addr_https).serve(make_service);

    hyper::rt::run(server_http.map_err(|e| {
        eprintln!("server error: {}", e);
    }));

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
        &block.header.prev_blockhash[..],
        idx - 1,
        rewind,
    )?;
    block.exec(db, idx, rewind)?;
    ldb_try!(db.put(&[0_u8], &(idx + 1).to_ne_bytes()));

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

    let mut cow = std::borrow::Cow::Borrowed(hash);
    let hash = cow.to_mut();
    hash.reverse();
    let mut block_key = Vec::with_capacity(5);
    block_key.push(3_u8);
    block_key.extend(&idx.to_ne_bytes());
    let old_hash = ldb_try!(db.get(&block_key)).ok_or(format_err!("missing block_hash"))?;
    if old_hash.as_slice() == AsRef::<[u8]>::as_ref(hash) {
        return Ok(());
    }
    let block_raw = match client.getblock(hex::encode(old_hash), false)? {
        throttled_bitcoin_rpc::reply::getblock::False(a) => hex::decode(a)?,
        _ => bail!("unexpected response"),
    };
    let block = Block::from_slice(&block_raw)?;
    block.undo(db, idx, rewind)?;
    let block_raw = match client.getblock(hex::encode(&hash), false)? {
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
    ldb_try!(db.put(&block_key, hash));

    Ok(())
}
